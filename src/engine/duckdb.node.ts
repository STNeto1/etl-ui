// Node.js native DuckDB adapter for tests
// This provides a compatible interface to the WASM version but uses native DuckDB
import * as duckdb from "duckdb";
import * as fs from "fs";
import * as path from "path";
import * as os from "os";
import type * as wasmDuckdb from "@duckdb/duckdb-wasm";

class NodeDuckDBConnection {
  constructor(private conn: duckdb.Connection) {}

  async query(sql: string): Promise<wasmDuckdb.Table> {
    return new Promise((resolve, reject) => {
      this.conn.all(sql, (err, rows) => {
        if (err) {
          reject(err);
          return;
        }

        // Convert to Arrow-like table format that the WASM version returns
        const fields = rows.length > 0 ? Object.keys(rows[0]!).map((name) => ({ name })) : [];
        const numCols = fields.length;
        const numRows = rows.length;

        // Create column accessors
        const columns = fields.map((field) => ({
          get: (rowIndex: number) => {
            if (rowIndex < 0 || rowIndex >= numRows) return null;
            return rows[rowIndex]?.[field.name] ?? null;
          },
        }));

        const table = {
          toArray: () => rows,
          schema: { fields },
          numCols,
          numRows,
          getChildAt: (index: number) => {
            if (index < 0 || index >= columns.length) return null;
            return columns[index]!;
          },
        } as unknown as wasmDuckdb.Table;

        resolve(table);
      });
    });
  }

  async close(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.conn.close((err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }
}

class NodeAsyncDuckDB {
  private db: duckdb.Database;
  private tempDir: string;
  private registeredFiles: Map<string, string> = new Map();

  constructor() {
    // In-memory database for tests
    this.db = new duckdb.Database(":memory:");
    // Create temp directory for test files
    this.tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "duckdb-test-"));
  }

  async connect(): Promise<NodeDuckDBConnection> {
    return Promise.resolve(new NodeDuckDBConnection(this.db.connect()));
  }

  async registerFileText(name: string, text: string): Promise<void> {
    // Write to temp directory and track absolute path
    const filePath = path.join(this.tempDir, name);
    fs.writeFileSync(filePath, text, "utf-8");
    this.registeredFiles.set(name, filePath);
  }

  async registerFileHandle(
    name: string,
    file: File,
    _protocol: number,
    _directIO: boolean,
  ): Promise<void> {
    // Read File to buffer and write to temp directory
    const arrayBuffer = await file.arrayBuffer();
    const buffer = Buffer.from(arrayBuffer);
    const filePath = path.join(this.tempDir, name);
    fs.writeFileSync(filePath, buffer);
    this.registeredFiles.set(name, filePath);
  }

  async dropFile(name: string): Promise<void> {
    const filePath = this.registeredFiles.get(name);
    if (filePath) {
      try {
        fs.unlinkSync(filePath);
      } catch {
        // Ignore errors
      }
      this.registeredFiles.delete(name);
    } else {
      // Also try to clean up from temp dir or CWD if not registered
      const tempPath = path.join(this.tempDir, name);
      const cwdPath = path.resolve(name);
      for (const p of [tempPath, cwdPath]) {
        try {
          if (fs.existsSync(p)) {
            fs.unlinkSync(p);
          }
        } catch {
          // Ignore errors
        }
      }
    }
  }

  async copyFileToBuffer(name: string): Promise<Uint8Array> {
    let filePath = this.registeredFiles.get(name);
    // If not in registered files, check temp directory and current working directory
    if (!filePath) {
      const tempPath = path.join(this.tempDir, name);
      if (fs.existsSync(tempPath)) {
        filePath = tempPath;
      } else {
        // Check current working directory (where COPY writes by default)
        const cwdPath = path.resolve(name);
        if (fs.existsSync(cwdPath)) {
          filePath = cwdPath;
        } else {
          throw new Error(`File not found: ${name} (checked ${tempPath} and ${cwdPath})`);
        }
      }
    }
    const buffer = fs.readFileSync(filePath);
    return new Uint8Array(buffer);
  }

  async terminate(): Promise<void> {
    // Clean up all registered files
    for (const filePath of this.registeredFiles.values()) {
      try {
        fs.unlinkSync(filePath);
      } catch {
        // Ignore errors
      }
    }
    this.registeredFiles.clear();

    // Clean up temp directory
    try {
      fs.rmdirSync(this.tempDir);
    } catch {
      // Ignore errors - directory might not be empty
    }

    return new Promise((resolve, reject) => {
      this.db.close((err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  // Helper method to get absolute path for a registered file
  getRegisteredFilePath(name: string): string | undefined {
    return this.registeredFiles.get(name);
  }

  // Helper method to construct absolute path for a file to be created
  getFilePathForWrite(name: string): string {
    return path.join(this.tempDir, name);
  }
}

export async function createNodeDuckDB(): Promise<wasmDuckdb.AsyncDuckDB> {
  const db = new NodeAsyncDuckDB();
  // Test the connection
  const conn = await db.connect();
  try {
    await conn.query("SELECT 1");
  } finally {
    await conn.close();
  }
  return db as unknown as wasmDuckdb.AsyncDuckDB;
}
