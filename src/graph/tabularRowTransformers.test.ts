import { describe, expect, it } from "vitest";
import { applyCastToPayload } from "../cast/applyCast";
import { applyComputeRow } from "../computeColumn/template";
import { applyConstantColumns } from "../constantColumn/applyConstantColumns";
import { applyFillReplaceToPayload } from "../fillReplace/applyFillReplace";
import type { ComputeColumnDef } from "../types/flow";
import { applyHttpColumnRenames } from "./tabularCsvRename";
import {
  compileCastColumns,
  compileComputeColumns,
  compileConstantColumns,
  compileFillReplace,
  compileHttpColumnRenames,
  compileSelectColumns,
} from "./tabularRowTransformers";

describe("tabularRowTransformers", () => {
  it("compileCastColumns matches payload cast behavior", () => {
    const headers = ["id", "n", "flag"];
    const row = { id: "7", n: " 3.9 ", flag: "yes" };
    const rules = [
      { column: "n", target: "integer" as const },
      { column: "flag", target: "boolean" as const },
    ];
    const transform = compileCastColumns(headers, rules);
    expect(transform).not.toBeNull();
    const compiled = transform!(row);
    const payload = applyCastToPayload({ headers, rows: [row] }, rules).rows[0];
    expect(compiled).toEqual(payload);
  });

  it("compileFillReplace matches payload fill/replace behavior", () => {
    const headers = ["a", "b", "c"];
    const row = { a: " ", b: "x", c: "x" };
    const fills = [{ id: "f1", column: "a", fillValue: "filled" }];
    const replacements = [
      { id: "r1", column: null, from: "x", to: "y" },
      { id: "r2", column: "a", from: "filled", to: "z" },
    ];
    const transform = compileFillReplace(headers, fills, replacements);
    expect(transform).not.toBeNull();
    const compiled = transform!(row);
    const payload = applyFillReplaceToPayload({ headers, rows: [row] }, fills, replacements)
      .rows[0];
    expect(compiled).toEqual(payload);
  });

  it("compileHttpColumnRenames matches payload rename behavior", () => {
    const headers = ["First Name", "Last Name"];
    const row = { "First Name": "Ada", "Last Name": "Lovelace" };
    const renames = [
      { id: "r1", fromColumn: "First Name", toColumn: "FirstName" },
      { id: "r2", fromColumn: "Last Name", toColumn: "LastName" },
    ];
    const compiled = compileHttpColumnRenames(headers, renames);
    const payload = applyHttpColumnRenames({ headers, rows: [row] }, renames);
    expect(compiled.headers).toEqual(payload.headers);
    expect(compiled.transform!(row)).toEqual(payload.rows[0]);
  });

  it("compileConstantColumns matches payload constant behavior", () => {
    const headers = ["id"];
    const row = { id: "1" };
    const constants = [
      { columnName: "tag", value: "new" },
      { columnName: "id", value: "override" },
    ];
    const compiled = compileConstantColumns(headers, constants);
    const payload = applyConstantColumns({ headers, rows: [row] }, constants);
    expect(compiled.headers).toEqual(payload.headers);
    expect(compiled.transform!(row)).toEqual(payload.rows[0]);
  });

  it("compileComputeColumns preserves chained compute semantics", () => {
    const headers = ["x"];
    const row = { x: "2" };
    const defs: ComputeColumnDef[] = [
      { id: "c1", outputName: "y", expression: "{{x}}+3" },
      { id: "c2", outputName: "z", expression: "{{y}}*2" },
    ];
    const compiled = compileComputeColumns(headers, defs);
    const payload = applyComputeRow(row, headers, defs).row;
    expect(compiled.transform).not.toBeNull();
    expect(compiled.transform!(row)).toEqual(payload);
  });

  it("compileSelectColumns drops missing and preserves order", () => {
    const compiled = compileSelectColumns(["a", "b", "c"], ["c", "missing", "a"]);
    expect(compiled.headers).toEqual(["c", "a"]);
    expect(compiled.transform).not.toBeNull();
    expect(compiled.transform!({ a: "1", b: "2", c: "3" })).toEqual({ c: "3", a: "1" });
  });
});
