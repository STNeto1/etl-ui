export function quoteSqlIdent(value: string): string {
  return `"${value.replaceAll('"', '""')}"`;
}

export function quoteSqlString(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}
