import type { CsvPayload } from "../types/flow";

/** Same data as `public/template.csv` — keep in sync when the template changes. */
export const DEMO_TEMPLATE_CSV: CsvPayload = {
  headers: ["id", "name", "region", "amount"],
  rows: [
    { id: "1", name: "Alpha", region: "North", amount: "120.50" },
    { id: "2", name: "Beta", region: "South", amount: "89.00" },
    { id: "3", name: "Gamma", region: "East", amount: "200.25" },
  ],
};
