# etl-ui

Browser-based canvas for wiring CSV / HTTP sources through transforms (filter, join, aggregate, pivot, and others) and into visualizations or CSV download. Graphs and node settings are stored locally in **IndexedDB** (database name `etl-ui`).

## Requirements

- [Bun](https://bun.sh/) (lockfile is `bun.lock`)

## Scripts

| Command | Description |
| --- | --- |
| `bun install` | Install dependencies |
| `bun run dev` | Vite dev server with HMR |
| `bun run build` | Typecheck and production build |
| `bun run test` | Vitest suite |
| `bun run lint` | Oxlint with autofix |
| `bun run lint:check` | Oxlint without writes (CI) |
| `bun run format` | Oxfmt |
| `bun run preview` | Preview production build |

## Workspace files

Use **Export** in the toolbar to download the current graph as JSON (same schema as persistence). **Import** replaces the active workspace graph after confirmation and saves to IndexedDB.

## Keyboard shortcuts

When focus is not in an input or textarea: **⌘/Ctrl+Z** undo, **⇧⌘Z** / **Ctrl+Y** redo, **⌫** / **Delete** removes selected nodes and edges, **⌘0** / **Ctrl+0** or **F** fits the view.

## CI

GitHub Actions runs install, `lint:check`, tests, and build on push and pull requests (see `.github/workflows/ci.yml`).
