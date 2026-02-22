# Labox JavaScript/TypeScript Library

TypeScript library for labox, built with Bun and Vite.

## Development

### Prerequisites

- [Bun](https://bun.sh/) installed on your system

### Setup

Install dependencies:

```bash
bun install
```

### Build

Build the library:

```bash
bun run build
```

The output will be generated in the `dist/` directory as JavaScript files.

### Development Mode

Watch mode for development:

```bash
bun run dev
```

### Type Checking

Run TypeScript type checking:

```bash
bun run type-check
```

## Project Structure

```
javascript/
├── src/
│   ├── index.ts           # Public entry point (re-exports)
│   ├── types.ts           # ContentRecord interface
│   ├── api.ts             # Server fetch helpers
│   ├── render.tsx         # renderContent() implementation
│   └── renderers/
│       ├── types.ts       # Renderer interface
│       ├── index.ts       # RENDERERS array + findRenderer()
│       ├── json.tsx       # application/json renderer
│       ├── csv.tsx        # text/csv renderer
│       ├── image.tsx      # image/* renderer
│       ├── plotly.tsx     # application/vnd.plotly.v1+json renderer
│       └── fallback.tsx   # Catch-all renderer
├── dist/          # Build output (generated)
├── package.json   # Project configuration
├── tsconfig.json  # TypeScript configuration
└── vite.config.ts # Vite build configuration
```

## Usage

```typescript
import { renderContent } from 'labox-js';

// Fetch and render a content record into a DOM element.
await renderContent(
  'c1d2e3f4-...',           // content UUID
  'https://your-server',    // labox server base URL
  document.getElementById('root')!,
);
```

### Custom renderers

Pass your own renderer objects ahead of the defaults to override or extend behaviour:

```tsx
// my-renderer.tsx
import { renderContent, buildRendererMap, RENDERERS } from 'labox-js';
import type { Renderer } from 'labox-js';

const myRenderer: Renderer = {
  types: ['application/x-my-type'],
  render: (data, record) => (
    <div class="my-widget">{record.content_key}</div>
  ),
};

// Build the map once, reuse it across renderContent calls.
const rendererMap = buildRendererMap([myRenderer, ...RENDERERS]);

await renderContent(id, baseUrl, container, rendererMap);
```

## CSS Class Names

The library adds no inline styles. Use these class names to style the output from your application.

| Class name | Element | Detail |
|---|---|---|
| `labox-loading` | `div` | Loading state |
| `labox-error` | `div` | Error state |
| `labox-content` | wrapper `div` | All renderers |
| `labox-content--json` | wrapper `div` | JSON |
| `labox-content--csv` | wrapper `div` | CSV |
| `labox-content--image` | wrapper `div` | Image (`image/png`, `image/jpeg`, `image/gif`, `image/webp`, `image/svg+xml`, `image/bmp`, `image/tiff`, `image/avif`, `image/apng`, `image/x-icon`) |
| `labox-content--plotly` | wrapper `div` | Plotly |
| `labox-content--fallback` | wrapper `div` | Fallback |
| `labox-json` | `pre` | JSON — wraps the `<code>` block |
| `labox-csv-table` | `table` | CSV |
| `labox-csv-thead` | `thead` | CSV — header row group |
| `labox-csv-tbody` | `tbody` | CSV — body row group |
| `labox-csv-tr` | `tr` | CSV — every row (header and body) |
| `labox-csv-th` | `th` | CSV — header cell |
| `labox-csv-td` | `td` | CSV — body cell |
| `labox-image` | `img` | Image |
| `labox-plotly` | `div` | Plotly — Plotly.js mounts here |
| `labox-fallback` | `dl` | Fallback — description list |
