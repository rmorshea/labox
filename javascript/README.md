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
├── src/           # TypeScript source files
│   └── index.ts   # Main entry point
├── dist/          # Build output (generated)
├── package.json   # Project configuration
├── tsconfig.json  # TypeScript configuration
└── vite.config.ts # Vite build configuration
```

## Usage

After building, the library can be imported:

```typescript
import { hello, version } from 'labox-js';

console.log(hello('Developer'));
console.log(`Version: ${version}`);
```
