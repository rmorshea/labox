import { defineConfig } from 'vite';
import { resolve } from 'path';
import dts from 'vite-plugin-dts';

export default defineConfig({
  resolve: {
    alias: {
      // plotly.js (source) references Node.js built-ins (e.g. `buffer/`) which
      // leak as unresolvable bare specifiers when bundled in library mode.
      // Point to the pre-built browser bundle instead.
      'plotly.js': 'plotly.js-dist-min',
    },
  },
  esbuild: {
    jsx: 'automatic',
    jsxImportSource: 'preact',
  },
  plugins: [
    dts({
      include: ['src/**/*'],
      exclude: ['src/**/*.test.ts'],
      rollupTypes: true,
    }),
  ],
  build: {
    lib: {
      entry: resolve(__dirname, 'src/index.ts'),
      name: 'Labox',
      formats: ['es'],
      fileName: (format) => `index.js`,
    },
    rollupOptions: {
      external: [],
      output: {
        preserveModules: false,
      },
    },
    outDir: 'dist',
    sourcemap: true,
    minify: false,
  },
});
