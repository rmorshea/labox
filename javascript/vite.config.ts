import { defineConfig } from 'vite';
import { resolve } from 'path';
import dts from 'vite-plugin-dts';

export default defineConfig({
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
