import { defineConfig } from 'vite';
import { resolve } from 'path';
import dts from 'vite-plugin-dts';

export default defineConfig({
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
      output: {
        preserveModules: false,
      },
    },
    outDir: 'dist',
    sourcemap: true,
    minify: false,
  },
});
