import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  build: {
    rollupOptions: {
      output: {
        entryFileNames: 'widget.js',
        assetFileNames: 'widget.[ext]',
        chunkFileNames: 'widget-chunk.js',
      }
    }
  }
});
