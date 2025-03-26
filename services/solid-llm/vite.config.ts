import { defineConfig } from 'vite';
import solidPlugin from 'vite-plugin-solid';

export default defineConfig({
  plugins: [
    solidPlugin(),
  ],
  server: {
    port: 3000,
    allowedHosts: [
        "simple-llm.com",
        "llm-chat.com",
    ],
  },
  build: {
    target: 'esnext',
  },
  optimizeDeps: {
    include: ['solid-js', 'solid-start'],
  },
});
