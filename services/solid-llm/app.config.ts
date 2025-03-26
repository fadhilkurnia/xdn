import { defineConfig } from "@solidjs/start/config";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig({
    vite: {
        plugins: [tailwindcss()]
    },
    optimizeDeps: {
        include: ['solid-markdown > micromark', 'solid-markdown > unified'],
    },
});
