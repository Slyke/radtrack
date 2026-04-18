import { defineConfig } from 'vite';
import { sveltekit } from '@sveltejs/kit/vite';

const parsedPort = Number.parseInt(process.env.PORT ?? '4096', 10);
const watchPollingInterval = Number.parseInt(process.env.CHOKIDAR_INTERVAL ?? '300', 10);
const usePolling = process.env.CHOKIDAR_USEPOLLING === 'true';
const internalApiUrl = (process.env.INTERNAL_API_URL ?? '').trim().replace(/\/$/, '');

export default defineConfig({
  plugins: [sveltekit()],
  server: {
    host: '0.0.0.0',
    port: Number.isFinite(parsedPort) ? parsedPort : 4096,
    watch: usePolling
      ? {
          usePolling: true,
          interval: Number.isFinite(watchPollingInterval) ? watchPollingInterval : 300
        }
      : undefined,
    proxy: internalApiUrl
      ? {
          '/api': {
            target: internalApiUrl,
            changeOrigin: true
          },
          '/auth': {
            target: internalApiUrl,
            changeOrigin: true
          }
        }
      : undefined
  }
});
