import { defineConfig } from 'vite';
import vue from '@vitejs/plugin-vue';
import Components from 'unplugin-vue-components/vite';
import {PrimeVueResolver} from '@primevue/auto-import-resolver';

// https://vite.dev/config/
export default defineConfig({
  plugins: [
    vue(),
    Components({
      resolvers: [
        PrimeVueResolver()
      ]
    })],
    server: {
      proxy: {
        '/api/': {
          target: 'http://backend-dev:8080/',
          changeOrigin: false,
        },
        '/maps/': {
          target: 'http://martin:3000/',
          changeOrigin: false,
          rewrite: (path) => path.replace(/^\/maps/, ''),
        }
      },
    }
});
