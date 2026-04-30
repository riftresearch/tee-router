import react from '@vitejs/plugin-react'
import { defineConfig, loadEnv } from 'vite'

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '')
  const apiPort = Number(env.ADMIN_DASHBOARD_API_PORT ?? env.PORT ?? 3000)
  const webPort = Number(env.ADMIN_DASHBOARD_WEB_PORT ?? 5173)

  return {
    plugins: [react()],
    server: {
      host: '0.0.0.0',
      port: webPort,
      proxy: {
        '/api': {
          target: `http://127.0.0.1:${apiPort}`,
          changeOrigin: true
        }
      }
    },
    build: {
      outDir: 'dist',
      sourcemap: true
    }
  }
})
