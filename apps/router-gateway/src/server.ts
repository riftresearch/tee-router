import { createApp } from './app'
import { loadConfig } from './config'

const config = loadConfig()
const app = createApp(config)

Bun.serve({
  hostname: config.host,
  port: config.port,
  fetch: app.fetch
})

console.log(`router-gateway listening on http://${config.host}:${config.port}`)
