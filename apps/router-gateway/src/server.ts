import { createApp } from './app'
import { loadConfig, validateGatewayRuntimeConfig } from './config'
import { createDependencyHealthMonitor } from './health'

const config = loadConfig()
validateGatewayRuntimeConfig(config)

const dependencyHealthMonitor = createDependencyHealthMonitor(config)
dependencyHealthMonitor.start()

const app = createApp(config, { dependencyHealthMonitor })

Bun.serve({
  hostname: config.host,
  port: config.port,
  fetch: app.fetch
})

console.log(`router-gateway listening on http://${config.host}:${config.port}`)
