import { createApp } from './app'
import { loadConfig, validateGatewayRuntimeConfig } from './config'
import { migrateGatewayDatabase } from './database/migrations'
import { createDependencyHealthMonitor } from './health'

const config = loadConfig()
validateGatewayRuntimeConfig(config)

if (config.gatewayDatabaseUrl) {
  const result = await migrateGatewayDatabase(config.gatewayDatabaseUrl)
  const summary = [
    result.applied.length ? `applied=${result.applied.join(',')}` : undefined,
    result.skipped.length ? `skipped=${result.skipped.join(',')}` : undefined
  ]
    .filter(Boolean)
    .join(' ')
  console.log(`router-gateway migrations complete${summary ? ` ${summary}` : ''}`)
}

const dependencyHealthMonitor = createDependencyHealthMonitor(config)
dependencyHealthMonitor.start()

const app = createApp(config, { dependencyHealthMonitor })

Bun.serve({
  hostname: config.host,
  port: config.port,
  fetch: app.fetch
})

console.log(`router-gateway listening on http://${config.host}:${config.port}`)
