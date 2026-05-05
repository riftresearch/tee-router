import { createApp } from './app'
import { runAuthMigrations } from './auth-migrations'
import { loadConfig, validateRuntimeConfig } from './config'

const config = loadConfig()
validateRuntimeConfig(config)

const migrationResult = await runAuthMigrations(config)
if (migrationResult.status === 'migrated') {
  console.log(
    `better-auth migrations complete created=${migrationResult.created} altered=${migrationResult.altered}`
  )
} else if (migrationResult.status === 'unchanged') {
  console.log('better-auth schema is up to date')
} else if (migrationResult.reason === 'development_auth_bypass') {
  console.log('better-auth migrations skipped; development auth bypass is enabled')
} else {
  console.warn(
    `better-auth migrations skipped missing=${migrationResult.missing.join(',')}`
  )
}

const runtime = createApp(config)

const server = Bun.serve({
  hostname: config.host,
  port: config.port,
  fetch: runtime.app.fetch
})

const shutdown = async () => {
  server.stop(true)
  await runtime.close()
}

process.on('SIGINT', () => {
  void shutdown().finally(() => process.exit(0))
})
process.on('SIGTERM', () => {
  void shutdown().finally(() => process.exit(0))
})

console.log(
  `admin-dashboard api listening on http://${config.host}:${config.port}`
)
