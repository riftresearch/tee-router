import { migrateGatewayDatabase } from '../src/database/migrations'

const databaseUrl = Bun.env.ROUTER_GATEWAY_DATABASE_URL?.trim()
if (!databaseUrl) {
  throw new Error('ROUTER_GATEWAY_DATABASE_URL is required')
}

const result = await migrateGatewayDatabase(databaseUrl)

for (const filename of result.skipped) {
  console.log(`already applied ${filename}`)
}

for (const filename of result.applied) {
  console.log(`applied ${filename}`)
}
