import { Pool } from 'pg'

import type { AdminDashboardConfig } from './config'

export type ReplicaDatabaseRuntime = {
  pool: Pool
  close: () => Promise<void>
}

export function createReplicaDatabase(
  config: AdminDashboardConfig
): ReplicaDatabaseRuntime | null {
  if (!config.replicaDatabaseUrl) return null

  const pool = new Pool({
    connectionString: config.replicaDatabaseUrl,
    max: 5,
    application_name: 'rift-admin-dashboard-replica'
  })

  return {
    pool,
    close: () => pool.end()
  }
}
