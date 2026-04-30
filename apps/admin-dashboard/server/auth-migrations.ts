import { getMigrations } from 'better-auth/db/migration'

import { createDashboardAuth } from './auth'
import type { AdminDashboardConfig } from './config'

export type AuthMigrationResult =
  | {
      status: 'skipped'
      missing: string[]
    }
  | {
      status: 'unchanged'
    }
  | {
      status: 'migrated'
      created: number
      altered: number
    }

export async function runAuthMigrations(
  config: AdminDashboardConfig
): Promise<AuthMigrationResult> {
  const runtime = createDashboardAuth(config)

  if (!runtime) {
    return {
      status: 'skipped',
      missing: config.missingAuthConfig
    }
  }

  try {
    const { toBeCreated, toBeAdded, runMigrations } = await getMigrations(
      runtime.auth.options as never
    )

    if (toBeCreated.length === 0 && toBeAdded.length === 0) {
      return { status: 'unchanged' }
    }

    await runMigrations()
    return {
      status: 'migrated',
      created: toBeCreated.length,
      altered: toBeAdded.length
    }
  } finally {
    await runtime.close()
  }
}
