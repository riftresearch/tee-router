import { readdir } from 'node:fs/promises'

const migrationsUrl = new URL('../../migrations/', import.meta.url)

// Two signed 32-bit keys for Postgres transaction-scoped advisory locking.
const MIGRATION_LOCK_NAMESPACE = 0x74726777
const MIGRATION_LOCK_ID = 0x6d696772

export type GatewayMigrationResult = {
  applied: string[]
  skipped: string[]
}

export async function migrateGatewayDatabase(
  databaseUrl: string
): Promise<GatewayMigrationResult> {
  const sql = new Bun.SQL(databaseUrl)

  try {
    return await runGatewayMigrations(sql)
  } finally {
    await sql.close()
  }
}

export async function runGatewayMigrations(
  sql: Bun.SQL
): Promise<GatewayMigrationResult> {
  const filenames = (await readdir(migrationsUrl))
    .filter((filename) => filename.endsWith('.sql'))
    .sort()

  return await sql.begin(async (tx) => {
    await tx`
      select pg_advisory_xact_lock(${MIGRATION_LOCK_NAMESPACE}, ${MIGRATION_LOCK_ID})
    `

    await tx`
      create table if not exists gateway_schema_migrations (
        filename text primary key,
        applied_at timestamptz not null default now()
      )
    `

    const result: GatewayMigrationResult = {
      applied: [],
      skipped: []
    }

    for (const filename of filenames) {
      const [{ applied }] = await tx<{ applied: boolean }[]>`
        select exists (
          select 1
          from gateway_schema_migrations
          where filename = ${filename}
        ) as applied
      `

      if (applied) {
        result.skipped.push(filename)
        continue
      }

      const migrationSql = await Bun.file(new URL(filename, migrationsUrl)).text()
      await tx.unsafe(migrationSql)
      await tx`
        insert into gateway_schema_migrations (filename)
        values (${filename})
      `
      result.applied.push(filename)
    }

    return result
  })
}
