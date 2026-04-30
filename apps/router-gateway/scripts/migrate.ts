import { readdir } from 'node:fs/promises'

const databaseUrl = Bun.env.ROUTER_GATEWAY_DATABASE_URL?.trim()
if (!databaseUrl) {
  throw new Error('ROUTER_GATEWAY_DATABASE_URL is required')
}

const sql = new Bun.SQL(databaseUrl)
const migrationsUrl = new URL('../migrations/', import.meta.url)

await sql`
  create table if not exists gateway_schema_migrations (
    filename text primary key,
    applied_at timestamptz not null default now()
  )
`

const filenames = (await readdir(migrationsUrl))
  .filter((filename) => filename.endsWith('.sql'))
  .sort()

for (const filename of filenames) {
  const [{ applied }] = await sql<{ applied: boolean }[]>`
    select exists (
      select 1
      from gateway_schema_migrations
      where filename = ${filename}
    ) as applied
  `

  if (applied) {
    console.log(`already applied ${filename}`)
    continue
  }

  const migrationSql = await Bun.file(new URL(filename, migrationsUrl)).text()
  await sql.begin(async (tx) => {
    await tx(migrationSql)
    await tx`
      insert into gateway_schema_migrations (filename)
      values (${filename})
    `
  })
  console.log(`applied ${filename}`)
}

await sql.close()
