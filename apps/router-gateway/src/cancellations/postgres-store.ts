import type {
  CancellationStore,
  SaveManagedCancellationInput,
  StoredManagedCancellation
} from './store'

type GatewayCancellationRow = {
  router_order_id: string
  auth_mode: 'gateway_managed_token'
  auth_policy: Record<string, unknown> | string
  encrypted_cancellation_secret: string
  gateway_token_hash: string
  cancellation_requested_at: Date | string | null
  created_at: Date | string
  updated_at: Date | string
}

export class BunSqlCancellationStore implements CancellationStore {
  constructor(private readonly sql: Bun.SQL) {}

  async saveManagedCancellation(input: SaveManagedCancellationInput): Promise<void> {
    await this.sql`
      insert into gateway_managed_cancellations (
        router_order_id,
        auth_mode,
        auth_policy,
        encrypted_cancellation_secret,
        gateway_token_hash
      ) values (
        ${input.routerOrderId},
        ${input.authMode},
        ${JSON.stringify(input.authPolicy)}::jsonb,
        ${input.encryptedCancellationSecret},
        ${input.gatewayTokenHash}
      )
      on conflict (router_order_id) do update set
        auth_mode = excluded.auth_mode,
        auth_policy = excluded.auth_policy,
        encrypted_cancellation_secret = excluded.encrypted_cancellation_secret,
        gateway_token_hash = excluded.gateway_token_hash,
        updated_at = now()
    `
  }

  async findManagedCancellation(
    routerOrderId: string
  ): Promise<StoredManagedCancellation | undefined> {
    const rows = await this.sql<GatewayCancellationRow[]>`
      select
        router_order_id,
        auth_mode,
        auth_policy,
        encrypted_cancellation_secret,
        gateway_token_hash,
        cancellation_requested_at,
        created_at,
        updated_at
      from gateway_managed_cancellations
      where router_order_id = ${routerOrderId}
      limit 1
    `
    const row = rows[0]
    if (!row) return undefined

    return mapRow(row)
  }

  async markCancellationRequested(routerOrderId: string): Promise<void> {
    await this.sql`
      update gateway_managed_cancellations
      set
        cancellation_requested_at = now(),
        updated_at = now()
      where router_order_id = ${routerOrderId}
    `
  }
}

function mapRow(row: GatewayCancellationRow): StoredManagedCancellation {
  return {
    routerOrderId: row.router_order_id,
    authMode: row.auth_mode,
    authPolicy:
      typeof row.auth_policy === 'string'
        ? (JSON.parse(row.auth_policy) as Record<string, unknown>)
        : row.auth_policy,
    encryptedCancellationSecret: row.encrypted_cancellation_secret,
    gatewayTokenHash: row.gateway_token_hash,
    ...(row.cancellation_requested_at
      ? { cancellationRequestedAt: toIso(row.cancellation_requested_at) }
      : {}),
    createdAt: toIso(row.created_at),
    updatedAt: toIso(row.updated_at)
  }
}

function toIso(value: Date | string): string {
  return value instanceof Date ? value.toISOString() : value
}
