import type {
  RefundAuthorizationStore,
  RefundMode,
  SaveRefundAuthorizationInput,
  StoredRefundAuthorization
} from './store'

type GatewayRefundAuthorizationRow = {
  router_order_id: string
  refund_mode: RefundMode
  refund_authorizer: string | null
  encrypted_cancellation_secret: string
  refund_token_hash: string | null
  cancellation_requested_at: Date | string | null
  created_at: Date | string
  updated_at: Date | string
}

export class BunSqlRefundAuthorizationStore implements RefundAuthorizationStore {
  constructor(private readonly sql: Bun.SQL) {}

  async saveRefundAuthorization(input: SaveRefundAuthorizationInput): Promise<void> {
    await this.sql`
      insert into gateway_refund_authorizations (
        router_order_id,
        refund_mode,
        refund_authorizer,
        encrypted_cancellation_secret,
        refund_token_hash
      ) values (
        ${input.routerOrderId},
        ${input.refundMode},
        ${input.refundAuthorizer},
        ${input.encryptedCancellationSecret},
        ${input.refundTokenHash ?? null}
      )
      on conflict (router_order_id) do update set
        refund_mode = excluded.refund_mode,
        refund_authorizer = excluded.refund_authorizer,
        encrypted_cancellation_secret = excluded.encrypted_cancellation_secret,
        refund_token_hash = excluded.refund_token_hash,
        updated_at = now()
    `
  }

  async findRefundAuthorization(
    routerOrderId: string
  ): Promise<StoredRefundAuthorization | undefined> {
    const rows = await this.sql<GatewayRefundAuthorizationRow[]>`
      select
        router_order_id,
        refund_mode,
        refund_authorizer,
        encrypted_cancellation_secret,
        refund_token_hash,
        cancellation_requested_at,
        created_at,
        updated_at
      from gateway_refund_authorizations
      where router_order_id = ${routerOrderId}
      limit 1
    `
    const row = rows[0]
    if (!row) return undefined

    return mapRow(row)
  }

  async markCancellationRequested(routerOrderId: string): Promise<void> {
    await this.sql`
      update gateway_refund_authorizations
      set
        cancellation_requested_at = now(),
        updated_at = now()
      where router_order_id = ${routerOrderId}
    `
  }
}

function mapRow(row: GatewayRefundAuthorizationRow): StoredRefundAuthorization {
  return {
    routerOrderId: row.router_order_id,
    refundMode: row.refund_mode,
    refundAuthorizer: row.refund_authorizer,
    encryptedCancellationSecret: row.encrypted_cancellation_secret,
    ...(row.refund_token_hash ? { refundTokenHash: row.refund_token_hash } : {}),
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
