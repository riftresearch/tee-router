import type {
  ClaimRefundAuthorizationExpectation,
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
  cancellation_secret_hash: string
  refund_token_hash: string | null
  encrypted_refund_token: string | null
  cancellation_requested_at: Date | string | null
  created_at: Date | string
  updated_at: Date | string
}

export class BunSqlRefundAuthorizationStore implements RefundAuthorizationStore {
  constructor(private readonly sql: Bun.SQL) {}

  async saveRefundAuthorization(
    input: SaveRefundAuthorizationInput,
    claimTimeoutMs: number
  ): Promise<StoredRefundAuthorization | undefined> {
    const rows = await this.sql<GatewayRefundAuthorizationRow[]>`
      insert into gateway_refund_authorizations (
        router_order_id,
        refund_mode,
        refund_authorizer,
        encrypted_cancellation_secret,
        cancellation_secret_hash,
        refund_token_hash,
        encrypted_refund_token
      ) values (
        ${input.routerOrderId},
        ${input.refundMode},
        ${input.refundAuthorizer},
        ${input.encryptedCancellationSecret},
        ${input.cancellationSecretHash},
        ${input.refundTokenHash ?? null},
        ${input.encryptedRefundToken ?? null}
      )
      on conflict (router_order_id) do update set
        cancellation_claimed_at = null,
        cancellation_claim_id = null,
        updated_at = now()
      where gateway_refund_authorizations.cancellation_requested_at is null
        and gateway_refund_authorizations.refund_mode = excluded.refund_mode
        and gateway_refund_authorizations.refund_authorizer is not distinct from excluded.refund_authorizer
        and gateway_refund_authorizations.cancellation_secret_hash = excluded.cancellation_secret_hash
        and (
          gateway_refund_authorizations.cancellation_claimed_at is null
          or gateway_refund_authorizations.cancellation_claimed_at < now() - (${claimTimeoutMs} * interval '1 millisecond')
        )
      returning
        router_order_id,
        refund_mode,
        refund_authorizer,
        encrypted_cancellation_secret,
        cancellation_secret_hash,
        refund_token_hash,
        encrypted_refund_token,
        cancellation_requested_at,
        created_at,
        updated_at
    `
    return rows[0] ? mapRow(rows[0]) : undefined
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
        cancellation_secret_hash,
        refund_token_hash,
        encrypted_refund_token,
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

  async claimCancellationAttempt(
    routerOrderId: string,
    claimId: string,
    claimTimeoutMs: number,
    expected: ClaimRefundAuthorizationExpectation
  ): Promise<boolean> {
    const rows = await this.sql<{ router_order_id: string }[]>`
      update gateway_refund_authorizations
      set
        cancellation_claimed_at = now(),
        cancellation_claim_id = ${claimId},
        updated_at = now()
      where router_order_id = ${routerOrderId}
        and cancellation_requested_at is null
        and refund_mode = ${expected.refundMode}
        and refund_authorizer is not distinct from ${expected.refundAuthorizer}
        and encrypted_cancellation_secret = ${expected.encryptedCancellationSecret}
        and cancellation_secret_hash = ${expected.cancellationSecretHash}
        and refund_token_hash is not distinct from ${expected.refundTokenHash ?? null}
        and encrypted_refund_token is not distinct from ${expected.encryptedRefundToken ?? null}
        and (
          cancellation_claimed_at is null
          or cancellation_claimed_at < now() - (${claimTimeoutMs} * interval '1 millisecond')
        )
      returning router_order_id
    `
    return rows.length === 1
  }

  async releaseCancellationAttempt(
    routerOrderId: string,
    claimId: string
  ): Promise<void> {
    await this.sql`
      update gateway_refund_authorizations
      set
        cancellation_claimed_at = null,
        cancellation_claim_id = null,
        updated_at = now()
      where router_order_id = ${routerOrderId}
        and cancellation_requested_at is null
        and cancellation_claim_id = ${claimId}
    `
  }

  async markCancellationRequested(
    routerOrderId: string,
    claimId: string
  ): Promise<boolean> {
    const rows = await this.sql<{ router_order_id: string }[]>`
      update gateway_refund_authorizations
      set
        cancellation_requested_at = now(),
        cancellation_claimed_at = null,
        cancellation_claim_id = null,
        updated_at = now()
      where router_order_id = ${routerOrderId}
        and cancellation_claim_id = ${claimId}
      returning router_order_id
    `
    return rows.length === 1
  }
}

function mapRow(row: GatewayRefundAuthorizationRow): StoredRefundAuthorization {
  return {
    routerOrderId: row.router_order_id,
    refundMode: row.refund_mode,
    refundAuthorizer: row.refund_authorizer,
    encryptedCancellationSecret: row.encrypted_cancellation_secret,
    cancellationSecretHash: row.cancellation_secret_hash,
    ...(row.refund_token_hash ? { refundTokenHash: row.refund_token_hash } : {}),
    ...(row.encrypted_refund_token
      ? { encryptedRefundToken: row.encrypted_refund_token }
      : {}),
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
