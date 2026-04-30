import type { GatewayConfig } from '../config'
import { GatewayConfigurationError } from '../errors'
import { RouterClient, type FetchLike } from '../internal/router-client'
import { RouterCancellationSecretBox } from '../cancellations/crypto'
import { BunSqlRefundAuthorizationStore } from '../cancellations/postgres-store'
import { RefundAuthorizationService } from '../cancellations/service'

export type GatewayDeps = {
  refundAuthorizationService?: RefundAuthorizationService
  fetch?: FetchLike
  routerClient?: RouterClient
}

export function routerClientFor(
  config: GatewayConfig,
  deps: GatewayDeps = {}
): RouterClient {
  if (deps.routerClient) return deps.routerClient

  if (!config.routerInternalBaseUrl) {
    throw new GatewayConfigurationError('ROUTER_INTERNAL_BASE_URL is not configured')
  }

  return new RouterClient({
    baseUrl: config.routerInternalBaseUrl,
    fetch: deps.fetch,
    timeoutMs: config.requestTimeoutMs
  })
}

export function refundAuthorizationServiceFor(
  config: GatewayConfig,
  deps: GatewayDeps = {}
): RefundAuthorizationService {
  if (deps.refundAuthorizationService) return deps.refundAuthorizationService

  if (!config.gatewayDatabaseUrl) {
    throw new GatewayConfigurationError('ROUTER_GATEWAY_DATABASE_URL is not configured')
  }

  if (!config.cancellationSecretKey) {
    throw new GatewayConfigurationError(
      'ROUTER_GATEWAY_CANCELLATION_SECRET_KEY is not configured'
    )
  }

  return new RefundAuthorizationService(
    new BunSqlRefundAuthorizationStore(new Bun.SQL(config.gatewayDatabaseUrl)),
    RouterCancellationSecretBox.fromKeyMaterial(config.cancellationSecretKey)
  )
}
