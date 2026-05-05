import type { GatewayConfig } from '../config'
import { GatewayConfigurationError } from '../errors'
import { RouterClient, type FetchLike } from '../internal/router-client'
import { RouterCancellationSecretBox } from '../cancellations/crypto'
import { BunSqlRefundAuthorizationStore } from '../cancellations/postgres-store'
import {
  DEFAULT_CANCELLATION_CLAIM_TIMEOUT_MS,
  RefundAuthorizationService
} from '../cancellations/service'
import type { DependencyHealthMonitor } from '../health'

export type GatewayDeps = {
  refundAuthorizationService?: RefundAuthorizationService
  fetch?: FetchLike
  routerClient?: RouterClient
  dependencyHealthMonitor?: DependencyHealthMonitor
}

export function routerClientFor(
  config: GatewayConfig,
  deps: GatewayDeps = {}
): RouterClient {
  if (deps.routerClient) return deps.routerClient

  if (!config.routerInternalBaseUrl) {
    throw new GatewayConfigurationError('ROUTER_INTERNAL_BASE_URL is not configured')
  }

  deps.routerClient = new RouterClient({
    baseUrl: config.routerInternalBaseUrl,
    apiKey: config.routerInternalApiKey,
    fetch: deps.fetch,
    timeoutMs: config.requestTimeoutMs
  })
  return deps.routerClient
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

  deps.refundAuthorizationService = new RefundAuthorizationService(
    new BunSqlRefundAuthorizationStore(new Bun.SQL(config.gatewayDatabaseUrl)),
    RouterCancellationSecretBox.fromKeyMaterial(config.cancellationSecretKey),
    {
      cancellationClaimTimeoutMs: cancellationClaimTimeoutMsForRequestTimeout(
        config.requestTimeoutMs
      ),
      publicBaseUrl: config.publicBaseUrl
    }
  )
  return deps.refundAuthorizationService
}

function cancellationClaimTimeoutMsForRequestTimeout(requestTimeoutMs: number) {
  const timeoutWithSlack = requestTimeoutMs + 60_000
  return Math.max(DEFAULT_CANCELLATION_CLAIM_TIMEOUT_MS, timeoutWithSlack)
}
