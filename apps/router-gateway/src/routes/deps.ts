import type { GatewayConfig } from '../config'
import { GatewayConfigurationError } from '../errors'
import { RouterClient, type FetchLike } from '../internal/router-client'
import { CancellationSecretBox } from '../cancellations/crypto'
import { BunSqlCancellationStore } from '../cancellations/postgres-store'
import { CancellationService } from '../cancellations/service'

export type GatewayDeps = {
  cancellationService?: CancellationService
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

export function cancellationServiceFor(
  config: GatewayConfig,
  deps: GatewayDeps = {}
): CancellationService {
  if (deps.cancellationService) return deps.cancellationService

  if (!config.gatewayDatabaseUrl) {
    throw new GatewayConfigurationError('ROUTER_GATEWAY_DATABASE_URL is not configured')
  }

  if (!config.cancellationSecretKey) {
    throw new GatewayConfigurationError(
      'ROUTER_GATEWAY_CANCELLATION_SECRET_KEY is not configured'
    )
  }

  return new CancellationService(
    new BunSqlCancellationStore(new Bun.SQL(config.gatewayDatabaseUrl)),
    CancellationSecretBox.fromKeyMaterial(config.cancellationSecretKey)
  )
}
