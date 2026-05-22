import type { GatewayConfig } from '../config'
import { GatewayConfigurationError } from '../errors'
import { RouterClient, type FetchLike } from '../internal/router-client'
import type { DependencyHealthMonitor } from '../health'

export type GatewayDeps = {
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
