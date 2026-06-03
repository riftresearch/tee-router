import type { GatewayConfig } from '../config'
import { GatewayConfigurationError } from '../errors'
import { RouterClient, type FetchLike } from '../internal/router-client'
import type { DependencyHealthMonitor } from '../health'
import {
  createDecimalsResolver,
  type DecimalsResolver
} from '../token-decimals'

export type GatewayDeps = {
  fetch?: FetchLike
  routerClient?: RouterClient
  dependencyHealthMonitor?: DependencyHealthMonitor
  decimalsResolver?: DecimalsResolver
}

export function decimalsResolverFor(
  config: GatewayConfig,
  deps: GatewayDeps = {}
): DecimalsResolver {
  if (deps.decimalsResolver) return deps.decimalsResolver
  deps.decimalsResolver = createDecimalsResolver(config)
  return deps.decimalsResolver
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
