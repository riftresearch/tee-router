export type GatewayConfig = {
  host: string
  port: number
  routerInternalBaseUrl?: string
  routerQueryApiBaseUrl?: string
  gatewayDatabaseUrl?: string
  cancellationSecretKey?: string
  requestTimeoutMs: number
  version: string
}

type Env = Record<string, string | undefined>

const DEFAULT_PORT = 3000
const DEFAULT_REQUEST_TIMEOUT_MS = 30_000
const DEFAULT_VERSION = '0.1.0'

export function loadConfig(env: Env = Bun.env as Env): GatewayConfig {
  return {
    host: env.HOST ?? '0.0.0.0',
    port: parsePort(env.PORT),
    routerInternalBaseUrl: normalizeOptionalUrl(env.ROUTER_INTERNAL_BASE_URL),
    routerQueryApiBaseUrl: normalizeOptionalUrl(env.ROUTER_QUERY_API_BASE_URL),
    gatewayDatabaseUrl: normalizeOptionalUrl(env.ROUTER_GATEWAY_DATABASE_URL),
    cancellationSecretKey: normalizeOptionalSecret(
      env.ROUTER_GATEWAY_CANCELLATION_SECRET_KEY
    ),
    requestTimeoutMs: parsePositiveInteger(
      env.ROUTER_GATEWAY_REQUEST_TIMEOUT_MS,
      DEFAULT_REQUEST_TIMEOUT_MS,
      'ROUTER_GATEWAY_REQUEST_TIMEOUT_MS'
    ),
    version: env.npm_package_version ?? DEFAULT_VERSION
  }
}

function parsePort(value: string | undefined): number {
  if (!value) return DEFAULT_PORT

  const port = Number(value)
  if (!Number.isInteger(port) || port <= 0 || port > 65535) {
    throw new Error(`Invalid PORT: ${value}`)
  }

  return port
}

function parsePositiveInteger(
  value: string | undefined,
  defaultValue: number,
  name: string
): number {
  if (!value) return defaultValue

  const parsed = Number(value)
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`Invalid ${name}: ${value}`)
  }

  return parsed
}

function normalizeOptionalUrl(value: string | undefined): string | undefined {
  const trimmed = value?.trim()
  if (!trimmed) return undefined

  return trimmed.replace(/\/+$/, '')
}

function normalizeOptionalSecret(value: string | undefined): string | undefined {
  const trimmed = value?.trim()
  return trimmed ? trimmed : undefined
}
