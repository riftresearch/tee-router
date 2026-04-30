export type HealthTargetConfig = {
  name: string
  url: string
  method: 'GET' | 'HEAD'
  timeoutMs: number
}

export type GatewayConfig = {
  host: string
  port: number
  routerInternalBaseUrl?: string
  routerQueryApiBaseUrl?: string
  publicBaseUrl?: string
  gatewayDatabaseUrl?: string
  cancellationSecretKey?: string
  requestTimeoutMs: number
  healthTargets: HealthTargetConfig[]
  healthPollIntervalMs: number
  healthTargetTimeoutMs: number
  version: string
}

type Env = Record<string, string | undefined>

const DEFAULT_PORT = 3000
const DEFAULT_REQUEST_TIMEOUT_MS = 30_000
const DEFAULT_HEALTH_POLL_INTERVAL_MS = 30_000
const DEFAULT_HEALTH_TARGET_TIMEOUT_MS = 5_000
const DEFAULT_VERSION = '0.1.0'

export function loadConfig(env: Env = Bun.env as Env): GatewayConfig {
  const routerInternalBaseUrl = normalizeOptionalUrl(env.ROUTER_INTERNAL_BASE_URL)
  const routerQueryApiBaseUrl = normalizeOptionalUrl(env.ROUTER_QUERY_API_BASE_URL)
  const healthTargetTimeoutMs = parsePositiveInteger(
    env.ROUTER_GATEWAY_HEALTH_TARGET_TIMEOUT_MS,
    DEFAULT_HEALTH_TARGET_TIMEOUT_MS,
    'ROUTER_GATEWAY_HEALTH_TARGET_TIMEOUT_MS'
  )

  return {
    host: env.HOST ?? '0.0.0.0',
    port: parsePort(env.PORT),
    routerInternalBaseUrl,
    routerQueryApiBaseUrl,
    publicBaseUrl: normalizeOptionalUrl(env.ROUTER_GATEWAY_PUBLIC_BASE_URL),
    gatewayDatabaseUrl: normalizeOptionalUrl(env.ROUTER_GATEWAY_DATABASE_URL),
    cancellationSecretKey: normalizeOptionalSecret(
      env.ROUTER_GATEWAY_CANCELLATION_SECRET_KEY
    ),
    requestTimeoutMs: parsePositiveInteger(
      env.ROUTER_GATEWAY_REQUEST_TIMEOUT_MS,
      DEFAULT_REQUEST_TIMEOUT_MS,
      'ROUTER_GATEWAY_REQUEST_TIMEOUT_MS'
    ),
    healthTargets: [
      ...defaultHealthTargets({
        routerInternalBaseUrl,
        routerQueryApiBaseUrl,
        timeoutMs: healthTargetTimeoutMs
      }),
      ...parseHealthTargets(
        env.ROUTER_GATEWAY_HEALTH_TARGETS,
        healthTargetTimeoutMs
      )
    ],
    healthPollIntervalMs: parsePositiveInteger(
      env.ROUTER_GATEWAY_HEALTH_POLL_INTERVAL_MS,
      DEFAULT_HEALTH_POLL_INTERVAL_MS,
      'ROUTER_GATEWAY_HEALTH_POLL_INTERVAL_MS'
    ),
    healthTargetTimeoutMs,
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

function defaultHealthTargets(input: {
  routerInternalBaseUrl?: string
  routerQueryApiBaseUrl?: string
  timeoutMs: number
}): HealthTargetConfig[] {
  return [
    ...(input.routerInternalBaseUrl
      ? [
          {
            name: 'router-api',
            url: `${input.routerInternalBaseUrl}/status`,
            method: 'GET' as const,
            timeoutMs: input.timeoutMs
          }
        ]
      : []),
    ...(input.routerQueryApiBaseUrl
      ? [
          {
            name: 'router-query-api',
            url: `${input.routerQueryApiBaseUrl}/status`,
            method: 'GET' as const,
            timeoutMs: input.timeoutMs
          }
        ]
      : [])
  ]
}

function parseHealthTargets(
  value: string | undefined,
  defaultTimeoutMs: number
): HealthTargetConfig[] {
  const trimmed = value?.trim()
  if (!trimmed) return []

  let parsed: unknown
  try {
    parsed = JSON.parse(trimmed)
  } catch (error) {
    const message = error instanceof Error ? error.message : 'invalid JSON'
    throw new Error(`Invalid ROUTER_GATEWAY_HEALTH_TARGETS: ${message}`)
  }

  if (!Array.isArray(parsed)) {
    throw new Error('Invalid ROUTER_GATEWAY_HEALTH_TARGETS: expected an array')
  }

  return parsed.map((item, index) => parseHealthTarget(item, index, defaultTimeoutMs))
}

function parseHealthTarget(
  item: unknown,
  index: number,
  defaultTimeoutMs: number
): HealthTargetConfig {
  if (!isRecord(item)) {
    throw new Error(`Invalid ROUTER_GATEWAY_HEALTH_TARGETS[${index}]: expected object`)
  }

  const name = parseRequiredString(item.name, `ROUTER_GATEWAY_HEALTH_TARGETS[${index}].name`)
  const url = normalizeHealthTargetUrl(
    parseRequiredString(item.url, `ROUTER_GATEWAY_HEALTH_TARGETS[${index}].url`),
    `ROUTER_GATEWAY_HEALTH_TARGETS[${index}].url`
  )
  const method = parseHealthTargetMethod(item.method, index)
  const timeoutMs =
    item.timeoutMs === undefined
      ? defaultTimeoutMs
      : parsePositiveInteger(
          String(item.timeoutMs),
          defaultTimeoutMs,
          `ROUTER_GATEWAY_HEALTH_TARGETS[${index}].timeoutMs`
        )

  return {
    name,
    url,
    method,
    timeoutMs
  }
}

function parseRequiredString(value: unknown, name: string): string {
  if (typeof value !== 'string' || value.trim() === '') {
    throw new Error(`Invalid ${name}: expected non-empty string`)
  }

  return value.trim()
}

function normalizeHealthTargetUrl(value: string, name: string): string {
  try {
    return new URL(value).toString()
  } catch {
    throw new Error(`Invalid ${name}: expected absolute URL`)
  }
}

function parseHealthTargetMethod(
  value: unknown,
  index: number
): HealthTargetConfig['method'] {
  if (value === undefined) return 'GET'
  if (typeof value !== 'string') {
    throw new Error(
      `Invalid ROUTER_GATEWAY_HEALTH_TARGETS[${index}].method: expected GET or HEAD`
    )
  }

  const method = value.toUpperCase()
  if (method !== 'GET' && method !== 'HEAD') {
    throw new Error(
      `Invalid ROUTER_GATEWAY_HEALTH_TARGETS[${index}].method: expected GET or HEAD`
    )
  }

  return method
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}
