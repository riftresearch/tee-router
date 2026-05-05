import { RouterCancellationSecretBox } from './cancellations/crypto'
import { GatewayConfigurationError } from './errors'
import type { BitcoinAddressNetwork } from './assets'

export type HealthTargetConfig = {
  name: string
  url: string
  method: 'GET' | 'HEAD'
  timeoutMs: number
  response: 'generic' | 'routerProviderHealth'
  headers?: Record<string, string>
}

export type GatewayConfig = {
  host: string
  port: number
  routerInternalBaseUrl?: string
  routerInternalApiKey?: string
  routerQueryApiBaseUrl?: string
  publicBaseUrl?: string
  gatewayDatabaseUrl?: string
  cancellationSecretKey?: string
  requestTimeoutMs: number
  healthTargets: HealthTargetConfig[]
  healthPollIntervalMs: number
  healthTargetTimeoutMs: number
  bitcoinAddressNetworks: BitcoinAddressNetwork[]
  version: string
}

type Env = Record<string, string | undefined>

const DEFAULT_PORT = 3000
const DEFAULT_REQUEST_TIMEOUT_MS = 30_000
const DEFAULT_HEALTH_POLL_INTERVAL_MS = 30_000
const DEFAULT_HEALTH_TARGET_TIMEOUT_MS = 5_000
const DEFAULT_VERSION = '0.1.0'
const MIN_ROUTER_INTERNAL_API_KEY_LENGTH = 32
const DEFAULT_BITCOIN_ADDRESS_NETWORKS: BitcoinAddressNetwork[] = ['mainnet']
const BITCOIN_ADDRESS_NETWORK_VALUES = new Set<BitcoinAddressNetwork>([
  'mainnet',
  'testnet',
  'regtest'
])
const MAX_TIMER_MS = 2_147_483_647
const POSITIVE_DECIMAL_INTEGER_PATTERN = /^[1-9][0-9]*$/

export function loadConfig(env: Env = Bun.env as Env): GatewayConfig {
  const routerInternalBaseUrl = normalizeOptionalHttpUrl(
    env.ROUTER_INTERNAL_BASE_URL,
    'ROUTER_INTERNAL_BASE_URL'
  )
  const routerInternalApiKey = normalizeOptionalSecret(
    env.ROUTER_GATEWAY_API_KEY,
    MIN_ROUTER_INTERNAL_API_KEY_LENGTH,
    'ROUTER_GATEWAY_API_KEY'
  )
  const routerQueryApiBaseUrl = normalizeOptionalHttpUrl(
    env.ROUTER_QUERY_API_BASE_URL,
    'ROUTER_QUERY_API_BASE_URL'
  )
  const healthTargetTimeoutMs = parsePositiveInteger(
    env.ROUTER_GATEWAY_HEALTH_TARGET_TIMEOUT_MS,
    DEFAULT_HEALTH_TARGET_TIMEOUT_MS,
    'ROUTER_GATEWAY_HEALTH_TARGET_TIMEOUT_MS'
  )

  return {
    host: env.HOST ?? '0.0.0.0',
    port: parsePort(env.PORT),
    routerInternalBaseUrl,
    routerInternalApiKey,
    routerQueryApiBaseUrl,
    publicBaseUrl: normalizeOptionalHttpUrl(
      env.ROUTER_GATEWAY_PUBLIC_BASE_URL,
      'ROUTER_GATEWAY_PUBLIC_BASE_URL'
    ),
    gatewayDatabaseUrl: normalizeOptionalSecret(env.ROUTER_GATEWAY_DATABASE_URL),
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
        routerInternalApiKey,
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
    bitcoinAddressNetworks: parseBitcoinAddressNetworks(
      env.ROUTER_GATEWAY_BITCOIN_ADDRESS_NETWORKS
    ),
    version: env.npm_package_version ?? DEFAULT_VERSION
  }
}

export function validateGatewayRuntimeConfig(config: GatewayConfig): void {
  if (isLoopbackHost(config.host)) {
    if (config.cancellationSecretKey) {
      RouterCancellationSecretBox.fromKeyMaterial(config.cancellationSecretKey)
    }
    return
  }

  if (!config.routerInternalBaseUrl) {
    throw new GatewayConfigurationError(
      'ROUTER_INTERNAL_BASE_URL must be configured when router-gateway binds a non-loopback host'
    )
  }
  if (!config.routerInternalApiKey) {
    throw new GatewayConfigurationError(
      'ROUTER_GATEWAY_API_KEY must be configured when router-gateway binds a non-loopback host'
    )
  }
  if (!config.gatewayDatabaseUrl) {
    throw new GatewayConfigurationError(
      'ROUTER_GATEWAY_DATABASE_URL must be configured when router-gateway binds a non-loopback host'
    )
  }
  if (!config.cancellationSecretKey) {
    throw new GatewayConfigurationError(
      'ROUTER_GATEWAY_CANCELLATION_SECRET_KEY must be configured when router-gateway binds a non-loopback host'
    )
  }
  if (!config.publicBaseUrl) {
    throw new GatewayConfigurationError(
      'ROUTER_GATEWAY_PUBLIC_BASE_URL must be configured when router-gateway binds a non-loopback host'
    )
  }
  RouterCancellationSecretBox.fromKeyMaterial(config.cancellationSecretKey)
}

function parsePort(value: string | undefined): number {
  if (!value) return DEFAULT_PORT

  if (!POSITIVE_DECIMAL_INTEGER_PATTERN.test(value)) {
    throw new Error(`Invalid PORT: ${value}`)
  }

  const port = Number(value)
  if (!Number.isSafeInteger(port) || port > 65535) {
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

  if (!POSITIVE_DECIMAL_INTEGER_PATTERN.test(value)) {
    throw new Error(
      `Invalid ${name}: expected an integer between 1 and ${MAX_TIMER_MS}`
    )
  }

  const parsed = Number(value)
  if (!Number.isSafeInteger(parsed) || parsed > MAX_TIMER_MS) {
    throw new Error(
      `Invalid ${name}: expected an integer between 1 and ${MAX_TIMER_MS}`
    )
  }

  return parsed
}

function normalizeOptionalHttpUrl(
  value: string | undefined,
  name: string
): string | undefined {
  const trimmed = value?.trim()
  if (!trimmed) return undefined

  try {
    const url = new URL(trimmed)
    if (url.protocol !== 'http:' && url.protocol !== 'https:') {
      throw new Error('expected http or https URL')
    }
    if (url.username || url.password) {
      throw new Error('URL credentials are not allowed')
    }
    if (url.search || url.hash) {
      throw new Error('query strings and fragments are not allowed')
    }
    return url.toString().replace(/\/+$/, '')
  } catch {
    throw new Error(
      `Invalid ${name}: expected absolute HTTP(S) URL without credentials, query string, or fragment`
    )
  }
}

function normalizeOptionalSecret(
  value: string | undefined,
  minLength = 1,
  name = 'secret'
): string | undefined {
  const trimmed = value?.trim()
  if (!trimmed) return undefined
  if (trimmed.length < minLength) {
    throw new Error(
      `Invalid ${name}: expected at least ${minLength} non-whitespace characters`
    )
  }
  return trimmed
}

function parseBitcoinAddressNetworks(
  value: string | undefined
): BitcoinAddressNetwork[] {
  const trimmed = value?.trim()
  if (!trimmed) return [...DEFAULT_BITCOIN_ADDRESS_NETWORKS]

  const networks: BitcoinAddressNetwork[] = []
  for (const rawNetwork of trimmed.split(',')) {
    const network = rawNetwork.trim().toLowerCase()
    if (!network) {
      throw new Error(
        'Invalid ROUTER_GATEWAY_BITCOIN_ADDRESS_NETWORKS: empty network'
      )
    }
    if (!BITCOIN_ADDRESS_NETWORK_VALUES.has(network as BitcoinAddressNetwork)) {
      throw new Error(
        'Invalid ROUTER_GATEWAY_BITCOIN_ADDRESS_NETWORKS: expected comma-separated mainnet, testnet, or regtest'
      )
    }
    if (!networks.includes(network as BitcoinAddressNetwork)) {
      networks.push(network as BitcoinAddressNetwork)
    }
  }

  return networks
}

function defaultHealthTargets(input: {
  routerInternalBaseUrl?: string
  routerInternalApiKey?: string
  routerQueryApiBaseUrl?: string
  timeoutMs: number
}): HealthTargetConfig[] {
  return [
    ...(input.routerInternalBaseUrl
      ? [
          {
            name: 'router-api',
            url: `${input.routerInternalBaseUrl}/api/v1/provider-health`,
            method: 'GET' as const,
            timeoutMs: input.timeoutMs,
            response: 'routerProviderHealth' as const,
            ...(input.routerInternalApiKey
              ? {
                  headers: {
                    authorization: `Bearer ${input.routerInternalApiKey}`
                  }
                }
              : {})
          }
        ]
      : []),
    ...(input.routerQueryApiBaseUrl
      ? [
          {
            name: 'router-query-api',
            url: `${input.routerQueryApiBaseUrl}/status`,
            method: 'GET' as const,
            timeoutMs: input.timeoutMs,
            response: 'generic' as const
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
    timeoutMs,
    response: 'generic'
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
    const url = new URL(value)
    if (url.protocol !== 'http:' && url.protocol !== 'https:') {
      throw new Error('expected http or https URL')
    }
    if (url.username || url.password) {
      throw new Error('URL credentials are not allowed')
    }
    if (url.search || url.hash) {
      throw new Error('query strings and fragments are not allowed')
    }
    return url.toString()
  } catch {
    throw new Error(
      `Invalid ${name}: expected absolute HTTP(S) URL without credentials, query string, or fragment`
    )
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

function isLoopbackHost(value: string): boolean {
  const host = value.trim().toLowerCase()
  return (
    host === 'localhost' ||
    host === '127.0.0.1' ||
    host === '::1' ||
    host === '[::1]'
  )
}
