export type GatewayConfig = {
  host: string
  port: number
  routerInternalBaseUrl?: string
  version: string
}

type Env = Record<string, string | undefined>

const DEFAULT_PORT = 3000
const DEFAULT_VERSION = '0.1.0'

export function loadConfig(env: Env = Bun.env as Env): GatewayConfig {
  return {
    host: env.HOST ?? '0.0.0.0',
    port: parsePort(env.PORT),
    routerInternalBaseUrl: normalizeOptionalUrl(env.ROUTER_INTERNAL_BASE_URL),
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

function normalizeOptionalUrl(value: string | undefined): string | undefined {
  const trimmed = value?.trim()
  if (!trimmed) return undefined

  return trimmed.replace(/\/+$/, '')
}
