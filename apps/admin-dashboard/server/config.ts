export const ALLOWED_ADMIN_EMAILS = [
  'cliff@rift.trade',
  'samee@rift.trade',
  'tristan@rift.trade'
] as const

export type AdminDashboardConfig = {
  host: string
  port: number
  webOrigin: string
  authBaseUrl: string
  trustedOrigins: string[]
  authDatabaseUrl?: string
  replicaDatabaseUrl?: string
  routerAdminApiKey?: string
  googleClientId?: string
  googleClientSecret?: string
  betterAuthSecret: string
  secureCookies: boolean
  orderLimit: number
  orderPollIntervalMs: number
  serveStatic: boolean
  version: string
  missingAuthConfig: string[]
}

type Env = Record<string, string | undefined>

const DEFAULT_API_PORT = 3000
const DEFAULT_WEB_ORIGIN = 'http://localhost:5173'
const DEFAULT_ORDER_LIMIT = 200
const DEFAULT_ORDER_POLL_INTERVAL_MS = 2_000
const DEV_AUTH_SECRET = 'rift-admin-dashboard-development-secret-change-me'

export function loadConfig(env: Env = Bun.env as Env): AdminDashboardConfig {
  const port = parsePort(
    env.ADMIN_DASHBOARD_API_PORT ?? env.PORT,
    DEFAULT_API_PORT,
    'ADMIN_DASHBOARD_API_PORT'
  )
  const isProduction = env.NODE_ENV === 'production'
  const authBaseUrl = normalizeUrl(
    env.BETTER_AUTH_URL ??
      env.ADMIN_DASHBOARD_AUTH_BASE_URL ??
      `http://localhost:${port}`,
    'BETTER_AUTH_URL'
  )
  const webOrigin = normalizeOrigin(
    env.ADMIN_DASHBOARD_WEB_ORIGIN ??
      (isProduction ? originFromUrl(authBaseUrl) : DEFAULT_WEB_ORIGIN),
    'ADMIN_DASHBOARD_WEB_ORIGIN'
  )
  const trustedOrigins = unique([
    originFromUrl(authBaseUrl),
    webOrigin,
    ...parseOrigins(env.ADMIN_DASHBOARD_TRUSTED_ORIGINS)
  ])
  const betterAuthSecret =
    normalizeOptionalSecret(env.BETTER_AUTH_SECRET ?? env.AUTH_SECRET) ??
    (isProduction ? '' : DEV_AUTH_SECRET)

  const authDatabaseUrl = normalizeOptionalSecret(
    env.ADMIN_DASHBOARD_AUTH_DATABASE_URL
  )
  const googleClientId = normalizeOptionalSecret(env.GOOGLE_CLIENT_ID)
  const googleClientSecret = normalizeOptionalSecret(env.GOOGLE_CLIENT_SECRET)

  const missingAuthConfig = [
    authDatabaseUrl ? undefined : 'ADMIN_DASHBOARD_AUTH_DATABASE_URL',
    googleClientId ? undefined : 'GOOGLE_CLIENT_ID',
    googleClientSecret ? undefined : 'GOOGLE_CLIENT_SECRET',
    betterAuthSecret ? undefined : 'BETTER_AUTH_SECRET'
  ].filter((value): value is string => Boolean(value))

  return {
    host: env.HOST ?? '0.0.0.0',
    port,
    webOrigin,
    authBaseUrl,
    trustedOrigins,
    authDatabaseUrl,
    replicaDatabaseUrl: normalizeOptionalSecret(
      env.ADMIN_DASHBOARD_REPLICA_DATABASE_URL ?? env.ROUTER_REPLICA_DATABASE_URL
    ),
    routerAdminApiKey: normalizeOptionalSecret(env.ROUTER_ADMIN_API_KEY),
    googleClientId,
    googleClientSecret,
    betterAuthSecret,
    secureCookies: authBaseUrl.startsWith('https://'),
    orderLimit: parsePositiveInteger(
      env.ADMIN_DASHBOARD_ORDER_LIMIT,
      DEFAULT_ORDER_LIMIT,
      'ADMIN_DASHBOARD_ORDER_LIMIT'
    ),
    orderPollIntervalMs: parsePositiveInteger(
      env.ADMIN_DASHBOARD_ORDER_POLL_INTERVAL_MS,
      DEFAULT_ORDER_POLL_INTERVAL_MS,
      'ADMIN_DASHBOARD_ORDER_POLL_INTERVAL_MS'
    ),
    serveStatic: env.ADMIN_DASHBOARD_SERVE_STATIC !== 'false',
    version: env.npm_package_version ?? '0.1.0',
    missingAuthConfig
  }
}

export function isAllowedAdminEmail(email: string | null | undefined): boolean {
  if (!email) return false
  const normalized = email.trim().toLowerCase()
  return ALLOWED_ADMIN_EMAILS.some((allowed) => allowed === normalized)
}

export function normalizeAdminEmail(email: string | null | undefined) {
  return email?.trim().toLowerCase() ?? null
}

function parsePort(
  value: string | undefined,
  defaultValue: number,
  name: string
): number {
  if (!value) return defaultValue
  const port = Number(value)
  if (!Number.isInteger(port) || port <= 0 || port > 65535) {
    throw new Error(`Invalid ${name}: ${value}`)
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

function normalizeOptionalSecret(value: string | undefined): string | undefined {
  const trimmed = value?.trim()
  return trimmed ? trimmed : undefined
}

function normalizeUrl(value: string, name: string): string {
  const trimmed = value.trim().replace(/\/+$/, '')
  try {
    const parsed = new URL(trimmed)
    if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
      throw new Error('expected http or https')
    }
    return trimmed
  } catch (error) {
    throw new Error(`Invalid ${name}: ${value}`)
  }
}

function normalizeOrigin(value: string, name: string): string {
  try {
    return new URL(value.trim()).origin
  } catch (_error) {
    throw new Error(`Invalid ${name}: ${value}`)
  }
}

function originFromUrl(value: string): string {
  return new URL(value).origin
}

function parseOrigins(value: string | undefined): string[] {
  if (!value) return []
  return value
    .split(',')
    .map((origin) => origin.trim())
    .filter(Boolean)
    .map((origin) => normalizeOrigin(origin, 'ADMIN_DASHBOARD_TRUSTED_ORIGINS'))
}

function unique(values: string[]): string[] {
  return [...new Set(values)]
}
