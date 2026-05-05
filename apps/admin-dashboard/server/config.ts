export const ALLOWED_ADMIN_EMAILS = [
  'cliff@rift.trade',
  'samee@rift.trade',
  'tristan@rift.trade'
] as const

export type AdminDashboardConfig = {
  production: boolean
  hostedEnvironment: boolean
  host: string
  port: number
  webOrigin: string
  authBaseUrl: string
  trustedOrigins: string[]
  authDatabaseUrl?: string
  replicaDatabaseUrl?: string
  analyticsDatabaseUrl?: string
  routerAdminApiKey?: string
  googleClientId?: string
  googleClientSecret?: string
  betterAuthSecret: string
  secureCookies: boolean
  orderLimit: number
  cdcSlotName: string
  cdcPublicationName: string
  cdcMessagePrefix: string
  allowInsecureDevAuthBypass: boolean
  serveStatic: boolean
  version: string
  missingAuthConfig: string[]
}

type Env = Record<string, string | undefined>

const DEFAULT_API_PORT = 3000
const DEFAULT_DEV_API_HOST = '127.0.0.1'
const DEFAULT_PRODUCTION_API_HOST = '0.0.0.0'
const DEFAULT_WEB_ORIGIN = 'http://localhost:5173'
const DEFAULT_ORDER_LIMIT = 100
const DEFAULT_CDC_SLOT_NAME = 'admin_dashboard_orders_cdc'
const DEFAULT_CDC_PUBLICATION_NAME = 'router_cdc_publication'
const DEFAULT_CDC_MESSAGE_PREFIX = 'rift.router.change'
const DEV_AUTH_SECRET = 'rift-admin-dashboard-development-secret-change-me'
const MIN_PRODUCTION_AUTH_SECRET_LENGTH = 32
const MIN_ROUTER_ADMIN_API_KEY_LENGTH = 32
const MAX_POSTGRES_IDENTIFIER_BYTES = 63
const POSTGRES_IDENTIFIER_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/
const POSITIVE_DECIMAL_INTEGER_PATTERN = /^[1-9][0-9]{0,14}$/

export function loadConfig(env: Env = Bun.env as Env): AdminDashboardConfig {
  const port = parsePort(
    env.ADMIN_DASHBOARD_API_PORT ?? env.PORT,
    DEFAULT_API_PORT,
    'ADMIN_DASHBOARD_API_PORT'
  )
  const authBaseUrl = normalizeUrl(
    env.BETTER_AUTH_URL ??
      env.ADMIN_DASHBOARD_AUTH_BASE_URL ??
      `http://localhost:${port}`,
    'BETTER_AUTH_URL'
  )
  const hostedEnvironment = isHostedEnvironment(env)
  const production = parseBooleanFlag(
    env.ADMIN_DASHBOARD_PRODUCTION,
    defaultProductionMode(env, authBaseUrl, hostedEnvironment),
    'ADMIN_DASHBOARD_PRODUCTION'
  )
  const host = normalizeHost(
    env.HOST ?? (production ? DEFAULT_PRODUCTION_API_HOST : DEFAULT_DEV_API_HOST),
    'HOST'
  )
  const webOrigin = normalizeOrigin(
    env.ADMIN_DASHBOARD_WEB_ORIGIN ??
      (production ? originFromUrl(authBaseUrl) : DEFAULT_WEB_ORIGIN),
    'ADMIN_DASHBOARD_WEB_ORIGIN'
  )
  const trustedOrigins = unique([
    originFromUrl(authBaseUrl),
    webOrigin,
    ...parseOrigins(env.ADMIN_DASHBOARD_TRUSTED_ORIGINS)
  ])
  const rawBetterAuthSecret = normalizeOptionalSecret(
    env.BETTER_AUTH_SECRET ?? env.AUTH_SECRET
  )
  const betterAuthSecret = production
    ? productionAuthSecret(rawBetterAuthSecret)
    : (rawBetterAuthSecret ?? DEV_AUTH_SECRET)

  const authDatabaseUrl = normalizeOptionalSecret(
    env.ADMIN_DASHBOARD_AUTH_DATABASE_URL
  )
  const googleClientId = normalizeOptionalSecret(env.GOOGLE_CLIENT_ID)
  const googleClientSecret = normalizeOptionalSecret(env.GOOGLE_CLIENT_SECRET)

  const missingAuthConfig = production
    ? [
        authDatabaseUrl ? undefined : 'ADMIN_DASHBOARD_AUTH_DATABASE_URL',
        googleClientId ? undefined : 'GOOGLE_CLIENT_ID',
        googleClientSecret ? undefined : 'GOOGLE_CLIENT_SECRET',
        betterAuthSecret ? undefined : 'BETTER_AUTH_SECRET'
      ].filter((value): value is string => Boolean(value))
    : []

  return {
    production,
    hostedEnvironment,
    host,
    port,
    webOrigin,
    authBaseUrl,
    trustedOrigins,
    authDatabaseUrl,
    replicaDatabaseUrl: normalizeOptionalSecret(
      env.ADMIN_DASHBOARD_REPLICA_DATABASE_URL ?? env.ROUTER_REPLICA_DATABASE_URL
    ),
    analyticsDatabaseUrl: normalizeOptionalSecret(
      env.ADMIN_DASHBOARD_ANALYTICS_DATABASE_URL
    ),
    routerAdminApiKey: normalizeOptionalSecret(
      env.ROUTER_ADMIN_API_KEY,
      MIN_ROUTER_ADMIN_API_KEY_LENGTH
    ),
    googleClientId,
    googleClientSecret,
    betterAuthSecret,
    secureCookies: authBaseUrl.startsWith('https://'),
    orderLimit: parsePositiveInteger(
      env.ADMIN_DASHBOARD_ORDER_LIMIT,
      DEFAULT_ORDER_LIMIT,
      'ADMIN_DASHBOARD_ORDER_LIMIT'
    ),
    cdcSlotName:
      normalizePostgresIdentifier(
        normalizeOptionalSecret(env.ADMIN_DASHBOARD_CDC_SLOT_NAME) ??
          DEFAULT_CDC_SLOT_NAME,
        'ADMIN_DASHBOARD_CDC_SLOT_NAME'
      ),
    cdcPublicationName: normalizeCdcPublicationName(
      env.ROUTER_CDC_PUBLICATION_NAME
    ),
    cdcMessagePrefix: normalizeCdcMessagePrefix(env.ROUTER_CDC_MESSAGE_PREFIX),
    allowInsecureDevAuthBypass: parseBooleanFlag(
      env.ADMIN_DASHBOARD_ALLOW_INSECURE_DEV_AUTH_BYPASS,
      false,
      'ADMIN_DASHBOARD_ALLOW_INSECURE_DEV_AUTH_BYPASS'
    ),
    serveStatic: env.ADMIN_DASHBOARD_SERVE_STATIC !== 'false',
    version: env.npm_package_version ?? '0.1.0',
    missingAuthConfig
  }
}

export function validateRuntimeConfig(config: AdminDashboardConfig): void {
  if (!config.production) {
    if (config.hostedEnvironment) {
      throw new Error(
        'Admin dashboard development auth bypass is not allowed in hosted environments'
      )
    }
    if (!config.allowInsecureDevAuthBypass && !isLoopbackHost(config.host)) {
      throw new Error(
        'ADMIN_DASHBOARD_ALLOW_INSECURE_DEV_AUTH_BYPASS=true is required when development auth bypass binds non-loopback HOST'
      )
    }
    return
  }

  if (config.missingAuthConfig.length > 0) {
    throw new Error(
      `Admin dashboard production auth config is missing: ${config.missingAuthConfig.join(', ')}`
    )
  }
  if (!config.authBaseUrl.startsWith('https://')) {
    throw new Error('BETTER_AUTH_URL must use https in production')
  }
  if (!config.webOrigin.startsWith('https://')) {
    throw new Error('ADMIN_DASHBOARD_WEB_ORIGIN must use https in production')
  }
  for (const origin of config.trustedOrigins) {
    if (!origin.startsWith('https://')) {
      throw new Error(
        `ADMIN_DASHBOARD_TRUSTED_ORIGINS must use https in production: ${origin}`
      )
    }
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
  if (!POSITIVE_DECIMAL_INTEGER_PATTERN.test(value)) {
    throw new Error(`Invalid ${name}: ${value}`)
  }
  const port = Number(value)
  if (!Number.isSafeInteger(port) || port > 65535) {
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
  if (!POSITIVE_DECIMAL_INTEGER_PATTERN.test(value)) {
    throw new Error(`Invalid ${name}: ${value}`)
  }
  const parsed = Number(value)
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(`Invalid ${name}: ${value}`)
  }
  return parsed
}

function parseBooleanFlag(
  value: string | undefined,
  defaultValue: boolean,
  name: string
): boolean {
  if (value === undefined) return defaultValue
  const normalized = value.trim().toLowerCase()
  if (['1', 'true', 'yes', 'on'].includes(normalized)) return true
  if (['0', 'false', 'no', 'off'].includes(normalized)) return false
  throw new Error(`Invalid ${name}: ${value}`)
}

function defaultProductionMode(
  env: Env,
  authBaseUrl: string,
  hostedEnvironment: boolean
): boolean {
  if (env.NODE_ENV === 'production') return true
  if (hostedEnvironment) return true
  if (!isLocalUrl(authBaseUrl)) return true

  const explicitWebOrigin = normalizeOptionalSecret(env.ADMIN_DASHBOARD_WEB_ORIGIN)
  return explicitWebOrigin ? !isLocalUrl(explicitWebOrigin) : false
}

function isHostedEnvironment(env: Env): boolean {
  return Boolean(
    env.RAILWAY_ENVIRONMENT ||
      env.RAILWAY_PROJECT_ID ||
      env.RAILWAY_SERVICE_ID ||
      env.RENDER ||
      env.FLY_APP_NAME ||
      env.VERCEL ||
      env.NETLIFY
  )
}

function productionAuthSecret(value: string | undefined): string {
  if (!value || value.length < MIN_PRODUCTION_AUTH_SECRET_LENGTH) return ''
  if (value === DEV_AUTH_SECRET) return ''
  return value
}

function normalizeOptionalSecret(
  value: string | undefined,
  minLength = 1
): string | undefined {
  const trimmed = value?.trim()
  return trimmed && trimmed.length >= minLength ? trimmed : undefined
}

function normalizePostgresIdentifier(value: string, name: string): string {
  const trimmed = value.trim()
  if (!trimmed) throw new Error(`${name} must not be empty`)
  if (new TextEncoder().encode(trimmed).length > MAX_POSTGRES_IDENTIFIER_BYTES) {
    throw new Error(`${name} must be at most ${MAX_POSTGRES_IDENTIFIER_BYTES} bytes`)
  }
  if (!POSTGRES_IDENTIFIER_PATTERN.test(trimmed)) {
    throw new Error(`${name} must be an unquoted PostgreSQL identifier`)
  }
  return trimmed
}

function normalizeCdcPublicationName(value: string | undefined): string {
  const normalized = normalizePostgresIdentifier(
    normalizeOptionalSecret(value) ?? DEFAULT_CDC_PUBLICATION_NAME,
    'ROUTER_CDC_PUBLICATION_NAME'
  )
  if (normalized !== DEFAULT_CDC_PUBLICATION_NAME) {
    throw new Error(
      `ROUTER_CDC_PUBLICATION_NAME must be ${DEFAULT_CDC_PUBLICATION_NAME} because router migrations create that fixed publication`
    )
  }
  return normalized
}

function normalizeCdcMessagePrefix(value: string | undefined): string {
  const normalized = normalizeOptionalSecret(value) ?? DEFAULT_CDC_MESSAGE_PREFIX
  if (normalized !== DEFAULT_CDC_MESSAGE_PREFIX) {
    throw new Error(
      `ROUTER_CDC_MESSAGE_PREFIX must be ${DEFAULT_CDC_MESSAGE_PREFIX} because router migrations emit that fixed logical-message prefix`
    )
  }
  return normalized
}

function normalizeHost(value: string, name: string): string {
  const trimmed = value.trim()
  if (!trimmed) throw new Error(`${name} must not be empty`)
  return trimmed
}

function normalizeUrl(value: string, name: string): string {
  const trimmed = value.trim().replace(/\/+$/, '')
  try {
    const parsed = new URL(trimmed)
    if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
      throw new Error('expected http or https')
    }
    if (parsed.username || parsed.password) {
      throw new Error('URL credentials are not allowed')
    }
    if (parsed.search || parsed.hash) {
      throw new Error('query strings and fragments are not allowed')
    }
    if (parsed.pathname !== '/') {
      throw new Error('paths are not allowed')
    }
    return parsed.origin
  } catch (error) {
    const reason = error instanceof Error ? `: ${error.message}` : ''
    throw new Error(`Invalid ${name}${reason}: ${sanitizeUrlForError(value)}`)
  }
}

function normalizeOrigin(value: string, name: string): string {
  try {
    const parsed = new URL(value.trim())
    if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
      throw new Error('expected http or https')
    }
    if (parsed.username || parsed.password) {
      throw new Error('URL credentials are not allowed')
    }
    if (parsed.search || parsed.hash) {
      throw new Error('query strings and fragments are not allowed')
    }
    if (parsed.pathname !== '/') {
      throw new Error('paths are not allowed')
    }
    return parsed.origin
  } catch (error) {
    const reason = error instanceof Error ? `: ${error.message}` : ''
    throw new Error(`Invalid ${name}${reason}: ${sanitizeUrlForError(value)}`)
  }
}

function originFromUrl(value: string): string {
  return new URL(value).origin
}

function isLocalUrl(value: string): boolean {
  try {
    const parsed = new URL(value)
    return (
      parsed.hostname === 'localhost' ||
      parsed.hostname === '127.0.0.1' ||
      parsed.hostname === '::1' ||
      parsed.hostname === '[::1]' ||
      parsed.hostname.endsWith('.localhost')
    )
  } catch (_error) {
    return false
  }
}

function isLoopbackHost(value: string): boolean {
  const host = value.trim().toLowerCase()
  return (
    host === 'localhost' ||
    host === '127.0.0.1' ||
    host === '::1' ||
    host === '[::1]' ||
    host.endsWith('.localhost')
  )
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

function sanitizeUrlForError(value: string): string {
  const trimmed = value.trim()
  try {
    const parsed = new URL(trimmed)
    if (parsed.username) parsed.username = '<redacted>'
    if (parsed.password) parsed.password = '<redacted>'
    if (parsed.search) parsed.search = '?<redacted>'
    if (parsed.hash) parsed.hash = '#<redacted>'
    return parsed.toString()
  } catch (_error) {
    return sanitizeUnparsedUrlForError(trimmed)
  }
}

function sanitizeUnparsedUrlForError(value: string): string {
  let sanitized = value
  if (sanitized.includes('://')) {
    const credentialsStart = sanitized.indexOf('://') + 3
    const atIndex = sanitized.indexOf('@', credentialsStart)
    if (atIndex > credentialsStart) {
      sanitized = `${sanitized.slice(0, credentialsStart)}<redacted>${sanitized.slice(atIndex)}`
    }
  }
  const queryIndex = sanitized.indexOf('?')
  if (queryIndex >= 0) sanitized = `${sanitized.slice(0, queryIndex)}?<redacted>`
  const fragmentIndex = sanitized.indexOf('#')
  if (fragmentIndex >= 0) sanitized = `${sanitized.slice(0, fragmentIndex)}#<redacted>`
  const maxLength = 256
  return sanitized.length > maxLength ? `${sanitized.slice(0, maxLength)}...` : sanitized
}
