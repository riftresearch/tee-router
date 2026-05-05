import { expect, test } from 'bun:test'

import { loadConfig, validateRuntimeConfig } from './config'

test('loadConfig keeps localhost development bypass by default', () => {
  const config = loadConfig({})

  expect(config.production).toBe(false)
  expect(config.host).toBe('127.0.0.1')
  expect(config.authBaseUrl).toBe('http://localhost:3000')
  expect(config.webOrigin).toBe('http://localhost:5173')
  expect(config.betterAuthSecret).toBe(
    'rift-admin-dashboard-development-secret-change-me'
  )
  expect(config.missingAuthConfig).toEqual([])
  expect(() => validateRuntimeConfig(config)).not.toThrow()
})

test('loadConfig treats IPv6 localhost as local development', () => {
  const config = loadConfig({
    BETTER_AUTH_URL: 'http://[::1]:3000'
  })

  expect(config.production).toBe(false)
  expect(config.authBaseUrl).toBe('http://[::1]:3000')
})

test('loadConfig defaults public auth URLs to production', () => {
  const config = loadConfig({
    BETTER_AUTH_URL: 'https://admin.rift.trade'
  })

  expect(config.production).toBe(true)
  expect(config.host).toBe('0.0.0.0')
  expect(config.webOrigin).toBe('https://admin.rift.trade')
  expect(config.authBaseUrl).toBe('https://admin.rift.trade')
  expect(config.missingAuthConfig).toEqual([
    'ADMIN_DASHBOARD_AUTH_DATABASE_URL',
    'GOOGLE_CLIENT_ID',
    'GOOGLE_CLIENT_SECRET',
    'BETTER_AUTH_SECRET'
  ])
})

test('loadConfig defaults hosted railway environments to production', () => {
  const config = loadConfig({
    RAILWAY_ENVIRONMENT: 'production'
  })

  expect(config.hostedEnvironment).toBe(true)
  expect(config.production).toBe(true)
  expect(config.missingAuthConfig).toContain('BETTER_AUTH_SECRET')
})

test('loadConfig rejects non-decimal numeric env values', () => {
  expect(() =>
    loadConfig({
      ADMIN_DASHBOARD_API_PORT: '3e3'
    })
  ).toThrow('Invalid ADMIN_DASHBOARD_API_PORT')

  expect(() =>
    loadConfig({
      ADMIN_DASHBOARD_ORDER_LIMIT: '1e2'
    })
  ).toThrow('Invalid ADMIN_DASHBOARD_ORDER_LIMIT')
})

test('loadConfig rejects and redacts credentialed auth and origin URLs', () => {
  expect(() =>
    loadConfig({
      BETTER_AUTH_URL: 'https://user:pass@admin.rift.trade'
    })
  ).toThrow('URL credentials are not allowed')

  try {
    loadConfig({
      BETTER_AUTH_URL: 'https://user:pass@admin.rift.trade'
    })
    throw new Error('expected loadConfig to reject credentialed URL')
  } catch (error) {
    const rendered = String(error)
    expect(rendered).not.toContain('user')
    expect(rendered).not.toContain('pass')
  }

  try {
    loadConfig({
      ADMIN_DASHBOARD_TRUSTED_ORIGINS: 'https://ops.rift.trade?token=secret'
    })
    throw new Error('expected loadConfig to reject query URL')
  } catch (error) {
    const rendered = String(error)
    expect(rendered).toContain('query strings and fragments are not allowed')
    expect(rendered).not.toContain('token')
    expect(rendered).not.toContain('secret')
  }
})

test('loadConfig rejects path-scoped auth and origin URLs', () => {
  expect(() =>
    loadConfig({
      BETTER_AUTH_URL: 'https://admin.rift.trade/api/auth'
    })
  ).toThrow('paths are not allowed')

  expect(() =>
    loadConfig({
      ADMIN_DASHBOARD_TRUSTED_ORIGINS: 'https://ops.rift.trade/admin'
    })
  ).toThrow('paths are not allowed')
})

test('loadConfig allows explicit local auth bypass', () => {
  const config = loadConfig({
    ADMIN_DASHBOARD_PRODUCTION: 'false',
    BETTER_AUTH_URL: 'https://admin.rift.trade'
  })

  expect(config.production).toBe(false)
  expect(config.authBaseUrl).toBe('https://admin.rift.trade')
  expect(config.webOrigin).toBe('http://localhost:5173')
  expect(config.missingAuthConfig).toEqual([])
})

test('validateRuntimeConfig rejects publicly bound development auth bypass unless explicit', () => {
  const exposed = loadConfig({
    ADMIN_DASHBOARD_PRODUCTION: 'false',
    HOST: '0.0.0.0'
  })

  expect(() => validateRuntimeConfig(exposed)).toThrow(
    'ADMIN_DASHBOARD_ALLOW_INSECURE_DEV_AUTH_BYPASS=true is required'
  )

  const explicit = loadConfig({
    ADMIN_DASHBOARD_PRODUCTION: 'false',
    HOST: '0.0.0.0',
    ADMIN_DASHBOARD_ALLOW_INSECURE_DEV_AUTH_BYPASS: 'true'
  })

  expect(() => validateRuntimeConfig(explicit)).not.toThrow()
})

test('validateRuntimeConfig rejects development auth bypass in hosted environments', () => {
  const config = loadConfig({
    RAILWAY_ENVIRONMENT: 'production',
    ADMIN_DASHBOARD_PRODUCTION: 'false',
    ADMIN_DASHBOARD_ALLOW_INSECURE_DEV_AUTH_BYPASS: 'true',
    HOST: '0.0.0.0'
  })

  expect(config.hostedEnvironment).toBe(true)
  expect(config.production).toBe(false)
  expect(() => validateRuntimeConfig(config)).toThrow(
    'Admin dashboard development auth bypass is not allowed in hosted environments'
  )
})

test('loadConfig rejects weak production auth secrets as missing config', () => {
  const config = loadConfig({
    NODE_ENV: 'production',
    ADMIN_DASHBOARD_AUTH_DATABASE_URL: 'postgres://postgres:postgres@localhost/auth',
    GOOGLE_CLIENT_ID: 'google-client-id',
    GOOGLE_CLIENT_SECRET: 'google-client-secret',
    BETTER_AUTH_SECRET: 'too-short'
  })

  expect(config.production).toBe(true)
  expect(config.betterAuthSecret).toBe('')
  expect(config.missingAuthConfig).toEqual(['BETTER_AUTH_SECRET'])
})

test('loadConfig rejects the development auth secret in production', () => {
  const config = loadConfig({
    NODE_ENV: 'production',
    ADMIN_DASHBOARD_AUTH_DATABASE_URL: 'postgres://postgres:postgres@localhost/auth',
    GOOGLE_CLIENT_ID: 'google-client-id',
    GOOGLE_CLIENT_SECRET: 'google-client-secret',
    BETTER_AUTH_SECRET: 'rift-admin-dashboard-development-secret-change-me'
  })

  expect(config.production).toBe(true)
  expect(config.betterAuthSecret).toBe('')
  expect(config.missingAuthConfig).toEqual(['BETTER_AUTH_SECRET'])
})

test('loadConfig treats weak router admin API keys as unconfigured', () => {
  const weak = loadConfig({
    ROUTER_ADMIN_API_KEY: 'short-router-key'
  })
  const strong = loadConfig({
    ROUTER_ADMIN_API_KEY: 'router-admin-key-0000000000000000'
  })

  expect(weak.routerAdminApiKey).toBeUndefined()
  expect(strong.routerAdminApiKey).toBe('router-admin-key-0000000000000000')
})

test('loadConfig validates CDC postgres identifiers', () => {
  const config = loadConfig({
    ADMIN_DASHBOARD_CDC_SLOT_NAME: 'admin_dashboard_orders_cdc_test',
    ROUTER_CDC_PUBLICATION_NAME: ' router_cdc_publication '
  })

  expect(config.cdcSlotName).toBe('admin_dashboard_orders_cdc_test')
  expect(config.cdcPublicationName).toBe('router_cdc_publication')

  expect(() =>
    loadConfig({
      ADMIN_DASHBOARD_CDC_SLOT_NAME: '1bad'
    })
  ).toThrow('ADMIN_DASHBOARD_CDC_SLOT_NAME must be an unquoted PostgreSQL identifier')

  expect(() =>
    loadConfig({
      ROUTER_CDC_PUBLICATION_NAME: 'bad-name'
    })
  ).toThrow('ROUTER_CDC_PUBLICATION_NAME must be an unquoted PostgreSQL identifier')

  expect(() =>
    loadConfig({
      ADMIN_DASHBOARD_CDC_SLOT_NAME: 'a'.repeat(64)
    })
  ).toThrow('ADMIN_DASHBOARD_CDC_SLOT_NAME must be at most 63 bytes')
})

test('loadConfig rejects non-default CDC publication names', () => {
  expect(() =>
    loadConfig({
      ROUTER_CDC_PUBLICATION_NAME: 'router_cdc_publication_test'
    })
  ).toThrow('ROUTER_CDC_PUBLICATION_NAME must be router_cdc_publication')
})

test('loadConfig rejects non-default CDC logical-message prefixes', () => {
  expect(
    loadConfig({
      ROUTER_CDC_MESSAGE_PREFIX: ' rift.router.change '
    }).cdcMessagePrefix
  ).toBe('rift.router.change')

  expect(() =>
    loadConfig({
      ROUTER_CDC_MESSAGE_PREFIX: 'custom.router.change'
    })
  ).toThrow('ROUTER_CDC_MESSAGE_PREFIX must be rift.router.change')
})

test('validateRuntimeConfig fails fast on incomplete production auth config', () => {
  const config = loadConfig({
    BETTER_AUTH_URL: 'https://admin.rift.trade'
  })

  expect(() => validateRuntimeConfig(config)).toThrow(
    'Admin dashboard production auth config is missing'
  )
})

test('validateRuntimeConfig requires HTTPS production origins', () => {
  const config = loadConfig({
    ADMIN_DASHBOARD_PRODUCTION: 'true',
    BETTER_AUTH_URL: 'http://admin.rift.trade',
    ADMIN_DASHBOARD_WEB_ORIGIN: 'http://admin.rift.trade',
    ADMIN_DASHBOARD_AUTH_DATABASE_URL: 'postgres://postgres:postgres@localhost/auth',
    GOOGLE_CLIENT_ID: 'google-client-id',
    GOOGLE_CLIENT_SECRET: 'google-client-secret',
    BETTER_AUTH_SECRET: 'production-secret-000000000000000000'
  })

  expect(() => validateRuntimeConfig(config)).toThrow(
    'BETTER_AUTH_URL must use https in production'
  )
})

test('validateRuntimeConfig accepts complete HTTPS production config', () => {
  const config = loadConfig({
    ADMIN_DASHBOARD_PRODUCTION: 'true',
    BETTER_AUTH_URL: 'https://admin.rift.trade',
    ADMIN_DASHBOARD_WEB_ORIGIN: 'https://admin.rift.trade',
    ADMIN_DASHBOARD_AUTH_DATABASE_URL: 'postgres://postgres:postgres@localhost/auth',
    GOOGLE_CLIENT_ID: 'google-client-id',
    GOOGLE_CLIENT_SECRET: 'google-client-secret',
    BETTER_AUTH_SECRET: 'production-secret-000000000000000000',
    ADMIN_DASHBOARD_TRUSTED_ORIGINS: 'https://ops.rift.trade'
  })

  expect(() => validateRuntimeConfig(config)).not.toThrow()
})
