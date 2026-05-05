import { expect, test } from 'bun:test'

import { loadConfig, validateGatewayRuntimeConfig } from '../config'

const STRONG_CANCELLATION_KEY =
  '1111111111111111111111111111111111111111111111111111111111111111'
const STRONG_ROUTER_GATEWAY_API_KEY = 'gateway-secret-00000000000000000'

test('loadConfig normalizes HTTP service URLs', () => {
  const config = loadConfig({
    ROUTER_INTERNAL_BASE_URL: ' http://router.internal/ ',
    ROUTER_GATEWAY_API_KEY: ` ${STRONG_ROUTER_GATEWAY_API_KEY} `,
    ROUTER_QUERY_API_BASE_URL: 'https://query.internal/status-root/',
    ROUTER_GATEWAY_PUBLIC_BASE_URL: 'https://gateway.example.com/'
  })

  expect(config.routerInternalBaseUrl).toBe('http://router.internal')
  expect(config.routerInternalApiKey).toBe(STRONG_ROUTER_GATEWAY_API_KEY)
  expect(config.routerQueryApiBaseUrl).toBe('https://query.internal/status-root')
  expect(config.publicBaseUrl).toBe('https://gateway.example.com')
  expect(config.healthTargets[0]).toMatchObject({
    name: 'router-api',
    headers: {
      authorization: `Bearer ${STRONG_ROUTER_GATEWAY_API_KEY}`
    }
  })
})

test('loadConfig rejects invalid upstream URLs', () => {
  expect(() =>
    loadConfig({
      ROUTER_INTERNAL_BASE_URL: 'router.internal'
    })
  ).toThrow('Invalid ROUTER_INTERNAL_BASE_URL')
  expect(() =>
    loadConfig({
      ROUTER_GATEWAY_PUBLIC_BASE_URL: 'postgres://gateway'
    })
  ).toThrow('Invalid ROUTER_GATEWAY_PUBLIC_BASE_URL')
  expect(() =>
    loadConfig({
      ROUTER_INTERNAL_BASE_URL: 'https://user:pass@router.internal'
    })
  ).toThrow('Invalid ROUTER_INTERNAL_BASE_URL')
  expect(() =>
    loadConfig({
      ROUTER_GATEWAY_PUBLIC_BASE_URL: 'https://user:pass@gateway.example.com'
    })
  ).toThrow('Invalid ROUTER_GATEWAY_PUBLIC_BASE_URL')
  expect(() =>
    loadConfig({
      ROUTER_INTERNAL_BASE_URL: 'https://router.internal?token=secret'
    })
  ).toThrow('Invalid ROUTER_INTERNAL_BASE_URL')
  expect(() =>
    loadConfig({
      ROUTER_QUERY_API_BASE_URL: 'https://query.internal#status'
    })
  ).toThrow('Invalid ROUTER_QUERY_API_BASE_URL')
})

test('loadConfig rejects weak configured router gateway API keys', () => {
  expect(() =>
    loadConfig({
      ROUTER_GATEWAY_API_KEY: 'short'
    })
  ).toThrow('Invalid ROUTER_GATEWAY_API_KEY')
})

test('loadConfig rejects non-HTTP custom health target URLs', () => {
  expect(() =>
    loadConfig({
      ROUTER_GATEWAY_HEALTH_TARGETS: JSON.stringify([
        {
          name: 'local-file',
          url: 'file:///etc/passwd'
        }
      ])
    })
  ).toThrow('Invalid ROUTER_GATEWAY_HEALTH_TARGETS[0].url')
  expect(() =>
    loadConfig({
      ROUTER_GATEWAY_HEALTH_TARGETS: JSON.stringify([
        {
          name: 'credentialed',
          url: 'https://user:pass@health.internal'
        }
      ])
    })
  ).toThrow('Invalid ROUTER_GATEWAY_HEALTH_TARGETS[0].url')
  expect(() =>
    loadConfig({
      ROUTER_GATEWAY_HEALTH_TARGETS: JSON.stringify([
        {
          name: 'query-secret',
          url: 'https://health.internal/status?token=secret'
        }
      ])
    })
  ).toThrow('Invalid ROUTER_GATEWAY_HEALTH_TARGETS[0].url')
})

test('loadConfig rejects timer values outside the safe setTimeout range', () => {
  expect(() =>
    loadConfig({
      ROUTER_GATEWAY_REQUEST_TIMEOUT_MS: '2147483648'
    })
  ).toThrow('Invalid ROUTER_GATEWAY_REQUEST_TIMEOUT_MS')
  expect(() =>
    loadConfig({
      ROUTER_GATEWAY_HEALTH_TARGET_TIMEOUT_MS: '9007199254740992'
    })
  ).toThrow('Invalid ROUTER_GATEWAY_HEALTH_TARGET_TIMEOUT_MS')
  expect(() =>
    loadConfig({
      ROUTER_GATEWAY_REQUEST_TIMEOUT_MS: '1e3'
    })
  ).toThrow('Invalid ROUTER_GATEWAY_REQUEST_TIMEOUT_MS')
})

test('loadConfig rejects non-decimal port values', () => {
  expect(() =>
    loadConfig({
      PORT: '3e3'
    })
  ).toThrow('Invalid PORT')
})

test('loadConfig defaults Bitcoin address validation to mainnet only', () => {
  expect(loadConfig({}).bitcoinAddressNetworks).toEqual(['mainnet'])
})

test('loadConfig parses explicit Bitcoin address validation networks', () => {
  expect(
    loadConfig({
      ROUTER_GATEWAY_BITCOIN_ADDRESS_NETWORKS: ' regtest, testnet, regtest '
    }).bitcoinAddressNetworks
  ).toEqual(['regtest', 'testnet'])

  expect(() =>
    loadConfig({
      ROUTER_GATEWAY_BITCOIN_ADDRESS_NETWORKS: 'mainnet,,regtest'
    })
  ).toThrow('Invalid ROUTER_GATEWAY_BITCOIN_ADDRESS_NETWORKS')
  expect(() =>
    loadConfig({
      ROUTER_GATEWAY_BITCOIN_ADDRESS_NETWORKS: 'signet'
    })
  ).toThrow('Invalid ROUTER_GATEWAY_BITCOIN_ADDRESS_NETWORKS')
})

test('validateGatewayRuntimeConfig allows loopback development without order dependencies', () => {
  expect(() =>
    validateGatewayRuntimeConfig(
      loadConfig({
        HOST: '127.0.0.1'
      })
    )
  ).not.toThrow()
})

test('validateGatewayRuntimeConfig requires order dependencies on public binds', () => {
  expect(() =>
    validateGatewayRuntimeConfig(
      loadConfig({
        HOST: '0.0.0.0'
      })
    )
  ).toThrow('ROUTER_INTERNAL_BASE_URL must be configured')
  expect(() =>
    validateGatewayRuntimeConfig(
      loadConfig({
        HOST: '0.0.0.0',
        ROUTER_INTERNAL_BASE_URL: 'http://router.internal'
      })
    )
  ).toThrow('ROUTER_GATEWAY_API_KEY must be configured')
  expect(() =>
    validateGatewayRuntimeConfig(
      loadConfig({
        HOST: '0.0.0.0',
        ROUTER_INTERNAL_BASE_URL: 'http://router.internal',
        ROUTER_GATEWAY_API_KEY: STRONG_ROUTER_GATEWAY_API_KEY
      })
    )
  ).toThrow('ROUTER_GATEWAY_DATABASE_URL must be configured')
  expect(() =>
    validateGatewayRuntimeConfig(
      loadConfig({
        HOST: '0.0.0.0',
        ROUTER_INTERNAL_BASE_URL: 'http://router.internal',
        ROUTER_GATEWAY_API_KEY: STRONG_ROUTER_GATEWAY_API_KEY,
        ROUTER_GATEWAY_DATABASE_URL: 'postgres://postgres:postgres@db/gateway'
      })
    )
  ).toThrow('ROUTER_GATEWAY_CANCELLATION_SECRET_KEY must be configured')
  expect(() =>
    validateGatewayRuntimeConfig(
      loadConfig({
        HOST: '0.0.0.0',
        ROUTER_INTERNAL_BASE_URL: 'http://router.internal',
        ROUTER_GATEWAY_API_KEY: STRONG_ROUTER_GATEWAY_API_KEY,
        ROUTER_GATEWAY_DATABASE_URL: 'postgres://postgres:postgres@db/gateway',
        ROUTER_GATEWAY_CANCELLATION_SECRET_KEY: STRONG_CANCELLATION_KEY
      })
    )
  ).toThrow('ROUTER_GATEWAY_PUBLIC_BASE_URL must be configured')
})

test('validateGatewayRuntimeConfig validates cancellation key material before serving', () => {
  expect(() =>
    validateGatewayRuntimeConfig(
      loadConfig({
        HOST: '0.0.0.0',
        ROUTER_INTERNAL_BASE_URL: 'http://router.internal',
        ROUTER_GATEWAY_API_KEY: STRONG_ROUTER_GATEWAY_API_KEY,
        ROUTER_GATEWAY_DATABASE_URL: 'postgres://postgres:postgres@db/gateway',
        ROUTER_GATEWAY_CANCELLATION_SECRET_KEY: 'short',
        ROUTER_GATEWAY_PUBLIC_BASE_URL: 'https://gateway.example.com'
      })
    )
  ).toThrow('ROUTER_GATEWAY_CANCELLATION_SECRET_KEY must decode to 32 bytes')

  expect(() =>
    validateGatewayRuntimeConfig(
      loadConfig({
        HOST: '0.0.0.0',
        ROUTER_INTERNAL_BASE_URL: 'http://router.internal',
        ROUTER_GATEWAY_API_KEY: STRONG_ROUTER_GATEWAY_API_KEY,
        ROUTER_GATEWAY_DATABASE_URL: 'postgres://postgres:postgres@db/gateway',
        ROUTER_GATEWAY_CANCELLATION_SECRET_KEY: STRONG_CANCELLATION_KEY,
        ROUTER_GATEWAY_PUBLIC_BASE_URL: 'https://gateway.example.com'
      })
    )
  ).not.toThrow()
})
