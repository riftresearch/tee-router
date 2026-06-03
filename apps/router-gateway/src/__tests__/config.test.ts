import { expect, test } from 'bun:test'

import { loadConfig, validateGatewayRuntimeConfig } from '../config'

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
  ).toThrow('ROUTER_GATEWAY_PUBLIC_BASE_URL must be configured')
})

const FULLY_CONFIGURED_PUBLIC_ENV: Record<string, string | undefined> = {
  HOST: '0.0.0.0',
  ROUTER_INTERNAL_BASE_URL: 'http://router.internal',
  ROUTER_GATEWAY_API_KEY: STRONG_ROUTER_GATEWAY_API_KEY,
  ROUTER_GATEWAY_PUBLIC_BASE_URL: 'https://gateway.example.com',
  ETH_RPC_URL: 'https://eth.example.com',
  BASE_RPC_URL: 'https://base.example.com',
  ARBITRUM_RPC_URL: 'https://arb.example.com',
  REDIS_URL: 'redis://redis:6379'
}

test('validateGatewayRuntimeConfig accepts a fully configured public bind', () => {
  expect(() =>
    validateGatewayRuntimeConfig(loadConfig({ ...FULLY_CONFIGURED_PUBLIC_ENV }))
  ).not.toThrow()
})

test('validateGatewayRuntimeConfig requires an RPC URL for every supported EVM chain', () => {
  const env = { ...FULLY_CONFIGURED_PUBLIC_ENV }
  delete env.BASE_RPC_URL
  expect(() => validateGatewayRuntimeConfig(loadConfig(env))).toThrow(
    'BASE_RPC_URL must be configured'
  )
})

test('validateGatewayRuntimeConfig requires REDIS_URL on a public bind', () => {
  const env = { ...FULLY_CONFIGURED_PUBLIC_ENV }
  delete env.REDIS_URL
  expect(() => validateGatewayRuntimeConfig(loadConfig(env))).toThrow(
    'REDIS_URL must be configured'
  )
})

test('loadConfig surfaces evmRpcUrls and redisUrl', () => {
  const config = loadConfig({ ...FULLY_CONFIGURED_PUBLIC_ENV })
  expect(config.evmRpcUrls['evm:8453']).toBe('https://base.example.com')
  expect(config.redisUrl).toBe('redis://redis:6379')
})

test('loadConfig rejects a non-redis REDIS_URL', () => {
  expect(() => loadConfig({ REDIS_URL: 'http://redis:6379' })).toThrow(
    'Invalid REDIS_URL'
  )
})
