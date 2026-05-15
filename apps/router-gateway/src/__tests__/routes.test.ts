import { describe, expect, test } from 'bun:test'
import { privateKeyToAccount } from 'viem/accounts'

import { MAX_GATEWAY_JSON_BODY_BYTES, createApp } from '../app'
import {
  RouterCancellationSecretBox,
  hashRefundToken
} from '../cancellations/crypto'
import {
  DEFAULT_REFUND_SIGNATURE_GATEWAY,
  REFUND_EIP712_DOMAIN,
  REFUND_EIP712_TYPES,
  MAX_REFUND_SIGNATURE_DEADLINE_SECONDS,
  RefundAuthorizationService,
  refundEip712Domain
} from '../cancellations/service'
import type {
  ClaimRefundAuthorizationExpectation,
  RefundAuthorizationStore,
  SaveRefundAuthorizationInput,
  StoredRefundAuthorization
} from '../cancellations/store'
import type { GatewayConfig } from '../config'
import { createDependencyHealthMonitor } from '../health'
import type { FetchLike } from '../internal/router-client'

const QUOTE_ID = '00000000-0000-4000-8000-000000000001'
const ORDER_ID = '00000000-0000-4000-8000-000000000002'
const LIMIT_QUOTE_ID = '00000000-0000-4000-8000-000000000003'
const IDEMPOTENCY_KEY = 'gateway-test-idempotency-key'
const TO_ADDRESS = '0x1111111111111111111111111111111111111111'
const FROM_ADDRESS = 'bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh'
const BTC_ADDRESS = 'bcrt1q2pfqp8a574jxyszmk0h5rxf02wwkpaf4hd8009'
const ROUTER_GATEWAY_API_KEY = 'gateway-secret-00000000000000000'
const REFUND_ACCOUNT = privateKeyToAccount(
  '0x1000000000000000000000000000000000000000000000000000000000000001'
)

describe('router gateway routes', () => {
  test('serves OpenAPI 3.1 from the live route registry', async () => {
    const app = createApp(testConfig())
    const response = await app.request('/openapi.json')
    const body = await response.json()

    expect(response.status).toBe(200)
    expect(body.openapi).toBe('3.1.0')
    expect(body.paths['/health']).toBeDefined()
    expect(body.paths['/providers']).toBeDefined()
    expect(body.paths['/health/dependencies']).toBeUndefined()
    expect(body.paths['/status']).toBeUndefined()
    expect(body.paths['/quote']).toBeDefined()
    expect(body.paths['/order/market']).toBeDefined()
    expect(body.paths['/order/limit']).toBeDefined()
    expect(body.paths['/order/{orderId}/cancel']).toBeDefined()
    expect(Object.keys(body.paths)).toEqual([
      '/quote',
      '/order/market',
      '/order/limit',
      '/order/{orderId}/cancel',
      '/health',
      '/providers'
    ])
    expect(Object.keys(body.components.schemas.QuoteRequest.properties)).toEqual([
      'from',
      'to',
      'amountFormat',
      'toAddress',
      'orderType',
      'fromAmount',
      'toAmount',
      'maxSlippage'
    ])
    expect(
      Object.keys(body.components.schemas.OrderMarketRequest.properties)
    ).toEqual([
      'quoteId',
      'fromAddress',
      'toAddress',
      'refundAddress',
      'integrator',
      'idempotencyKey',
      'refundMode',
      'refundAuthorizer',
      'amountFormat'
    ])
    expect(Object.keys(body.components.schemas.OrderLimitRequest.properties)).toEqual([
      'from',
      'to',
      'amountFormat',
      'fromAddress',
      'toAddress',
      'fromAmount',
      'toAmount',
      'price',
      'refundAddress',
      'refundMode',
      'refundAuthorizer',
      'integrator',
      'idempotencyKey'
    ])
    expect(
      body.components.schemas.OrderLimitRequest.properties.refundAuthorizer.example
    ).toBe('0x2222222222222222222222222222222222222222')
    expect(body.components.schemas.HealthResponse.properties.status.example).toBe(
      'ok'
    )
    expect(body.servers).toEqual([
      {
        url: 'http://localhost:3000',
        description: 'Local development'
      }
    ])
  })

  test('serves the configured public base URL in OpenAPI servers', async () => {
    const app = createApp({
      ...testConfig(),
      publicBaseUrl: 'https://router-gateway.example.com'
    })
    const response = await app.request('/openapi.json')
    const body = await response.json()

    expect(response.status).toBe(200)
    expect(body.servers).toEqual([
      {
        url: 'https://router-gateway.example.com',
        description: 'Production'
      }
    ])
  })

  test('serves the public health check shape', async () => {
    const app = createApp(testConfig())
    const response = await app.request('/health')
    const body = await response.json()

    expect(response.status).toBe(200)
    expect(Object.keys(body).sort()).toEqual(['status', 'timestamp'])
    expect(body.status).toBe('ok')
    expect(new Date(body.timestamp).toISOString()).toBe(body.timestamp)
  })

  test('serves cached dependency health without exposing target URLs', async () => {
    const calls: RecordedCall[] = []
    const config = {
      ...testConfig(),
      healthTargets: [
        {
          name: 'router-api',
          url: 'http://router.internal/api/v1/provider-health',
          method: 'GET' as const,
          timeoutMs: 1_000,
          response: 'routerProviderHealth' as const
        }
      ]
    }
    const monitor = createDependencyHealthMonitor(
      config,
      mockFetch(calls, async () =>
        Response.json({
          status: 'ok',
          timestamp: '2026-04-30T16:46:58.105Z',
          providers: [
            {
              provider: 'hyperliquid',
              status: 'ok',
              checked_at: '2026-04-30T16:46:58.105Z',
              latency_ms: 20,
              http_status: 200
            }
          ]
        })
      )
    )
    await monitor.refresh()

    const app = createApp(config, { dependencyHealthMonitor: monitor })
    const response = await app.request('/providers')
    const body = await response.json()

    expect(response.status).toBe(200)
    expect(body.status).toBe('ok')
    expect(body.dependencies).toHaveLength(1)
    expect(body.dependencies[0]).toMatchObject({
      name: 'hyperliquid',
      status: 'reachable',
      checkedAt: '2026-04-30T16:46:58.105Z'
    })
    expect(Object.keys(body.dependencies[0]).sort()).toEqual([
      'checkedAt',
      'name',
      'status'
    ])
    expect(body.dependencies[0].url).toBeUndefined()
    expect(calls[0]?.path).toBe('/api/v1/provider-health')

    const oldResponse = await app.request('/health/dependencies')
    expect(oldResponse.status).toBe(404)
  })

  test('maps unknown router provider health statuses to unknown, not reachable', async () => {
    const calls: RecordedCall[] = []
    const config = {
      ...testConfig(),
      healthTargets: [
        {
          name: 'router-api',
          url: 'http://router.internal/api/v1/provider-health',
          method: 'GET' as const,
          timeoutMs: 1_000,
          response: 'routerProviderHealth' as const
        }
      ]
    }
    const monitor = createDependencyHealthMonitor(
      config,
      mockFetch(calls, async () =>
        Response.json({
          status: 'ok',
          providers: [
            {
              provider: 'unit',
              status: 'maintenance_mode',
              checked_at: '2026-04-30T16:46:58.105Z'
            }
          ]
        })
      )
    )
    await monitor.refresh()

    const app = createApp(config, { dependencyHealthMonitor: monitor })
    const response = await app.request('/providers')
    const body = await response.json()

    expect(response.status).toBe(200)
    expect(body.dependencies).toEqual([
      {
        name: 'unit',
        status: 'unknown',
        checkedAt: '2026-04-30T16:46:58.105Z'
      }
    ])
  })

  test('router provider health refresh replaces stale provider rows', async () => {
    const calls: RecordedCall[] = []
    let refresh = 0
    const config = {
      ...testConfig(),
      healthTargets: [
        {
          name: 'router-api',
          url: 'http://router.internal/api/v1/provider-health',
          method: 'GET' as const,
          timeoutMs: 1_000,
          response: 'routerProviderHealth' as const
        }
      ]
    }
    const monitor = createDependencyHealthMonitor(
      config,
      mockFetch(calls, async () => {
        refresh += 1
        return Response.json({
          status: 'ok',
          providers:
            refresh === 1
              ? [
                  {
                    provider: 'hyperliquid',
                    status: 'down',
                    checked_at: '2026-04-30T16:46:58.105Z'
                  },
                  {
                    provider: 'unit',
                    status: 'ok',
                    checked_at: '2026-04-30T16:46:58.105Z'
                  }
                ]
              : [
                  {
                    provider: 'unit',
                    status: 'ok',
                    checked_at: '2026-04-30T16:47:58.105Z'
                  }
                ]
        })
      })
    )

    await monitor.refresh()
    await monitor.refresh()

    const app = createApp(config, { dependencyHealthMonitor: monitor })
    const response = await app.request('/providers')
    const body = await response.json()

    expect(response.status).toBe(200)
    expect(body.status).toBe('ok')
    expect(body.dependencies).toEqual([
      {
        name: 'unit',
        status: 'reachable',
        checkedAt: '2026-04-30T16:47:58.105Z'
      }
    ])
  })

  test('serves fully permissive CORS headers', async () => {
    const app = createApp(testConfig())
    const response = await app.request('/health', {
      headers: {
        origin: 'https://example.com'
      }
    })

    expect(response.status).toBe(200)
    expect(response.headers.get('access-control-allow-origin')).toBe('*')
  })

  test('rejects oversized public request bodies before routing upstream', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      fetch: mockFetch(calls, async () => Response.json({ message: 'unexpected' }))
    })

    const oversizedBody = JSON.stringify({
      from: 'Bitcoin.BTC',
      to: 'Ethereum.USDC',
      toAddress: TO_ADDRESS,
      fromAmount: '1',
      maxSlippage: '1',
      padding: 'x'.repeat(MAX_GATEWAY_JSON_BODY_BYTES)
    })
    const response = await app.request('/quote', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'content-length': String(oversizedBody.length)
      },
      body: oversizedBody
    })
    const body = await response.json()

    expect(response.status).toBe(413)
    expect(body.error.code).toBe('PAYLOAD_TOO_LARGE')
    expect(calls).toHaveLength(0)
  })

  test('returns the standard error envelope for malformed public JSON bodies', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      fetch: mockFetch(calls, async () => Response.json({ message: 'unexpected' }))
    })

    const response = await app.request('/quote', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: 'not-json'
    })
    const body = await response.json()

    expect(response.status).toBe(400)
    expect(body.error.code).toBe('VALIDATION_ERROR')
    expect(body.error.message).toBe('Malformed JSON in request body')
    expect(calls).toHaveLength(0)
  })

  test('returns the standard error envelope for public schema failures', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      fetch: mockFetch(calls, async () => Response.json({ message: 'unexpected' }))
    })

    const response = await app.request('/quote', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        from: 'Bitcoin.BTC',
        to: 'Ethereum.USDC',
        toAddress: TO_ADDRESS,
        amountFormat: 'readable'
      })
    })
    const body = await response.json()

    expect(response.status).toBe(400)
    expect(body.error.code).toBe('VALIDATION_ERROR')
    expect(body.error.message).toBe('exactly one of fromAmount or toAmount is required')
    expect(body.error.details).toEqual({
      target: 'json',
      issues: [
        {
          path: 'fromAmount',
          message: 'exactly one of fromAmount or toAmount is required'
        }
      ]
    })
    expect(calls).toHaveLength(0)
  })

  test('rejects oversized successful upstream responses without buffering them into the client response', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      fetch: mockFetch(
        calls,
        async () => new Response('x'.repeat(1024 * 1024 + 1), { status: 200 })
      )
    })

    const response = await app.request('/quote', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        from: 'Bitcoin.BTC',
        to: 'Ethereum.USDC',
        toAddress: TO_ADDRESS,
        fromAmount: '1',
        maxSlippage: '1',
        amountFormat: 'readable'
      })
    })
    const bodyText = await response.text()
    const body = JSON.parse(bodyText)

    expect(response.status).toBe(502)
    expect(body.error.code).toBe('UPSTREAM_ERROR')
    expect(body.error.message).toBe(
      'upstream router API response exceeded 1048576 bytes'
    )
    expect(bodyText.length).toBeLessThan(1_000)
    expect(calls).toHaveLength(1)
  })

  test('rejects malformed successful upstream quote responses as controlled upstream errors', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      fetch: mockFetch(calls, async () =>
        Response.json({ quote: { type: 'market_order' } })
      )
    })

    const response = await app.request('/quote', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        from: 'Bitcoin.BTC',
        to: 'Ethereum.USDC',
        toAddress: TO_ADDRESS,
        fromAmount: '1',
        maxSlippage: '1',
        amountFormat: 'readable'
      })
    })
    const body = await response.json()

    expect(response.status).toBe(502)
    expect(body.error.code).toBe('UPSTREAM_ERROR')
    expect(body.error.message).toBe(
      'internal router API returned malformed quote response'
    )
    expect(body.error.details).toEqual({ upstreamStatus: 502 })
  })

  test('rejects oversized upstream quote fields before presentation', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      fetch: mockFetch(calls, async () => {
        const quote = internalQuote()
        quote.quote.payload.amount_out = '1'.repeat(97)
        return Response.json(quote)
      })
    })

    const response = await app.request('/quote', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        from: 'Bitcoin.BTC',
        to: 'Ethereum.USDC',
        toAddress: TO_ADDRESS,
        fromAmount: '1',
        maxSlippage: '1',
        amountFormat: 'readable'
      })
    })
    const body = await response.json()

    expect(response.status).toBe(502)
    expect(body.error.code).toBe('UPSTREAM_ERROR')
    expect(body.error.message).toBe(
      'internal router API returned malformed quote response'
    )
    expect(body.error.details).toEqual({ upstreamStatus: 502 })
  })

  test('accepts long routed upstream provider ids', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      fetch: mockFetch(calls, async () => {
        const quote = internalQuote()
        quote.quote.payload.provider_id =
          'path:' + 'universal_router_swap:velora:evm:8453:native->evm:8453:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913|'.repeat(8)
        return Response.json(quote)
      })
    })

    const response = await app.request('/quote', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        from: 'Bitcoin.BTC',
        to: 'Ethereum.USDC',
        toAddress: TO_ADDRESS,
        fromAmount: '1',
        maxSlippage: '1',
        amountFormat: 'readable'
      })
    })
    const body = await response.json()

    expect(response.status).toBe(201)
    expect(body.quoteId).toBe(QUOTE_ID)
  })

  test('accepts long routed upstream provider ids for limit quotes', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      fetch: mockFetch(calls, async () => {
        const quote = internalLimitQuote()
        quote.quote.payload.provider_id =
          'path:' + 'unit_deposit:unit:bitcoin:native->hyperliquid:UBTC|hyperliquid_trade:hyperliquid|'.repeat(8)
        return Response.json(quote, { status: 201 })
      })
    })

    const response = await app.request('/quote', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        from: 'Bitcoin.BTC',
        to: 'Ethereum.USDC',
        toAddress: TO_ADDRESS,
        fromAmount: '1',
        toAmount: '1000',
        orderType: 'limit',
        amountFormat: 'readable'
      })
    })
    const body = await response.json()

    expect(response.status).toBe(201)
    expect(body.quoteId).toBe(LIMIT_QUOTE_ID)
  })

  test('rejects zero upstream quote amounts as controlled upstream errors', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      fetch: mockFetch(calls, async () => {
        const quote = internalQuote()
        quote.quote.payload.amount_out = '0'
        return Response.json(quote)
      })
    })

    const response = await app.request('/quote', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        from: 'Bitcoin.BTC',
        to: 'Ethereum.USDC',
        toAddress: TO_ADDRESS,
        fromAmount: '1',
        maxSlippage: '1',
        amountFormat: 'readable'
      })
    })
    const body = await response.json()

    expect(response.status).toBe(502)
    expect(body.error.code).toBe('UPSTREAM_ERROR')
    expect(body.error.message).toBe(
      'internal router API returned malformed quote response'
    )
    expect(body.error.details).toEqual({ upstreamStatus: 502 })
  })

  test('rejects upstream quote slippage outside the public bps range', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      fetch: mockFetch(calls, async () => {
        const quote = internalQuote()
        quote.quote.payload.slippage_bps = 10_001
        return Response.json(quote)
      })
    })

    const response = await app.request('/quote', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        from: 'Bitcoin.BTC',
        to: 'Ethereum.USDC',
        toAddress: TO_ADDRESS,
        fromAmount: '1',
        maxSlippage: '1',
        amountFormat: 'readable'
      })
    })
    const body = await response.json()

    expect(response.status).toBe(502)
    expect(body.error.code).toBe('UPSTREAM_ERROR')
    expect(body.error.message).toBe(
      'internal router API returned malformed quote response'
    )
    expect(body.error.details).toEqual({ upstreamStatus: 502 })
  })

  test('rejects oversized public string fields before routing upstream', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      fetch: mockFetch(calls, async () => Response.json({ message: 'unexpected' }))
    })

    const response = await app.request('/quote', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        from: 'Bitcoin.BTC',
        to: 'Ethereum.USDC',
        toAddress: 'b'.repeat(129),
        fromAmount: '1',
        maxSlippage: '1',
        amountFormat: 'readable'
      })
    })

    expect(response.status).toBe(400)
    expect(calls).toHaveLength(0)
  })

  test('rejects market-order cancelAfter before routing upstream', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      fetch: mockFetch(calls, async () => Response.json({ message: 'unexpected' }))
    })

    const response = await app.request('/order/market', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        quoteId: QUOTE_ID,
        idempotencyKey: IDEMPOTENCY_KEY,
        fromAddress: FROM_ADDRESS,
        toAddress: TO_ADDRESS,
        refundAuthorizer: REFUND_ACCOUNT.address,
        cancelAfter: '2026-05-05T00:00:00.000Z'
      })
    })

    expect(response.status).toBe(400)
    expect(calls).toHaveLength(0)
  })

  test('requires market-order idempotency keys before routing upstream', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      fetch: mockFetch(calls, async () => Response.json({ message: 'unexpected' }))
    })

    const response = await app.request('/order/market', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        quoteId: QUOTE_ID,
        fromAddress: FROM_ADDRESS,
        toAddress: TO_ADDRESS,
        refundAuthorizer: REFUND_ACCOUNT.address
      })
    })

    expect(response.status).toBe(400)
    expect(calls).toHaveLength(0)
  })

  test('rejects weak market-order idempotency keys before routing upstream', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      fetch: mockFetch(calls, async () => Response.json({ message: 'unexpected' }))
    })

    const response = await app.request('/order/market', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        quoteId: QUOTE_ID,
        idempotencyKey: 'short-key',
        fromAddress: FROM_ADDRESS,
        toAddress: TO_ADDRESS,
        refundAuthorizer: REFUND_ACCOUNT.address
      })
    })

    expect(response.status).toBe(400)
    expect(calls).toHaveLength(0)
  })

  test('rejects non-token market-order idempotency keys before routing upstream', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      fetch: mockFetch(calls, async () => Response.json({ message: 'unexpected' }))
    })

    const response = await app.request('/order/market', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        quoteId: QUOTE_ID,
        idempotencyKey: 'gateway key with spaces',
        fromAddress: FROM_ADDRESS,
        toAddress: TO_ADDRESS,
        refundAuthorizer: REFUND_ACCOUNT.address
      })
    })
    const body = await response.json()

    expect(response.status).toBe(400)
    expect(body.error.message).toBe(
      'idempotencyKey must contain only letters, numbers, dot, underscore, colon, or hyphen'
    )
    expect(calls).toHaveLength(0)
  })

  test('rejects non-base64url refund tokens before resolving cancellation auth', async () => {
    const app = createApp(testConfig(), {
      fetch: mockFetch([], async () => Response.json({ message: 'unexpected' }))
    })

    const response = await app.request(`/order/${ORDER_ID}/cancel`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        refundToken: 'token with spaces',
        amountFormat: 'raw'
      })
    })
    const body = await response.json()

    expect(response.status).toBe(400)
    expect(body.error.message).toBe('refundToken must be base64url text')
  })

  test('rejects invalid EVM quote recipient addresses before routing upstream', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      fetch: mockFetch(calls, async () => Response.json({ message: 'unexpected' }))
    })

    const response = await app.request('/quote', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        from: 'Bitcoin.BTC',
        to: 'Ethereum.USDC',
        toAddress: 'not-an-evm-address',
        fromAmount: '1',
        maxSlippage: '1',
        amountFormat: 'readable'
      })
    })
    const body = await response.json()

    expect(response.status).toBe(400)
    expect(body.error.message).toBe('toAddress must be a valid EVM address')
    expect(calls).toHaveLength(0)
  })

  test('rejects invalid Bitcoin quote recipient addresses before routing upstream', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      fetch: mockFetch(calls, async () => Response.json({ message: 'unexpected' }))
    })

    const response = await app.request('/quote', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        from: 'Base.USDC',
        to: 'Bitcoin.BTC',
        toAddress: TO_ADDRESS,
        fromAmount: '100000000',
        maxSlippage: '100',
        amountFormat: 'raw'
      })
    })
    const body = await response.json()

    expect(response.status).toBe(400)
    expect(body.error.message).toBe('toAddress must be a valid Bitcoin address')
    expect(calls).toHaveLength(0)
  })

  test('rejects Bitcoin quote recipients from disabled address networks', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(
      { ...testConfig(), bitcoinAddressNetworks: ['mainnet'] },
      {
        fetch: mockFetch(calls, async () => Response.json({ message: 'unexpected' }))
      }
    )

    const response = await app.request('/quote', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        from: 'Base.USDC',
        to: 'Bitcoin.BTC',
        toAddress: BTC_ADDRESS,
        fromAmount: '100000000',
        maxSlippage: '100',
        amountFormat: 'raw'
      })
    })
    const body = await response.json()

    expect(response.status).toBe(400)
    expect(body.error.message).toBe('toAddress must be a valid Bitcoin address')
    expect(calls).toHaveLength(0)
  })

  test('allows market quotes without maxSlippage', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      fetch: mockFetch(calls, async () => {
        const quote = internalQuote()
        quote.quote.payload.min_amount_out = null as any
        quote.quote.payload.slippage_bps = null as any
        return Response.json(quote, { status: 201 })
      })
    })

    const response = await app.request('/quote', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        from: 'Bitcoin.BTC',
        to: 'Ethereum.USDC',
        toAddress: TO_ADDRESS,
        fromAmount: '1',
        amountFormat: 'readable'
      })
    })
    const body = await response.json()

    expect(response.status).toBe(201)
    expect(calls[0]?.body).toMatchObject({
      type: 'market_order',
      kind: 'exact_in',
      amount_in: '100000000'
    })
    expect(calls[0]?.body).not.toHaveProperty('slippage_bps')
    expect(body).not.toHaveProperty('maxSlippage')
    expect(body).not.toHaveProperty('minOut')
  })

  test('rejects invalid EVM order refund addresses before creating orders', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      fetch: mockFetch(calls, async (path) => {
        if (path === `/api/v1/quotes/${QUOTE_ID}`) {
          return Response.json(internalBaseToBitcoinLimitQuote())
        }

        return Response.json({ message: 'unexpected' }, { status: 500 })
      })
    })

    const response = await app.request('/order/market', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        quoteId: QUOTE_ID,
        idempotencyKey: IDEMPOTENCY_KEY,
        fromAddress: '0x2222222222222222222222222222222222222222',
        toAddress: BTC_ADDRESS,
        refundAddress: 'not-an-evm-address',
        refundMode: 'token',
        refundAuthorizer: null,
        amountFormat: 'raw'
      })
    })
    const body = await response.json()

    expect(response.status).toBe(400)
    expect(body.error.message).toBe('refundAddress must be a valid EVM address')
    expect(calls.map((call) => call.path)).toEqual([`/api/v1/quotes/${QUOTE_ID}`])
  })

  test('rejects invalid Bitcoin order recipients before creating orders', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      fetch: mockFetch(calls, async (path) => {
        if (path === `/api/v1/quotes/${QUOTE_ID}`) {
          return Response.json(internalBaseToBitcoinLimitQuote())
        }

        return Response.json({ message: 'unexpected' }, { status: 500 })
      })
    })

    const response = await app.request('/order/market', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        quoteId: QUOTE_ID,
        idempotencyKey: IDEMPOTENCY_KEY,
        fromAddress: '0x2222222222222222222222222222222222222222',
        toAddress: TO_ADDRESS,
        refundMode: 'token',
        refundAuthorizer: null,
        amountFormat: 'raw'
      })
    })
    const body = await response.json()

    expect(response.status).toBe(400)
    expect(body.error.message).toBe('toAddress must be a valid Bitcoin address')
    expect(calls.map((call) => call.path)).toEqual([`/api/v1/quotes/${QUOTE_ID}`])
  })

  test('rejects order quotes with unsupported address-validation chains', async () => {
    const calls: RecordedCall[] = []
    const unsupportedQuote = internalQuote()
    unsupportedQuote.quote.payload.source_asset = {
      chain: 'hyperliquid',
      asset: 'USDC'
    }
    const app = createApp(testConfig(), {
      fetch: mockFetch(calls, async (path) => {
        if (path === `/api/v1/quotes/${QUOTE_ID}`) {
          return Response.json(unsupportedQuote)
        }

        return Response.json({ message: 'unexpected' }, { status: 500 })
      })
    })

    const response = await app.request('/order/market', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        quoteId: QUOTE_ID,
        idempotencyKey: IDEMPOTENCY_KEY,
        fromAddress: TO_ADDRESS,
        toAddress: TO_ADDRESS,
        refundMode: 'token',
        refundAuthorizer: null,
        amountFormat: 'raw'
      })
    })
    const body = await response.json()

    expect(response.status).toBe(400)
    expect(body.error.message).toBe(
      'fromAddress cannot be validated for unsupported chain'
    )
    expect(calls.map((call) => call.path)).toEqual([`/api/v1/quotes/${QUOTE_ID}`])
  })

  test('sanitizes upstream error messages and details', async () => {
    const app = createApp(testConfig(), {
      fetch: mockFetch([], async (path) => {
        if (path === '/api/v1/quotes') {
          return Response.json(
            {
              error: {
                message: 'upstream quote failed',
                privateTrace: 'should not leave the gateway'
              }
            },
            { status: 500 }
          )
        }

        return Response.json({ message: 'not found' }, { status: 404 })
      })
    })

    const response = await app.request('/quote', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        from: 'Bitcoin.BTC',
        to: 'Ethereum.USDC',
        toAddress: TO_ADDRESS,
        fromAmount: '1',
        maxSlippage: '1',
        amountFormat: 'readable'
      })
    })
    const body = await response.json()

    expect(response.status).toBe(502)
    expect(body.error.message).toBe('upstream router API returned 500')
    expect(body.error.details).toEqual({ upstreamStatus: 500 })
  })

  test('sanitizes upstream transport failure messages', async () => {
    const app = createApp(testConfig(), {
      fetch: async () => {
        throw new Error(
          'transport failed with secret 0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
        )
      }
    })

    const response = await app.request('/quote', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        from: 'Bitcoin.BTC',
        to: 'Ethereum.USDC',
        toAddress: TO_ADDRESS,
        fromAmount: '1',
        maxSlippage: '1',
        amountFormat: 'readable'
      })
    })
    const body = await response.json()

    expect(response.status).toBe(502)
    expect(body.error.message).toBe('upstream router API request failed')
    expect(JSON.stringify(body)).not.toContain(
      '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
    )
  })

  test('maps upstream timeouts to gateway timeout responses', async () => {
    const app = createApp(
      {
        ...testConfig(),
        requestTimeoutMs: 1
      },
      {
        fetch: async (_input, init) =>
          new Promise<Response>((_resolve, reject) => {
            const signal = init?.signal
            if (!(signal instanceof AbortSignal)) return
            if (signal.aborted) {
              reject(new DOMException('aborted', 'AbortError'))
              return
            }
            signal.addEventListener(
              'abort',
              () => reject(new DOMException('aborted', 'AbortError')),
              { once: true }
            )
          })
      }
    )

    const response = await app.request('/quote', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        from: 'Bitcoin.BTC',
        to: 'Ethereum.USDC',
        toAddress: TO_ADDRESS,
        fromAmount: '1',
        maxSlippage: '1',
        amountFormat: 'readable'
      })
    })
    const body = await response.json()

    expect(response.status).toBe(504)
    expect(body.error.code).toBe('UPSTREAM_ERROR')
    expect(body.error.details).toEqual({ upstreamStatus: 504 })
  })

  test('translates a readable exact-in quote request to the internal router API', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      fetch: mockFetch(calls, async (path) => {
        if (path === '/api/v1/quotes') {
          return Response.json(internalQuote(), { status: 201 })
        }

        return Response.json({ message: 'not found' }, { status: 404 })
      })
    })

    const response = await app.request('/quote', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        from: 'Bitcoin.BTC',
        to: 'Ethereum.USDC',
        toAddress: TO_ADDRESS,
        fromAmount: '1.25',
        maxSlippage: '1.5',
        amountFormat: 'readable'
      })
    })

    expect(response.status).toBe(201)
    expect(calls[0]?.method).toBe('POST')
    expect(calls[0]?.path).toBe('/api/v1/quotes')
    expect(calls[0]?.body).toEqual({
      type: 'market_order',
      from_asset: {
        chain: 'bitcoin',
        asset: 'native'
      },
      to_asset: {
        chain: 'evm:1',
        asset: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
      },
      recipient_address: TO_ADDRESS,
      kind: 'exact_in',
      amount_in: '125000000',
      slippage_bps: 150
    })

    const body = await response.json()
    expect(body).toMatchObject({
      quoteId: QUOTE_ID,
      from: 'Bitcoin.BTC',
      to: 'Ethereum.USDC',
      expectedOut: '100',
      minOut: '99',
      maxSlippage: '1.5',
      amountFormat: 'readable'
    })
  })

  test('sends the configured gateway bearer key to every internal router API request', async () => {
    const calls: RecordedCall[] = []
    const authorizations = new Map<string, string | null>()
    const app = createApp(testConfig(), {
      refundAuthorizationService: testRefundAuthorizationService(
        failingRefundAuthorizationStore()
      ),
      fetch: mockFetch(calls, async (path, request) => {
        authorizations.set(
          `${request.method} ${path}`,
          request.headers.get('authorization')
        )

        if (path === '/api/v1/quotes') {
          return Response.json(internalQuote(), { status: 201 })
        }

        if (path === `/api/v1/quotes/${QUOTE_ID}`) {
          return Response.json(internalQuote())
        }

        if (path === '/api/v1/orders') {
          return Response.json(internalOrder(), { status: 201 })
        }

        if (path === `/api/v1/orders/${ORDER_ID}/cancellations`) {
          return Response.json(internalOrder({ status: 'refunding' }))
        }

        return Response.json({ message: 'not found' }, { status: 404 })
      })
    })

    await app.request('/quote', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        from: 'Bitcoin.BTC',
        to: 'Ethereum.USDC',
        toAddress: TO_ADDRESS,
        fromAmount: '1.25',
        maxSlippage: '1.5',
        amountFormat: 'readable'
      })
    })

    const consoleError = console.error
    console.error = () => undefined
    try {
      await app.request('/order/market', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          quoteId: QUOTE_ID,
          idempotencyKey: IDEMPOTENCY_KEY,
          fromAddress: FROM_ADDRESS,
          toAddress: TO_ADDRESS,
          refundMode: 'token',
          refundAuthorizer: null
        })
      })
    } finally {
      console.error = consoleError
    }

    const expectedAuthorization = `Bearer ${ROUTER_GATEWAY_API_KEY}`
    expect(authorizations.get('POST /api/v1/quotes')).toBe(expectedAuthorization)
    expect(authorizations.get(`GET /api/v1/quotes/${QUOTE_ID}`)).toBe(
      expectedAuthorization
    )
    expect(authorizations.get('POST /api/v1/orders')).toBe(expectedAuthorization)
    expect(
      authorizations.get(`POST /api/v1/orders/${ORDER_ID}/cancellations`)
    ).toBe(expectedAuthorization)
  })

  test('translates a raw limit quote request to the internal router API', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      fetch: mockFetch(calls, async (path) => {
        if (path === '/api/v1/quotes') {
          return Response.json(internalBaseToBitcoinLimitQuote(), { status: 201 })
        }

        return Response.json({ message: 'not found' }, { status: 404 })
      })
    })

    const response = await app.request('/quote', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        orderType: 'limit_order',
        from: 'Base.USDC',
        to: 'Bitcoin.BTC',
        toAddress: BTC_ADDRESS,
        fromAmount: '100000000',
        toAmount: '100000',
        amountFormat: 'raw'
      })
    })

    expect(response.status).toBe(201)
    expect(calls[0]?.body).toEqual({
      type: 'limit_order',
      from_asset: {
        chain: 'evm:8453',
        asset: '0x833589fcd6edb6e08f4c7c32d4f71b54bda02913'
      },
      to_asset: {
        chain: 'bitcoin',
        asset: 'native'
      },
      recipient_address: BTC_ADDRESS,
      input_amount: '100000000',
      output_amount: '100000'
    })

    const body = await response.json()
    expect(body).toMatchObject({
      quoteId: QUOTE_ID,
      orderType: 'limit_order',
      from: 'Base.USDC',
      to: 'Bitcoin.BTC',
      expectedOut: '100000',
      maxIn: '101219825',
      maxSlippage: '0',
      amountFormat: 'raw'
    })
  })

  test('creates a market order and defaults refundAddress to fromAddress', async () => {
    const calls: RecordedCall[] = []
    const store = new InMemoryRefundAuthorizationStore()
    const app = createApp(testConfig(), {
      refundAuthorizationService: testRefundAuthorizationService(store),
      fetch: mockFetch(calls, async (path) => {
        if (path === `/api/v1/quotes/${QUOTE_ID}`) {
          return Response.json(internalQuote())
        }

        if (path === '/api/v1/orders') {
          return Response.json(internalOrder(), { status: 201 })
        }

        return Response.json({ message: 'not found' }, { status: 404 })
      })
    })

    const response = await app.request('/order/market', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        quoteId: QUOTE_ID,
        idempotencyKey: IDEMPOTENCY_KEY,
        fromAddress: FROM_ADDRESS,
        toAddress: TO_ADDRESS,
        refundAuthorizer: REFUND_ACCOUNT.address,
        integrator: 'partner-a'
      })
    })

    expect(response.status).toBe(201)
    expect(calls[1]?.method).toBe('POST')
    expect(calls[1]?.path).toBe('/api/v1/orders')
    expect(calls[1]?.body).toEqual({
      quote_id: QUOTE_ID,
      refund_address: FROM_ADDRESS,
      idempotency_key: IDEMPOTENCY_KEY,
      metadata: {
        integrator: 'partner-a',
        from_address: FROM_ADDRESS,
        to_address: TO_ADDRESS,
        gateway: 'router-gateway'
      }
    })

    const body = await response.json()
    expect(body).toMatchObject({
      orderId: ORDER_ID,
      orderAddress: 'bc1qorderaddress0000000000000000000000000',
      amountToSend: '1.25',
      quoteId: QUOTE_ID,
      refundMode: 'evmSignature',
      refundAuthorizer: REFUND_ACCOUNT.address
    })
    expect(body.cancellationSecret).toBeUndefined()

    const stored = await store.findRefundAuthorization(ORDER_ID)
    expect(stored?.refundMode).toBe('evmSignature')
    expect(stored?.refundAuthorizer).toBe(REFUND_ACCOUNT.address)
  })

  test('cancels the upstream order when refund authorization persistence fails', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      refundAuthorizationService: testRefundAuthorizationService(
        failingRefundAuthorizationStore()
      ),
      fetch: mockFetch(calls, async (path) => {
        if (path === `/api/v1/quotes/${QUOTE_ID}`) {
          return Response.json(internalQuote())
        }

        if (path === '/api/v1/orders') {
          return Response.json(internalOrder(), { status: 201 })
        }

        if (path === `/api/v1/orders/${ORDER_ID}/cancellations`) {
          return Response.json(internalOrder({ status: 'refunding' }))
        }

        return Response.json({ message: 'not found' }, { status: 404 })
      })
    })
    const consoleError = console.error
    console.error = () => undefined

    try {
      const response = await app.request('/order/market', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          quoteId: QUOTE_ID,
          idempotencyKey: IDEMPOTENCY_KEY,
          fromAddress: FROM_ADDRESS,
          toAddress: TO_ADDRESS,
          refundMode: 'token',
          refundAuthorizer: null
        })
      })
      const body = await response.json()

      expect(response.status).toBe(500)
      expect(body.error.message).toBe('Unexpected gateway error')
      expect(calls.at(-1)).toEqual({
        method: 'POST',
        path: `/api/v1/orders/${ORDER_ID}/cancellations`,
        body: {
          cancellation_secret:
            '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
        }
      })
    } finally {
      console.error = consoleError
    }
  })

  test('cancels the upstream order when the created order response is malformed', async () => {
    const calls: RecordedCall[] = []
    const store = new InMemoryRefundAuthorizationStore()
    const app = createApp(testConfig(), {
      refundAuthorizationService: testRefundAuthorizationService(store),
      fetch: mockFetch(calls, async (path) => {
        if (path === `/api/v1/quotes/${QUOTE_ID}`) {
          return Response.json(internalQuote())
        }

        if (path === '/api/v1/orders') {
          const order = internalOrder()
          order.quote.payload.amount_in = 'not-a-raw-amount'
          return Response.json(order, { status: 201 })
        }

        if (path === `/api/v1/orders/${ORDER_ID}/cancellations`) {
          return Response.json(internalOrder({ status: 'refunding' }))
        }

        return Response.json({ message: 'not found' }, { status: 404 })
      })
    })

    const response = await app.request('/order/market', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        quoteId: QUOTE_ID,
        idempotencyKey: IDEMPOTENCY_KEY,
        fromAddress: FROM_ADDRESS,
        toAddress: TO_ADDRESS,
        refundMode: 'token',
        refundAuthorizer: null
      })
    })
    const body = await response.json()

    expect(response.status).toBe(502)
    expect(body.error.message).toBe(
      'internal router API returned malformed order response'
    )
    expect(calls.at(-1)).toEqual({
      method: 'POST',
      path: `/api/v1/orders/${ORDER_ID}/cancellations`,
      body: {
        cancellation_secret:
          '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
      }
    })
    await expect(store.findRefundAuthorization(ORDER_ID)).resolves.toBeUndefined()
  })

  test('rejects malformed created order amounts even when public response uses raw amounts', async () => {
    const calls: RecordedCall[] = []
    const store = new InMemoryRefundAuthorizationStore()
    const app = createApp(testConfig(), {
      refundAuthorizationService: testRefundAuthorizationService(store),
      fetch: mockFetch(calls, async (path) => {
        if (path === `/api/v1/quotes/${QUOTE_ID}`) {
          return Response.json(internalQuote())
        }

        if (path === '/api/v1/orders') {
          const order = internalOrder()
          order.quote.payload.amount_in = 'not-a-raw-amount'
          return Response.json(order, { status: 201 })
        }

        if (path === `/api/v1/orders/${ORDER_ID}/cancellations`) {
          return Response.json(internalOrder({ status: 'refunding' }))
        }

        return Response.json({ message: 'not found' }, { status: 404 })
      })
    })

    const response = await app.request('/order/market', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        quoteId: QUOTE_ID,
        idempotencyKey: IDEMPOTENCY_KEY,
        fromAddress: FROM_ADDRESS,
        toAddress: TO_ADDRESS,
        refundMode: 'token',
        refundAuthorizer: null,
        amountFormat: 'raw'
      })
    })
    const body = await response.json()

    expect(response.status).toBe(502)
    expect(body.error.message).toBe(
      'internal router API returned malformed order response'
    )
    expect(calls.at(-1)?.path).toBe(`/api/v1/orders/${ORDER_ID}/cancellations`)
    await expect(store.findRefundAuthorization(ORDER_ID)).resolves.toBeUndefined()
  })

  test('cancels the upstream order when the created order has zero display amounts', async () => {
    const calls: RecordedCall[] = []
    const store = new InMemoryRefundAuthorizationStore()
    const app = createApp(testConfig(), {
      refundAuthorizationService: testRefundAuthorizationService(store),
      fetch: mockFetch(calls, async (path) => {
        if (path === `/api/v1/quotes/${QUOTE_ID}`) {
          return Response.json(internalQuote())
        }

        if (path === '/api/v1/orders') {
          const order = internalOrder()
          order.quote.payload.amount_out = '0'
          return Response.json(order, { status: 201 })
        }

        if (path === `/api/v1/orders/${ORDER_ID}/cancellations`) {
          return Response.json(internalOrder({ status: 'refunding' }))
        }

        return Response.json({ message: 'not found' }, { status: 404 })
      })
    })

    const response = await app.request('/order/market', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        quoteId: QUOTE_ID,
        idempotencyKey: IDEMPOTENCY_KEY,
        fromAddress: FROM_ADDRESS,
        toAddress: TO_ADDRESS,
        refundMode: 'token',
        refundAuthorizer: null
      })
    })
    const body = await response.json()

    expect(response.status).toBe(502)
    expect(body.error.code).toBe('UPSTREAM_ERROR')
    expect(body.error.message).toBe(
      'internal router API returned malformed order response'
    )
    expect(calls.at(-1)).toEqual({
      method: 'POST',
      path: `/api/v1/orders/${ORDER_ID}/cancellations`,
      body: {
        cancellation_secret:
          '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
      }
    })
    await expect(store.findRefundAuthorization(ORDER_ID)).resolves.toBeUndefined()
  })

  test('rejects create-order responses that omit the cancellation secret as upstream errors', async () => {
    const calls: RecordedCall[] = []
    const store = new InMemoryRefundAuthorizationStore()
    const app = createApp(testConfig(), {
      refundAuthorizationService: testRefundAuthorizationService(store),
      fetch: mockFetch(calls, async (path) => {
        if (path === `/api/v1/quotes/${QUOTE_ID}`) {
          return Response.json(internalQuote())
        }

        if (path === '/api/v1/orders') {
          const order = internalOrder()
          delete (order as { cancellation_secret?: string }).cancellation_secret
          return Response.json(order, { status: 201 })
        }

        return Response.json({ message: 'not found' }, { status: 404 })
      })
    })

    const response = await app.request('/order/market', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        quoteId: QUOTE_ID,
        idempotencyKey: IDEMPOTENCY_KEY,
        fromAddress: FROM_ADDRESS,
        toAddress: TO_ADDRESS,
        refundMode: 'token',
        refundAuthorizer: null
      })
    })
    const body = await response.json()

    expect(response.status).toBe(502)
    expect(body.error.message).toBe(
      'internal router API returned order response without cancellation secret'
    )
    await expect(store.findRefundAuthorization(ORDER_ID)).resolves.toBeUndefined()
  })

  test('creates a limit order from a limit quote', async () => {
    const calls: RecordedCall[] = []
    const store = new InMemoryRefundAuthorizationStore()
    const app = createApp(testConfig(), {
      refundAuthorizationService: testRefundAuthorizationService(store),
      fetch: mockFetch(calls, async (path) => {
        if (path === `/api/v1/quotes/${LIMIT_QUOTE_ID}`) {
          return Response.json(internalLimitQuote())
        }

        if (path === '/api/v1/orders') {
          return Response.json(internalLimitOrder(), { status: 201 })
        }

        return Response.json({ message: 'not found' }, { status: 404 })
      })
    })

    const response = await app.request('/order/market', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        quoteId: LIMIT_QUOTE_ID,
        idempotencyKey: IDEMPOTENCY_KEY,
        fromAddress: FROM_ADDRESS,
        toAddress: TO_ADDRESS,
        refundMode: 'token',
        refundAuthorizer: null,
        integrator: 'limit-loadgen',
        amountFormat: 'raw'
      })
    })

    expect(response.status).toBe(201)
    expect(calls[1]?.path).toBe('/api/v1/orders')
    expect(calls[1]?.body).toEqual({
      quote_id: LIMIT_QUOTE_ID,
      refund_address: FROM_ADDRESS,
      idempotency_key: IDEMPOTENCY_KEY,
      metadata: {
        integrator: 'limit-loadgen',
        from_address: FROM_ADDRESS,
        to_address: TO_ADDRESS,
        gateway: 'router-gateway'
      }
    })

    const body = await response.json()
    expect(body).toMatchObject({
      orderId: ORDER_ID,
      orderType: 'limit_order',
      amountToSend: '125000000',
      maxIn: '125000000',
      expectedOut: '100000000000',
      refundMode: 'token'
    })
  })

  test('creates a limit order with explicit input and output amounts', async () => {
    const calls: RecordedCall[] = []
    const store = new InMemoryRefundAuthorizationStore()
    const app = createApp(testConfig(), {
      refundAuthorizationService: testRefundAuthorizationService(store),
      fetch: mockFetch(calls, async (path) => {
        if (path === '/api/v1/quotes') {
          return Response.json(internalLimitQuote(), { status: 201 })
        }

        if (path === '/api/v1/orders') {
          return Response.json(internalLimitOrder(), { status: 201 })
        }

        return Response.json({ message: 'not found' }, { status: 404 })
      })
    })

    const response = await app.request('/order/limit', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        from: 'Bitcoin.BTC',
        to: 'Ethereum.USDC',
        fromAddress: FROM_ADDRESS,
        toAddress: TO_ADDRESS,
        fromAmount: '1.25',
        toAmount: '100000',
        refundAuthorizer: REFUND_ACCOUNT.address,
        integrator: 'partner-a',
        idempotencyKey: IDEMPOTENCY_KEY
      })
    })

    expect(response.status).toBe(201)
    expect(calls[0]?.method).toBe('POST')
    expect(calls[0]?.path).toBe('/api/v1/quotes')
    expect(calls[0]?.body).toEqual({
      type: 'limit_order',
      from_asset: {
        chain: 'bitcoin',
        asset: 'native'
      },
      to_asset: {
        chain: 'evm:1',
        asset: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
      },
      recipient_address: TO_ADDRESS,
      input_amount: '125000000',
      output_amount: '100000000000'
    })
    expect(calls[1]?.body).toEqual({
      quote_id: LIMIT_QUOTE_ID,
      refund_address: FROM_ADDRESS,
      idempotency_key: IDEMPOTENCY_KEY,
      metadata: {
        integrator: 'partner-a',
        from_address: FROM_ADDRESS,
        to_address: TO_ADDRESS,
        gateway: 'router-gateway',
        order_type: 'limit_order'
      }
    })

    const body = await response.json()
    expect(body).toMatchObject({
      orderId: ORDER_ID,
      orderAddress: 'bc1qorderaddress0000000000000000000000000',
      orderType: 'limit_order',
      amountToSend: '1.25',
      quoteId: LIMIT_QUOTE_ID,
      from: 'Bitcoin.BTC',
      to: 'Ethereum.USDC',
      expectedOut: '100000',
      maxIn: '1.25',
      maxSlippage: '0',
      refundMode: 'evmSignature',
      refundAuthorizer: REFUND_ACCOUNT.address
    })
    expect(body.cancellationSecret).toBeUndefined()
  })

  test('creates a limit order by deriving output from a readable price', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      refundAuthorizationService: testRefundAuthorizationService(
        new InMemoryRefundAuthorizationStore()
      ),
      fetch: mockFetch(calls, async (path) => {
        if (path === '/api/v1/quotes') {
          return Response.json(internalLimitQuote(), { status: 201 })
        }

        if (path === '/api/v1/orders') {
          return Response.json(internalLimitOrder(), { status: 201 })
        }

        return Response.json({ message: 'not found' }, { status: 404 })
      })
    })

    const response = await app.request('/order/limit', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        from: 'Bitcoin.BTC',
        to: 'Ethereum.USDC',
        fromAddress: FROM_ADDRESS,
        toAddress: TO_ADDRESS,
        fromAmount: '2',
        price: '100000',
        refundAuthorizer: REFUND_ACCOUNT.address,
        idempotencyKey: IDEMPOTENCY_KEY
      })
    })

    expect(response.status).toBe(201)
    expect(calls[0]?.body).toMatchObject({
      input_amount: '200000000',
      output_amount: '200000000000'
    })
  })

  test('creates a limit order by deriving input from a readable price', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      refundAuthorizationService: testRefundAuthorizationService(
        new InMemoryRefundAuthorizationStore()
      ),
      fetch: mockFetch(calls, async (path) => {
        if (path === '/api/v1/quotes') {
          return Response.json(internalLimitQuote(), { status: 201 })
        }

        if (path === '/api/v1/orders') {
          return Response.json(internalLimitOrder(), { status: 201 })
        }

        return Response.json({ message: 'not found' }, { status: 404 })
      })
    })

    const response = await app.request('/order/limit', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        from: 'Bitcoin.BTC',
        to: 'Ethereum.USDC',
        fromAddress: FROM_ADDRESS,
        toAddress: TO_ADDRESS,
        toAmount: '200000',
        price: '100000',
        refundAuthorizer: REFUND_ACCOUNT.address,
        idempotencyKey: IDEMPOTENCY_KEY
      })
    })

    expect(response.status).toBe(201)
    expect(calls[0]?.body).toMatchObject({
      input_amount: '200000000',
      output_amount: '200000000000'
    })
  })

  test('rejects numeric limit order amounts before calling the router API', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      fetch: mockFetch(calls, async () =>
        Response.json({ message: 'unexpected' }, { status: 500 })
      )
    })

    const response = await app.request('/order/limit', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        from: 'Bitcoin.BTC',
        to: 'Ethereum.USDC',
        fromAddress: FROM_ADDRESS,
        toAddress: TO_ADDRESS,
        fromAmount: 1.25,
        toAmount: '100000',
        refundAuthorizer: REFUND_ACCOUNT.address,
        idempotencyKey: IDEMPOTENCY_KEY
      })
    })

    expect(response.status).toBe(400)
    expect(calls).toHaveLength(0)
  })

  test('stores cancellation secrets and returns a refund token in token mode', async () => {
    const calls: RecordedCall[] = []
    const store = new InMemoryRefundAuthorizationStore()
    const app = createApp(testConfig(), {
      refundAuthorizationService: testRefundAuthorizationService(store),
      fetch: mockFetch(calls, async (path) => {
        if (path === `/api/v1/quotes/${QUOTE_ID}`) {
          return Response.json(internalQuote())
        }

        if (path === '/api/v1/orders') {
          return Response.json(internalOrder(), { status: 201 })
        }

        if (path === `/api/v1/orders/${ORDER_ID}/cancellations`) {
          return Response.json(internalOrder({ status: 'refunding' }))
        }

        return Response.json({ message: 'not found' }, { status: 404 })
      })
    })

    const orderResponse = await app.request('/order/market', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        quoteId: QUOTE_ID,
        idempotencyKey: IDEMPOTENCY_KEY,
        fromAddress: FROM_ADDRESS,
        toAddress: TO_ADDRESS,
        refundMode: 'token',
        refundAuthorizer: null
      })
    })
    const orderBody = await orderResponse.json()

    expect(orderResponse.status).toBe(201)
    expect(orderBody.refundMode).toBe('token')
    expect(orderBody.refundAuthorizer).toBeNull()
    expect(orderBody.refundToken.startsWith('rgt_')).toBe(true)
    expect(orderBody.cancellationSecret).toBeUndefined()

    const stored = await store.findRefundAuthorization(ORDER_ID)
    expect(stored?.encryptedCancellationSecret.startsWith('v2:')).toBe(true)
    expect(stored?.encryptedCancellationSecret).not.toContain(
      '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
    )

    const cancelResponse = await app.request(`/order/${ORDER_ID}/cancel`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        refundToken: orderBody.refundToken
      })
    })

    expect(cancelResponse.status).toBe(200)
    const cancelBody = await cancelResponse.json()
    expect(cancelBody.cancellationSecret).toBeUndefined()
    expect(calls.at(-1)?.body).toEqual({
      cancellation_secret:
        '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
    })
    expect(
      (await store.findRefundAuthorization(ORDER_ID))?.cancellationRequestedAt
    ).toBeDefined()

    const callsAfterFirstCancellation = calls.length
    const replayResponse = await app.request(`/order/${ORDER_ID}/cancel`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        refundToken: orderBody.refundToken
      })
    })
    const replayBody = await replayResponse.json()

    expect(replayResponse.status).toBe(401)
    expect(replayBody.error.message).toBe('refund authorization already used')
    expect(calls).toHaveLength(callsAfterFirstCancellation)

    const callsBeforeCreateRetry = calls.length
    const createRetryResponse = await app.request('/order/market', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        quoteId: QUOTE_ID,
        idempotencyKey: IDEMPOTENCY_KEY,
        fromAddress: FROM_ADDRESS,
        toAddress: TO_ADDRESS,
        refundMode: 'token',
        refundAuthorizer: null
      })
    })
    const createRetryBody = await createRetryResponse.json()
    expect(createRetryResponse.status).toBe(409)
    expect(createRetryBody.error.message).toBe(
      'refund authorization already used or in progress'
    )
    expect(calls).toHaveLength(callsBeforeCreateRetry + 2)
    expect(calls.at(-2)?.path).toBe(`/api/v1/quotes/${QUOTE_ID}`)
    expect(calls.at(-1)?.path).toBe('/api/v1/orders')
  })

  test('returns the same refund token on create-order retries before cancellation', async () => {
    const calls: RecordedCall[] = []
    const store = new InMemoryRefundAuthorizationStore()
    const app = createApp(testConfig(), {
      refundAuthorizationService: testRefundAuthorizationService(store),
      fetch: mockFetch(calls, async (path) => {
        if (path === `/api/v1/quotes/${QUOTE_ID}`) {
          return Response.json(internalQuote())
        }

        if (path === '/api/v1/orders') {
          return Response.json(internalOrder(), { status: 201 })
        }

        if (path === `/api/v1/orders/${ORDER_ID}/cancellations`) {
          return Response.json(internalOrder({ status: 'refunding' }))
        }

        return Response.json({ message: 'not found' }, { status: 404 })
      })
    })

    const createBody = {
      quoteId: QUOTE_ID,
      idempotencyKey: IDEMPOTENCY_KEY,
      fromAddress: FROM_ADDRESS,
      toAddress: TO_ADDRESS,
      refundMode: 'token',
      refundAuthorizer: null
    }
    const firstResponse = await app.request('/order/market', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(createBody)
    })
    const firstBody = await firstResponse.json()
    expect(firstResponse.status).toBe(201)
    expect(firstBody.refundToken.startsWith('rgt_')).toBe(true)

    const retryResponse = await app.request('/order/market', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(createBody)
    })
    const retryBody = await retryResponse.json()
    expect(retryResponse.status).toBe(201)
    expect(retryBody.refundToken).toBe(firstBody.refundToken)
    expect(retryBody.orderId).toBe(ORDER_ID)

    const cancelResponse = await app.request(`/order/${ORDER_ID}/cancel`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        refundToken: retryBody.refundToken
      })
    })
    expect(cancelResponse.status).toBe(200)
    expect(
      (await store.findRefundAuthorization(ORDER_ID))?.cancellationRequestedAt
    ).toBeDefined()
    expect(calls.at(-1)?.path).toBe(`/api/v1/orders/${ORDER_ID}/cancellations`)
  })

  test('keeps evm-signature refund authorization idempotent on create-order retries before cancellation', async () => {
    const calls: RecordedCall[] = []
    const store = new InMemoryRefundAuthorizationStore()
    const app = createApp(testConfig(), {
      refundAuthorizationService: testRefundAuthorizationService(store),
      fetch: mockFetch(calls, async (path) => {
        if (path === `/api/v1/quotes/${QUOTE_ID}`) {
          return Response.json(internalQuote())
        }

        if (path === '/api/v1/orders') {
          return Response.json(internalOrder(), { status: 201 })
        }

        return Response.json({ message: 'not found' }, { status: 404 })
      })
    })

    const createBody = {
      quoteId: QUOTE_ID,
      idempotencyKey: IDEMPOTENCY_KEY,
      fromAddress: FROM_ADDRESS,
      toAddress: TO_ADDRESS,
      refundMode: 'evmSignature',
      refundAuthorizer: REFUND_ACCOUNT.address
    }
    const firstResponse = await app.request('/order/market', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(createBody)
    })
    const firstBody = await firstResponse.json()
    expect(firstResponse.status).toBe(201)
    expect(firstBody.refundAuthorizer).toBe(REFUND_ACCOUNT.address)
    expect(firstBody.refundToken).toBeUndefined()

    const retryResponse = await app.request('/order/market', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(createBody)
    })
    const retryBody = await retryResponse.json()

    expect(retryResponse.status).toBe(201)
    expect(retryBody.orderId).toBe(ORDER_ID)
    expect(retryBody.refundAuthorizer).toBe(REFUND_ACCOUNT.address)
    expect(retryBody.refundToken).toBeUndefined()
  })

  test('does not consume refund authorization when upstream cancellation fails', async () => {
    const calls: RecordedCall[] = []
    const store = new InMemoryRefundAuthorizationStore()
    const app = createApp(testConfig(), {
      refundAuthorizationService: testRefundAuthorizationService(store),
      fetch: mockFetch(calls, async (path) => {
        if (path === `/api/v1/quotes/${QUOTE_ID}`) {
          return Response.json(internalQuote())
        }

        if (path === '/api/v1/orders') {
          return Response.json(internalOrder(), { status: 201 })
        }

        if (path === `/api/v1/orders/${ORDER_ID}/cancellations`) {
          return Response.json({ message: 'router unavailable' }, { status: 500 })
        }

        return Response.json({ message: 'not found' }, { status: 404 })
      })
    })

    const orderResponse = await app.request('/order/market', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        quoteId: QUOTE_ID,
        idempotencyKey: IDEMPOTENCY_KEY,
        fromAddress: FROM_ADDRESS,
        toAddress: TO_ADDRESS,
        refundMode: 'token',
        refundAuthorizer: null
      })
    })
    const orderBody = await orderResponse.json()

    const cancelResponse = await app.request(`/order/${ORDER_ID}/cancel`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        refundToken: orderBody.refundToken
      })
    })

    expect(cancelResponse.status).toBe(502)
    expect(
      (await store.findRefundAuthorization(ORDER_ID))?.cancellationRequestedAt
    ).toBeUndefined()
  })

  test('classifies malformed upstream cancellation responses as upstream errors', async () => {
    const calls: RecordedCall[] = []
    const store = new InMemoryRefundAuthorizationStore()
    const app = createApp(testConfig(), {
      refundAuthorizationService: testRefundAuthorizationService(store),
      fetch: mockFetch(calls, async (path) => {
        if (path === `/api/v1/quotes/${QUOTE_ID}`) {
          return Response.json(internalQuote())
        }

        if (path === '/api/v1/orders') {
          return Response.json(internalOrder(), { status: 201 })
        }

        if (path === `/api/v1/orders/${ORDER_ID}/cancellations`) {
          const order = internalOrder({ status: 'refunding' })
          order.quote.payload.amount_out = '0'
          return Response.json(order)
        }

        return Response.json({ message: 'not found' }, { status: 404 })
      })
    })

    const orderResponse = await app.request('/order/market', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        quoteId: QUOTE_ID,
        idempotencyKey: IDEMPOTENCY_KEY,
        fromAddress: FROM_ADDRESS,
        toAddress: TO_ADDRESS,
        refundMode: 'token',
        refundAuthorizer: null
      })
    })
    const orderBody = await orderResponse.json()

    const cancelResponse = await app.request(`/order/${ORDER_ID}/cancel`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        refundToken: orderBody.refundToken
      })
    })
    const cancelBody = await cancelResponse.json()

    expect(cancelResponse.status).toBe(502)
    expect(cancelBody.error.code).toBe('UPSTREAM_ERROR')
    expect(cancelBody.error.message).toBe(
      'internal router API returned malformed order response'
    )
    expect(calls.at(-1)?.path).toBe(`/api/v1/orders/${ORDER_ID}/cancellations`)
    expect(
      (await store.findRefundAuthorization(ORDER_ID))?.cancellationRequestedAt
    ).toBeDefined()
  })

  test('rejects stale token claims if refund authorization changes before claim', async () => {
    const store = new InMemoryRefundAuthorizationStore()
    const service = new RefundAuthorizationService(
      {
        saveRefundAuthorization: (...args) => store.saveRefundAuthorization(...args),
        findRefundAuthorization: (...args) => store.findRefundAuthorization(...args),
        claimCancellationAttempt: async () => false,
        releaseCancellationAttempt: (...args) =>
          store.releaseCancellationAttempt(...args),
        markCancellationRequested: (...args) =>
          store.markCancellationRequested(...args)
      },
      new RouterCancellationSecretBox(Buffer.alloc(32, 7))
    )

    const authorization = await service.createRefundAuthorization({
      routerOrderId: ORDER_ID,
      cancellationSecret:
        '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
      refundMode: 'token',
      refundAuthorizer: null
    })

    await expect(
      service.resolveTokenCancellationSecret(
        ORDER_ID,
        authorization.refundToken as string
      )
    ).rejects.toThrow('refund authorization already used or in progress')
  })

  test('does not claim refund authorization when stored cancellation secret cannot decrypt', async () => {
    const refundToken = 'rgt_test-token'
    let claimCalled = false
    const service = new RefundAuthorizationService(
      {
        saveRefundAuthorization: async (input) => ({
          routerOrderId: input.routerOrderId,
          refundMode: input.refundMode,
          refundAuthorizer: input.refundAuthorizer,
          encryptedCancellationSecret: input.encryptedCancellationSecret,
          cancellationSecretHash: input.cancellationSecretHash,
          ...(input.refundTokenHash ? { refundTokenHash: input.refundTokenHash } : {}),
          ...(input.encryptedRefundToken
            ? { encryptedRefundToken: input.encryptedRefundToken }
            : {}),
          createdAt: '2026-05-04T00:00:00.000Z',
          updatedAt: '2026-05-04T00:00:00.000Z'
        }),
        findRefundAuthorization: async () => ({
          routerOrderId: ORDER_ID,
          refundMode: 'token',
          refundAuthorizer: null,
          encryptedCancellationSecret: 'v2:bad:bad:bad',
          cancellationSecretHash:
            'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
          refundTokenHash: hashRefundToken(refundToken),
          createdAt: '2026-05-04T00:00:00.000Z',
          updatedAt: '2026-05-04T00:00:00.000Z'
        }),
        claimCancellationAttempt: async () => {
          claimCalled = true
          return true
        },
        releaseCancellationAttempt: async () => undefined,
        markCancellationRequested: async () => false
      },
      new RouterCancellationSecretBox(Buffer.alloc(32, 7))
    )

    await expect(
      service.resolveTokenCancellationSecret(ORDER_ID, refundToken)
    ).rejects.toThrow('invalid encrypted cancellation secret iv')
    expect(claimCalled).toBe(false)
  })

  test('allows only one in-flight refund cancellation attempt per order', async () => {
    const calls: RecordedCall[] = []
    const store = new InMemoryRefundAuthorizationStore()
    const releaseCancellation = deferred<Response>()
    let cancellationCalls = 0
    const app = createApp(testConfig(), {
      refundAuthorizationService: testRefundAuthorizationService(store),
      fetch: mockFetch(calls, async (path) => {
        if (path === `/api/v1/quotes/${QUOTE_ID}`) {
          return Response.json(internalQuote())
        }

        if (path === '/api/v1/orders') {
          return Response.json(internalOrder(), { status: 201 })
        }

        if (path === `/api/v1/orders/${ORDER_ID}/cancellations`) {
          cancellationCalls += 1
          if (cancellationCalls > 1) {
            return Response.json(internalOrder({ status: 'refunding' }))
          }
          return releaseCancellation.promise
        }

        return Response.json({ message: 'not found' }, { status: 404 })
      })
    })

    const orderResponse = await app.request('/order/market', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        quoteId: QUOTE_ID,
        idempotencyKey: IDEMPOTENCY_KEY,
        fromAddress: FROM_ADDRESS,
        toAddress: TO_ADDRESS,
        refundMode: 'token',
        refundAuthorizer: null
      })
    })
    const orderBody = await orderResponse.json()

    const firstCancellation = app.request(`/order/${ORDER_ID}/cancel`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        refundToken: orderBody.refundToken
      })
    })
    await eventually(() => {
      expect(cancellationCalls).toBe(1)
    })

    const callsBeforeRetry = calls.length
    const retriedOrderResponse = await app.request('/order/market', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        quoteId: QUOTE_ID,
        idempotencyKey: IDEMPOTENCY_KEY,
        fromAddress: FROM_ADDRESS,
        toAddress: TO_ADDRESS,
        refundMode: 'token',
        refundAuthorizer: null
      })
    })
    const retriedOrderBody = await retriedOrderResponse.json()
    expect(retriedOrderResponse.status).toBe(409)
    expect(retriedOrderBody.error.message).toBe(
      'refund authorization already used or in progress'
    )
    expect(calls).toHaveLength(callsBeforeRetry + 2)
    expect(calls.at(-2)?.path).toBe(`/api/v1/quotes/${QUOTE_ID}`)
    expect(calls.at(-1)?.path).toBe('/api/v1/orders')

    const secondCancellation = await app.request(`/order/${ORDER_ID}/cancel`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        refundToken: orderBody.refundToken
      })
    })
    const secondBody = await secondCancellation.json()

    expect(secondCancellation.status).toBe(401)
    expect(secondBody.error.message).toBe(
      'refund authorization already used or in progress'
    )
    expect(cancellationCalls).toBe(1)

    releaseCancellation.resolve(Response.json(internalOrder({ status: 'refunding' })))
    expect((await firstCancellation).status).toBe(200)
    expect(
      (await store.findRefundAuthorization(ORDER_ID))?.cancellationRequestedAt
    ).toBeDefined()
  })

  test('cancels token-authorized limit orders with the limit response shape', async () => {
    const calls: RecordedCall[] = []
    const store = new InMemoryRefundAuthorizationStore()
    const app = createApp(testConfig(), {
      refundAuthorizationService: testRefundAuthorizationService(store),
      fetch: mockFetch(calls, async (path) => {
        if (path === '/api/v1/quotes') {
          return Response.json(internalLimitQuote(), { status: 201 })
        }

        if (path === '/api/v1/orders') {
          return Response.json(internalLimitOrder(), { status: 201 })
        }

        if (path === `/api/v1/orders/${ORDER_ID}/cancellations`) {
          return Response.json(internalLimitOrder({ status: 'refunding' }))
        }

        return Response.json({ message: 'not found' }, { status: 404 })
      })
    })

    const orderResponse = await app.request('/order/limit', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        from: 'Bitcoin.BTC',
        to: 'Ethereum.USDC',
        fromAddress: FROM_ADDRESS,
        toAddress: TO_ADDRESS,
        fromAmount: '1.25',
        toAmount: '100000',
        refundMode: 'token',
        refundAuthorizer: null,
        idempotencyKey: IDEMPOTENCY_KEY
      })
    })
    const orderBody = await orderResponse.json()

    const cancelResponse = await app.request(`/order/${ORDER_ID}/cancel`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        refundToken: orderBody.refundToken
      })
    })
    const cancelBody = await cancelResponse.json()

    expect(cancelResponse.status).toBe(200)
    expect(cancelBody).toMatchObject({
      orderId: ORDER_ID,
      status: 'refunding',
      quoteId: LIMIT_QUOTE_ID,
      orderType: 'limit_order',
      expectedOut: '100000',
      maxIn: '1.25',
      maxSlippage: '0'
    })
  })

  test('verifies EIP-712 refund signatures before forwarding cancellation', async () => {
    const calls: RecordedCall[] = []
    const store = new InMemoryRefundAuthorizationStore()
    const app = createApp(testConfig(), {
      refundAuthorizationService: testRefundAuthorizationService(store),
      fetch: mockFetch(calls, async (path) => {
        if (path === `/api/v1/quotes/${QUOTE_ID}`) {
          return Response.json(internalQuote())
        }

        if (path === '/api/v1/orders') {
          return Response.json(internalOrder(), { status: 201 })
        }

        if (path === `/api/v1/orders/${ORDER_ID}/cancellations`) {
          return Response.json(internalOrder({ status: 'refunding' }))
        }

        return Response.json({ message: 'not found' }, { status: 404 })
      })
    })

    const orderResponse = await app.request('/order/market', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        quoteId: QUOTE_ID,
        idempotencyKey: IDEMPOTENCY_KEY,
        fromAddress: FROM_ADDRESS,
        toAddress: TO_ADDRESS,
        refundMode: 'evmSignature',
        refundAuthorizer: REFUND_ACCOUNT.address
      })
    })
    expect(orderResponse.status).toBe(201)

    const refundSignatureDeadline = Math.floor(Date.now() / 1000) + 600
    const refundSignature = await REFUND_ACCOUNT.signTypedData({
      domain: REFUND_EIP712_DOMAIN,
      types: REFUND_EIP712_TYPES,
      primaryType: 'CancelOrder',
      message: {
        orderId: ORDER_ID,
        gateway: DEFAULT_REFUND_SIGNATURE_GATEWAY,
        deadline: BigInt(refundSignatureDeadline)
      }
    })

    const response = await app.request(`/order/${ORDER_ID}/cancel`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        refundSignature,
        refundSignatureDeadline
      })
    })

    expect(response.status).toBe(200)
    expect(calls.at(-1)?.method).toBe('POST')
    expect(calls.at(-1)?.path).toBe(`/api/v1/orders/${ORDER_ID}/cancellations`)
    expect(calls.at(-1)?.body).toEqual({
      cancellation_secret:
        '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
    })

    const body = await response.json()
    expect(body.status).toBe('refunding')
  })

  test('rejects EIP-712 refund signatures with excessive future deadlines', async () => {
    const calls: RecordedCall[] = []
    const store = new InMemoryRefundAuthorizationStore()
    const app = createApp(testConfig(), {
      refundAuthorizationService: testRefundAuthorizationService(store),
      fetch: mockFetch(calls, async (path) => {
        if (path === `/api/v1/quotes/${QUOTE_ID}`) {
          return Response.json(internalQuote())
        }

        if (path === '/api/v1/orders') {
          return Response.json(internalOrder(), { status: 201 })
        }

        if (path === `/api/v1/orders/${ORDER_ID}/cancellations`) {
          return Response.json(internalOrder({ status: 'refunding' }))
        }

        return Response.json({ message: 'not found' }, { status: 404 })
      })
    })

    const orderResponse = await app.request('/order/market', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        quoteId: QUOTE_ID,
        idempotencyKey: IDEMPOTENCY_KEY,
        fromAddress: FROM_ADDRESS,
        toAddress: TO_ADDRESS,
        refundMode: 'evmSignature',
        refundAuthorizer: REFUND_ACCOUNT.address
      })
    })
    expect(orderResponse.status).toBe(201)

    const callsBeforeCancel = calls.length
    const refundSignatureDeadline =
      Math.floor(Date.now() / 1000) + MAX_REFUND_SIGNATURE_DEADLINE_SECONDS + 1
    const refundSignature = await REFUND_ACCOUNT.signTypedData({
      domain: REFUND_EIP712_DOMAIN,
      types: REFUND_EIP712_TYPES,
      primaryType: 'CancelOrder',
      message: {
        orderId: ORDER_ID,
        gateway: DEFAULT_REFUND_SIGNATURE_GATEWAY,
        deadline: BigInt(refundSignatureDeadline)
      }
    })

    const response = await app.request(`/order/${ORDER_ID}/cancel`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        refundSignature,
        refundSignatureDeadline
      })
    })
    const body = await response.json()

    expect(response.status).toBe(401)
    expect(body.error.message).toBe(
      `refund signature deadline must be within ${MAX_REFUND_SIGNATURE_DEADLINE_SECONDS} seconds`
    )
    expect(calls).toHaveLength(callsBeforeCancel)
  })

  test('rejects EIP-712 refund signatures scoped to a different gateway', async () => {
    const calls: RecordedCall[] = []
    const store = new InMemoryRefundAuthorizationStore()
    const app = createApp(testConfig(), {
      refundAuthorizationService: testRefundAuthorizationService(store, {
        publicBaseUrl: 'https://gateway.example.com'
      }),
      fetch: mockFetch(calls, async (path) => {
        if (path === `/api/v1/quotes/${QUOTE_ID}`) {
          return Response.json(internalQuote())
        }

        if (path === '/api/v1/orders') {
          return Response.json(internalOrder(), { status: 201 })
        }

        if (path === `/api/v1/orders/${ORDER_ID}/cancellations`) {
          return Response.json(internalOrder({ status: 'refunding' }))
        }

        return Response.json({ message: 'not found' }, { status: 404 })
      })
    })

    const orderResponse = await app.request('/order/market', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        quoteId: QUOTE_ID,
        idempotencyKey: IDEMPOTENCY_KEY,
        fromAddress: FROM_ADDRESS,
        toAddress: TO_ADDRESS,
        refundMode: 'evmSignature',
        refundAuthorizer: REFUND_ACCOUNT.address
      })
    })
    expect(orderResponse.status).toBe(201)

    const callsBeforeCancel = calls.length
    const refundSignatureDeadline = Math.floor(Date.now() / 1000) + 600
    const refundSignature = await REFUND_ACCOUNT.signTypedData({
      domain: refundEip712Domain('https://other-gateway.example.com'),
      types: REFUND_EIP712_TYPES,
      primaryType: 'CancelOrder',
      message: {
        orderId: ORDER_ID,
        gateway: 'https://other-gateway.example.com',
        deadline: BigInt(refundSignatureDeadline)
      }
    })

    const response = await app.request(`/order/${ORDER_ID}/cancel`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        refundSignature,
        refundSignatureDeadline
      })
    })
    const body = await response.json()

    expect(response.status).toBe(401)
    expect(body.error.message).toBe('invalid refund signature')
    expect(calls).toHaveLength(callsBeforeCancel)
  })

  test('rejects credentialed gateway URLs for refund signature domains', () => {
    expect(() => refundEip712Domain('https://user:pass@gateway.example.com')).toThrow(
      'refund signature gateway URL must be an absolute HTTP(S) URL without credentials, query string, or fragment'
    )
  })

  test('rejects query strings and fragments in gateway URLs for refund signature domains', () => {
    expect(() =>
      refundEip712Domain('https://gateway.example.com?token=secret')
    ).toThrow(
      'refund signature gateway URL must be an absolute HTTP(S) URL without credentials, query string, or fragment'
    )
    expect(() => refundEip712Domain('https://gateway.example.com#tenant')).toThrow(
      'refund signature gateway URL must be an absolute HTTP(S) URL without credentials, query string, or fragment'
    )
  })
})

type RecordedCall = {
  method: string
  path: string
  body?: unknown
}

class InMemoryRefundAuthorizationStore implements RefundAuthorizationStore {
  private readonly records = new Map<string, StoredRefundAuthorization>()
  private readonly claims = new Map<string, { claimId: string; claimedAt: number }>()

  async saveRefundAuthorization(
    input: SaveRefundAuthorizationInput,
    claimTimeoutMs: number
  ): Promise<StoredRefundAuthorization | undefined> {
    const existing = this.records.get(input.routerOrderId)
    const nowMs = Date.now()
    if (existing) {
      if (existing.cancellationRequestedAt) return undefined

      const existingClaim = this.claims.get(input.routerOrderId)
      if (existingClaim && nowMs - existingClaim.claimedAt < claimTimeoutMs) {
        return undefined
      }

      if (!authorizationMatches(existing, input)) return undefined

      this.claims.delete(input.routerOrderId)
      const next = {
        ...existing,
        updatedAt: new Date().toISOString()
      }
      this.records.set(input.routerOrderId, next)
      return next
    }

    const now = new Date().toISOString()
    const record = {
      routerOrderId: input.routerOrderId,
      refundMode: input.refundMode,
      refundAuthorizer: input.refundAuthorizer,
      encryptedCancellationSecret: input.encryptedCancellationSecret,
      cancellationSecretHash: input.cancellationSecretHash,
      ...(input.refundTokenHash ? { refundTokenHash: input.refundTokenHash } : {}),
      ...(input.encryptedRefundToken
        ? { encryptedRefundToken: input.encryptedRefundToken }
        : {}),
      createdAt: now,
      updatedAt: now
    }
    this.records.set(input.routerOrderId, record)
    return record
  }

  async findRefundAuthorization(
    routerOrderId: string
  ): Promise<StoredRefundAuthorization | undefined> {
    return this.records.get(routerOrderId)
  }

  async claimCancellationAttempt(
    routerOrderId: string,
    claimId: string,
    claimTimeoutMs: number,
    expected: ClaimRefundAuthorizationExpectation
  ): Promise<boolean> {
    const record = this.records.get(routerOrderId)
    if (!record || record.cancellationRequestedAt) return false
    if (!authorizationMatches(record, expected)) return false

    const existing = this.claims.get(routerOrderId)
    const now = Date.now()
    if (existing && now - existing.claimedAt < claimTimeoutMs) return false

    this.claims.set(routerOrderId, { claimId, claimedAt: now })
    return true
  }

  async releaseCancellationAttempt(
    routerOrderId: string,
    claimId: string
  ): Promise<void> {
    if (this.claims.get(routerOrderId)?.claimId === claimId) {
      this.claims.delete(routerOrderId)
    }
  }

  async markCancellationRequested(
    routerOrderId: string,
    claimId: string
  ): Promise<boolean> {
    const record = this.records.get(routerOrderId)
    if (!record) return false
    if (this.claims.get(routerOrderId)?.claimId !== claimId) return false

    this.records.set(routerOrderId, {
      ...record,
      cancellationRequestedAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    })
    this.claims.delete(routerOrderId)
    return true
  }
}

function authorizationMatches(
  record: StoredRefundAuthorization,
  expected: ClaimRefundAuthorizationExpectation
) {
  return (
    record.routerOrderId === expected.routerOrderId &&
    record.refundMode === expected.refundMode &&
    record.refundAuthorizer === expected.refundAuthorizer &&
    record.cancellationSecretHash === expected.cancellationSecretHash
  )
}

function testRefundAuthorizationService(
  store: RefundAuthorizationStore,
  options: { publicBaseUrl?: string } = {}
): RefundAuthorizationService {
  return new RefundAuthorizationService(
    store,
    new RouterCancellationSecretBox(Buffer.alloc(32, 7)),
    options
  )
}

function failingRefundAuthorizationStore(): RefundAuthorizationStore {
  return {
    saveRefundAuthorization: async () => {
      throw new Error('refund authorization database unavailable')
    },
    findRefundAuthorization: async () => undefined,
    claimCancellationAttempt: async () => false,
    releaseCancellationAttempt: async () => undefined,
    markCancellationRequested: async () => false
  }
}

function testConfig(): GatewayConfig {
  return {
    host: '0.0.0.0',
    port: 3000,
    routerInternalBaseUrl: 'http://router.internal',
    routerInternalApiKey: ROUTER_GATEWAY_API_KEY,
    requestTimeoutMs: 1_000,
    healthTargets: [],
    healthPollIntervalMs: 30_000,
    healthTargetTimeoutMs: 1_000,
    bitcoinAddressNetworks: ['mainnet', 'testnet', 'regtest'],
    version: 'test'
  }
}

function mockFetch(
  calls: RecordedCall[],
  handler: (path: string, request: Request) => Promise<Response>
): FetchLike {
  return async (input, init) => {
    const request = new Request(input, init)
    const url = new URL(request.url)
    const text = await request.text()
    calls.push({
      method: request.method,
      path: url.pathname,
      ...(text ? { body: JSON.parse(text) } : {})
    })

    return handler(url.pathname, request)
  }
}

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void
  let reject!: (reason?: unknown) => void
  const promise = new Promise<T>((promiseResolve, promiseReject) => {
    resolve = promiseResolve
    reject = promiseReject
  })
  return { promise, resolve, reject }
}

async function eventually(assertion: () => void, timeoutMs = 1_000) {
  const deadline = Date.now() + timeoutMs
  let lastError: unknown
  while (Date.now() < deadline) {
    try {
      assertion()
      return
    } catch (error) {
      lastError = error
      await new Promise((resolve) => setTimeout(resolve, 10))
    }
  }
  if (lastError) throw lastError
  assertion()
}

function internalQuote() {
  return {
    quote: {
      type: 'market_order',
      payload: {
        id: QUOTE_ID,
        order_id: null,
        source_asset: {
          chain: 'bitcoin',
          asset: 'native'
        },
        destination_asset: {
          chain: 'evm:1',
          asset: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
        },
        recipient_address: TO_ADDRESS,
        provider_id: 'unit',
        order_kind: 'exact_in',
        amount_in: '125000000',
        amount_out: '100000000',
        min_amount_out: '99000000',
        max_amount_in: null,
        slippage_bps: 150,
        provider_quote: {},
        expires_at: '2026-04-30T12:00:00Z',
        created_at: '2026-04-30T11:59:00Z'
      }
    }
  }
}

function internalLimitQuote() {
  return {
    quote: {
      type: 'limit_order',
      payload: {
        id: LIMIT_QUOTE_ID,
        order_id: null,
        source_asset: {
          chain: 'bitcoin',
          asset: 'native'
        },
        destination_asset: {
          chain: 'evm:1',
          asset: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
        },
        recipient_address: TO_ADDRESS,
        provider_id: 'hyperliquid',
        input_amount: '125000000',
        output_amount: '100000000000',
        residual_policy: 'refund',
        provider_quote: {},
        expires_at: '2026-04-30T12:00:00Z',
        created_at: '2026-04-30T11:59:00Z'
      }
    }
  }
}

function internalBaseToBitcoinLimitQuote() {
  const quote = internalLimitQuote()
  quote.quote.payload.id = QUOTE_ID
  quote.quote.payload.source_asset = {
    chain: 'evm:8453',
    asset: '0x833589fcd6edb6e08f4c7c32d4f71b54bda02913'
  }
  quote.quote.payload.destination_asset = {
    chain: 'bitcoin',
    asset: 'native'
  }
  quote.quote.payload.recipient_address = BTC_ADDRESS
  quote.quote.payload.input_amount = '101219825'
  quote.quote.payload.output_amount = '100000'
  return quote
}

function internalOrder(overrides: { status?: string } = {}) {
  return {
    order: {
      id: ORDER_ID,
      status: overrides.status ?? 'quoted',
      source_asset: {
        chain: 'bitcoin',
        asset: 'native'
      },
      destination_asset: {
        chain: 'evm:1',
        asset: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
      },
      recipient_address: TO_ADDRESS,
      refund_address: FROM_ADDRESS
    },
    quote: internalQuote().quote,
    funding_vault: {
      vault: {
        deposit_vault_address: 'bc1qorderaddress0000000000000000000000000'
      }
    },
    cancellation_secret:
      '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
  }
}

function internalLimitOrder(overrides: { status?: string } = {}) {
  return {
    order: {
      id: ORDER_ID,
      status: overrides.status ?? 'quoted',
      source_asset: {
        chain: 'bitcoin',
        asset: 'native'
      },
      destination_asset: {
        chain: 'evm:1',
        asset: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
      },
      recipient_address: TO_ADDRESS,
      refund_address: FROM_ADDRESS
    },
    quote: internalLimitQuote().quote,
    funding_vault: {
      vault: {
        deposit_vault_address: 'bc1qorderaddress0000000000000000000000000'
      }
    },
    cancellation_secret:
      '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
  }
}
