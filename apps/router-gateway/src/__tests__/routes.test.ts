import { describe, expect, test } from 'bun:test'

import { MAX_GATEWAY_JSON_BODY_BYTES, createApp } from '../app'
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
    expect(body.paths['/order/limit']).toBeUndefined()
    expect(body.paths['/order/{orderId}']).toBeDefined()
    expect(body.paths['/order/{orderId}/cancel']).toBeUndefined()
    expect(Object.keys(body.paths)).toEqual([
      '/quote',
      '/order/market',
      '/order/{orderId}',
      '/health',
      '/providers'
    ])
    expect(Object.keys(body.components.schemas.QuoteRequest.properties)).toEqual([
      'from',
      'to',
      'amountFormat',
      'toAddress',
      'orderType',
      'fromAmount'
    ])
    expect(body.components.schemas.QuoteRequest.properties.orderType.enum).toEqual([
      'market_order',
      'market'
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
      'amountFormat'
    ])
    expect(body.components.schemas.OrderLimitRequest).toBeUndefined()
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
    expect(body.error.message).toBe('fromAmount is required')
    expect(body.error.details).toEqual({
      target: 'json',
      issues: [
        {
          path: 'fromAmount',
          message: 'fromAmount is required'
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
        quote.quote.payload.estimated_amount_out = '1'.repeat(97)
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
        amountFormat: 'readable'
      })
    })
    const body = await response.json()

    expect(response.status).toBe(201)
    expect(body.quoteId).toBe(QUOTE_ID)
  })

  test('rejects limit quote shorthand before calling the router API', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      fetch: mockFetch(calls, async () =>
        Response.json({ message: 'unexpected' }, { status: 500 })
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
        toAmount: '1000',
        orderType: 'limit',
        amountFormat: 'readable'
      })
    })

    expect(response.status).toBe(400)
    expect(calls).toHaveLength(0)
  })

  test('rejects zero upstream quote amounts as controlled upstream errors', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      fetch: mockFetch(calls, async () => {
        const quote = internalQuote()
        quote.quote.payload.estimated_amount_out = '0'
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
        amountFormat: 'readable'
      })
    })

    expect(response.status).toBe(400)
    expect(calls).toHaveLength(0)
  })

  test('rejects unknown market-order fields before routing upstream', async () => {
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
        toAddress: TO_ADDRESS
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
        toAddress: TO_ADDRESS
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
        toAddress: TO_ADDRESS
      })
    })
    const body = await response.json()

    expect(response.status).toBe(400)
    expect(body.error.message).toBe(
      'idempotencyKey must contain only letters, numbers, dot, underscore, colon, or hyphen'
    )
    expect(calls).toHaveLength(0)
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
        amountFormat: 'raw'
      })
    })
    const body = await response.json()

    expect(response.status).toBe(400)
    expect(body.error.message).toBe('toAddress must be a valid Bitcoin address')
    expect(calls).toHaveLength(0)
  })

  test('creates exact-in market quotes', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      fetch: mockFetch(calls, async () => {
        const quote = internalQuote()
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
      amount_in: '100000000'
    })
    expect(calls[0]?.body).not.toHaveProperty('kind')
  })

  test('rejects invalid EVM order refund addresses before creating orders', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      fetch: mockFetch(calls, async (path) => {
        if (path === `/api/v1/quotes/${QUOTE_ID}`) {
          return Response.json(internalBaseToBitcoinQuote())
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
          return Response.json(internalBaseToBitcoinQuote())
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
        amountFormat: 'readable'
      })
    })
    const body = await response.json()

    expect(response.status).toBe(502)
    expect(body.error.message).toBe('upstream quote failed')
    expect(body.error.details).toEqual({ upstreamStatus: 500 })
  })
  test('passes through actionable upstream 422 messages', async () => {
    const app = createApp(testConfig(), {
      fetch: mockFetch([], async () =>
        Response.json(
          {
            error: {
              code: 422,
              message: 'balance 0 below required 40125000000000'
            }
          },
          { status: 422 }
        )
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
        amountFormat: 'readable'
      })
    })
    const body = await response.json()

    expect(response.status).toBe(422)
    expect(body.error.code).toBe('UPSTREAM_ERROR')
    expect(body.error.message).toBe(
      'balance 0 below required 40125000000000'
    )
    expect(body.error.details).toEqual({ upstreamStatus: 422 })
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
      amount_in: '125000000',
    })

    const body = await response.json()
    expect(body).toMatchObject({
      quoteId: QUOTE_ID,
      from: 'Bitcoin.BTC',
      to: 'Ethereum.USDC',
      estimatedOut: '100',
      amountFormat: 'readable'
    })
  })

  test('sends the configured gateway bearer key to every internal router API request', async () => {
    const calls: RecordedCall[] = []
    const authorizations = new Map<string, string | null>()
    const app = createApp(testConfig(), {
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

        if (path === `/api/v1/orders/${ORDER_ID}`) {
          return Response.json(internalOrder())
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
        amountFormat: 'readable'
      })
    })

    await app.request('/order/market', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        quoteId: QUOTE_ID,
        idempotencyKey: IDEMPOTENCY_KEY,
        fromAddress: FROM_ADDRESS,
        toAddress: TO_ADDRESS
      })
    })

    await app.request(`/order/${ORDER_ID}`)

    const expectedAuthorization = `Bearer ${ROUTER_GATEWAY_API_KEY}`
    expect(authorizations.get('POST /api/v1/quotes')).toBe(expectedAuthorization)
    expect(authorizations.get(`GET /api/v1/quotes/${QUOTE_ID}`)).toBe(
      expectedAuthorization
    )
    expect(authorizations.get('POST /api/v1/orders')).toBe(expectedAuthorization)
    expect(authorizations.get(`GET /api/v1/orders/${ORDER_ID}`)).toBe(
      expectedAuthorization
    )
  })

  test('rejects raw limit quote requests before calling the router API', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      fetch: mockFetch(calls, async () =>
        Response.json({ message: 'unexpected' }, { status: 500 })
      )
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

    expect(response.status).toBe(400)
    expect(calls).toHaveLength(0)
  })

  test('creates a market order and defaults refundAddress to fromAddress', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
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
      quoteId: QUOTE_ID
    })
  })

  test('forwards an explicit order refundAddress to the internal router API', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
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
        refundAddress: BTC_ADDRESS
      })
    })

    expect(response.status).toBe(201)
    expect(calls[1]?.body).toMatchObject({
      refund_address: BTC_ADDRESS
    })
  })

  test('reports malformed created order responses as upstream errors', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      fetch: mockFetch(calls, async (path) => {
        if (path === `/api/v1/quotes/${QUOTE_ID}`) {
          return Response.json(internalQuote())
        }

        if (path === '/api/v1/orders') {
          const order = internalOrder()
          order.quote.payload.amount_in = 'not-a-raw-amount'
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
        toAddress: TO_ADDRESS
      })
    })
    const body = await response.json()

    expect(response.status).toBe(502)
    expect(body.error.message).toBe(
      'internal router API returned malformed order response'
    )
  })

  test('fetches an order by id with the market response shape', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      fetch: mockFetch(calls, async (path) => {
        if (path === `/api/v1/orders/${ORDER_ID}`) {
          return Response.json(internalOrder({ status: 'refunding' }))
        }

        return Response.json({ message: 'not found' }, { status: 404 })
      })
    })

    const response = await app.request(`/order/${ORDER_ID}`)
    const body = await response.json()

    expect(response.status).toBe(200)
    expect(calls[0]?.method).toBe('GET')
    expect(calls[0]?.path).toBe(`/api/v1/orders/${ORDER_ID}`)
    expect(body).toMatchObject({
      orderId: ORDER_ID,
      status: 'refund_pending',
      quoteId: QUOTE_ID,
      orderType: 'market_order'
    })
  })

  test('fetches a limit order by id with the limit response shape', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      fetch: mockFetch(calls, async (path) => {
        if (path === `/api/v1/orders/${ORDER_ID}`) {
          return Response.json(internalLimitOrder({ status: 'refunding' }))
        }

        return Response.json({ message: 'not found' }, { status: 404 })
      })
    })

    const response = await app.request(`/order/${ORDER_ID}`)
    const body = await response.json()

    expect(response.status).toBe(200)
    expect(body).toMatchObject({
      orderId: ORDER_ID,
      status: 'refund_pending',
      quoteId: LIMIT_QUOTE_ID,
      from: 'Bitcoin.BTC',
      to: 'Ethereum.USDC',
      outputAmount: '100000'
    })
  })

  test('rejects creating an order from a limit quote', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
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
        integrator: 'limit-loadgen',
        amountFormat: 'raw'
      })
    })

    const body = await response.json()
    expect(response.status).toBe(400)
    expect(body.error.message).toBe('limit orders are currently disabled')
    expect(calls.map((call) => call.path)).toEqual([
      `/api/v1/quotes/${LIMIT_QUOTE_ID}`
    ])
  })

  test('does not register the limit order endpoint', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
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
        integrator: 'partner-a',
        idempotencyKey: IDEMPOTENCY_KEY
      })
    })

    expect(response.status).toBe(404)
    expect(calls).toHaveLength(0)
  })
})

type RecordedCall = {
  method: string
  path: string
  body?: unknown
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
        amount_in: '125000000',
        estimated_amount_out: '100000000',
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

function internalBaseToBitcoinQuote() {
  const quote = internalQuote()
  quote.quote.payload.source_asset = {
    chain: 'evm:8453',
    asset: '0x833589fcd6edb6e08f4c7c32d4f71b54bda02913'
  }
  quote.quote.payload.destination_asset = {
    chain: 'bitcoin',
    asset: 'native'
  }
  quote.quote.payload.recipient_address = BTC_ADDRESS
  quote.quote.payload.amount_in = '101219825'
  quote.quote.payload.estimated_amount_out = '100000'
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
    }
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
    }
  }
}
