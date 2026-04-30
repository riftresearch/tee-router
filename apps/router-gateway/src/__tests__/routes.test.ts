import { describe, expect, test } from 'bun:test'
import { privateKeyToAccount } from 'viem/accounts'

import { createApp } from '../app'
import { RouterCancellationSecretBox } from '../cancellations/crypto'
import {
  REFUND_EIP712_DOMAIN,
  REFUND_EIP712_TYPES,
  RefundAuthorizationService
} from '../cancellations/service'
import type {
  RefundAuthorizationStore,
  SaveRefundAuthorizationInput,
  StoredRefundAuthorization
} from '../cancellations/store'
import type { GatewayConfig } from '../config'
import { createDependencyHealthMonitor } from '../health'
import type { FetchLike } from '../internal/router-client'

const QUOTE_ID = '00000000-0000-4000-8000-000000000001'
const ORDER_ID = '00000000-0000-4000-8000-000000000002'
const TO_ADDRESS = '0x1111111111111111111111111111111111111111'
const FROM_ADDRESS = 'bc1qexample000000000000000000000000000000'
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
    expect(body.paths['/health/dependencies']).toBeDefined()
    expect(body.paths['/status']).toBeUndefined()
    expect(body.paths['/quote']).toBeDefined()
    expect(body.paths['/order/market']).toBeDefined()
    expect(body.paths['/order/{orderId}/cancel']).toBeDefined()
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
    const response = await app.request('/health/dependencies')
    const body = await response.json()

    expect(response.status).toBe(200)
    expect(body.status).toBe('ok')
    expect(body.dependencies).toHaveLength(1)
    expect(body.dependencies[0]).toMatchObject({
      name: 'hyperliquid',
      status: 'ok',
      httpStatus: 200
    })
    expect(body.dependencies[0].url).toBeUndefined()
    expect(calls[0]?.path).toBe('/api/v1/provider-health')
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
    expect(stored?.encryptedCancellationSecret.startsWith('v1:')).toBe(true)
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
    expect(calls.at(-1)?.body).toEqual({
      cancellation_secret:
        '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
    })
    expect(
      (await store.findRefundAuthorization(ORDER_ID))?.cancellationRequestedAt
    ).toBeDefined()
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
})

type RecordedCall = {
  method: string
  path: string
  body?: unknown
}

class InMemoryRefundAuthorizationStore implements RefundAuthorizationStore {
  private readonly records = new Map<string, StoredRefundAuthorization>()

  async saveRefundAuthorization(input: SaveRefundAuthorizationInput): Promise<void> {
    const now = new Date().toISOString()
    this.records.set(input.routerOrderId, {
      routerOrderId: input.routerOrderId,
      refundMode: input.refundMode,
      refundAuthorizer: input.refundAuthorizer,
      encryptedCancellationSecret: input.encryptedCancellationSecret,
      ...(input.refundTokenHash ? { refundTokenHash: input.refundTokenHash } : {}),
      createdAt: now,
      updatedAt: now
    })
  }

  async findRefundAuthorization(
    routerOrderId: string
  ): Promise<StoredRefundAuthorization | undefined> {
    return this.records.get(routerOrderId)
  }

  async markCancellationRequested(routerOrderId: string): Promise<void> {
    const record = this.records.get(routerOrderId)
    if (!record) return

    this.records.set(routerOrderId, {
      ...record,
      cancellationRequestedAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    })
  }
}

function testRefundAuthorizationService(
  store: RefundAuthorizationStore
): RefundAuthorizationService {
  return new RefundAuthorizationService(
    store,
    new RouterCancellationSecretBox(Buffer.alloc(32, 7))
  )
}

function testConfig(): GatewayConfig {
  return {
    host: '0.0.0.0',
    port: 3000,
    routerInternalBaseUrl: 'http://router.internal',
    requestTimeoutMs: 1_000,
    healthTargets: [],
    healthPollIntervalMs: 30_000,
    healthTargetTimeoutMs: 1_000,
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
