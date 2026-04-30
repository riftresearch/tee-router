import { describe, expect, test } from 'bun:test'

import { createApp } from '../app'
import { CancellationSecretBox } from '../cancellations/crypto'
import { CancellationService } from '../cancellations/service'
import type {
  CancellationStore,
  SaveManagedCancellationInput,
  StoredManagedCancellation
} from '../cancellations/store'
import type { GatewayConfig } from '../config'
import type { FetchLike } from '../internal/router-client'

const QUOTE_ID = '00000000-0000-4000-8000-000000000001'
const ORDER_ID = '00000000-0000-4000-8000-000000000002'
const TO_ADDRESS = '0x1111111111111111111111111111111111111111'
const FROM_ADDRESS = 'bc1qexample000000000000000000000000000000'

describe('router gateway routes', () => {
  test('serves OpenAPI 3.1 from the live route registry', async () => {
    const app = createApp(testConfig())
    const response = await app.request('/openapi.json')
    const body = await response.json()

    expect(response.status).toBe(200)
    expect(body.openapi).toBe('3.1.0')
    expect(body.paths['/quote']).toBeDefined()
    expect(body.paths['/order/market']).toBeDefined()
    expect(body.paths['/order/{orderId}/cancel']).toBeDefined()
  })

  test('serves fully permissive CORS headers', async () => {
    const app = createApp(testConfig())
    const response = await app.request('/status', {
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
      cancellationSecret: '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
    })
  })

  test('stores cancellation secrets and returns a gateway token in managed mode', async () => {
    const calls: RecordedCall[] = []
    const store = new InMemoryCancellationStore()
    const app = createApp(testConfig(), {
      cancellationService: new CancellationService(
        store,
        new CancellationSecretBox(Buffer.alloc(32, 7))
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

    const orderResponse = await app.request('/order/market', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        quoteId: QUOTE_ID,
        fromAddress: FROM_ADDRESS,
        toAddress: TO_ADDRESS,
        cancellation: {
          mode: 'gateway_managed_token'
        }
      })
    })
    const orderBody = await orderResponse.json()

    expect(orderResponse.status).toBe(201)
    expect(orderBody.cancellationMode).toBe('gateway_managed_token')
    expect(orderBody.cancellationToken.startsWith('rgt_')).toBe(true)
    expect(orderBody.cancellationSecret).toBeUndefined()

    const stored = await store.findManagedCancellation(ORDER_ID)
    expect(stored?.encryptedCancellationSecret.startsWith('v1:')).toBe(true)
    expect(stored?.encryptedCancellationSecret).not.toContain(
      '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
    )

    const cancelResponse = await app.request(`/order/${ORDER_ID}/cancel`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        cancellationToken: orderBody.cancellationToken
      })
    })

    expect(cancelResponse.status).toBe(200)
    expect(calls.at(-1)?.body).toEqual({
      cancellation_secret:
        '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
    })
    expect((await store.findManagedCancellation(ORDER_ID))?.cancellationRequestedAt).toBeDefined()
  })

  test('forwards direct cancellation secrets to the internal router API', async () => {
    const calls: RecordedCall[] = []
    const app = createApp(testConfig(), {
      fetch: mockFetch(calls, async (path) => {
        if (path === `/api/v1/orders/${ORDER_ID}/cancellations`) {
          return Response.json(internalOrder({ status: 'refunding' }))
        }

        return Response.json({ message: 'not found' }, { status: 404 })
      })
    })

    const response = await app.request(`/order/${ORDER_ID}/cancel`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        cancellationSecret:
          '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
      })
    })

    expect(response.status).toBe(200)
    expect(calls[0]?.method).toBe('POST')
    expect(calls[0]?.path).toBe(`/api/v1/orders/${ORDER_ID}/cancellations`)
    expect(calls[0]?.body).toEqual({
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

class InMemoryCancellationStore implements CancellationStore {
  private readonly records = new Map<string, StoredManagedCancellation>()

  async saveManagedCancellation(input: SaveManagedCancellationInput): Promise<void> {
    const now = new Date().toISOString()
    this.records.set(input.routerOrderId, {
      routerOrderId: input.routerOrderId,
      authMode: input.authMode,
      authPolicy: input.authPolicy,
      encryptedCancellationSecret: input.encryptedCancellationSecret,
      gatewayTokenHash: input.gatewayTokenHash,
      createdAt: now,
      updatedAt: now
    })
  }

  async findManagedCancellation(
    routerOrderId: string
  ): Promise<StoredManagedCancellation | undefined> {
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

function testConfig(): GatewayConfig {
  return {
    host: '0.0.0.0',
    port: 3000,
    routerInternalBaseUrl: 'http://router.internal',
    requestTimeoutMs: 1_000,
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
