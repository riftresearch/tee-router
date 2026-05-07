import { expect, test } from 'bun:test'
import type { Pool } from 'pg'

import {
  MAX_SSE_BACKPRESSURE_CHUNKS,
  MAX_SSE_PENDING_EVENTS,
  createApp,
  createOrderEventStream,
  fetchOrderPage,
  parseAnalyticsRange
} from './app'
import type { AdminDashboardConfig } from './config'
import type { ReplicaDatabaseRuntime } from './db'
import type { OrderCdcBroker, OrderCdcEvent } from './order-cdc'
import { orderMatchesLifecycleFilter, type OrderFirehoseRow } from './orders'

test('/api/status exposes non-sensitive CDC health', async () => {
  const runtime = createApp(testConfig())
  try {
    const response = await runtime.app.request('/api/status')
    expect(response.status).toBe(200)
    const body = (await response.json()) as {
      cdc: {
        configured: boolean
        startupResolved: boolean
        streaming: boolean
      }
    }

    expect(body.cdc).toEqual({
      configured: false,
      startupResolved: true,
      streaming: false
    })
  } finally {
    await runtime.close()
  }
})

test('/api/orders/events accepts market-order streams without an initial snapshot', async () => {
  const controller = new AbortController()
  const consoleError = console.error
  console.error = () => undefined
  const runtime = createApp(
    testConfig({
      replicaDatabaseUrl: 'postgres://postgres:postgres@127.0.0.1:1/postgres',
      cdcSlotName: `admin_dashboard_orders_cdc_test_${Date.now()}`
    })
  )

  try {
    const response = await runtime.app.request(
      '/api/orders/events?limit=100&orderType=market_order&snapshot=false',
      {
        signal: controller.signal
      }
    )
    expect(response.status).toBe(200)
    expect(response.headers.get('content-type')).toContain('text/event-stream')
    const body = await readUntil(response, 'event: ready')
    expect(body).toContain('event: ready')
  } finally {
    controller.abort()
    console.error = consoleError
    await runtime.close()
  }
})

test('/api/orders rejects malformed pagination limits', async () => {
  const consoleError = console.error
  console.error = () => undefined
  const runtime = createApp(
    testConfig({
      replicaDatabaseUrl: 'postgres://postgres:postgres@127.0.0.1:1/postgres',
      cdcSlotName: `admin_dashboard_orders_cdc_limit_test_${Date.now()}`
    })
  )

  try {
    const response = await runtime.app.request('/api/orders?limit=abc')
    expect(response.status).toBe(400)
    expect(await response.json()).toEqual({ error: 'invalid_limit' })
  } finally {
    console.error = consoleError
    await runtime.close()
  }
})

test('/api/orders rejects non-decimal pagination limits', async () => {
  const consoleError = console.error
  console.error = () => undefined
  const runtime = createApp(
    testConfig({
      replicaDatabaseUrl: 'postgres://postgres:postgres@127.0.0.1:1/postgres',
      cdcSlotName: `admin_dashboard_orders_cdc_limit_shape_test_${Date.now()}`
    })
  )

  try {
    const response = await runtime.app.request('/api/orders?limit=1e2')
    expect(response.status).toBe(400)
    expect(await response.json()).toEqual({ error: 'invalid_limit' })
  } finally {
    console.error = consoleError
    await runtime.close()
  }
})

test('/api/orders rejects oversized pagination cursors before decoding', async () => {
  const consoleError = console.error
  console.error = () => undefined
  const runtime = createApp(
    testConfig({
      replicaDatabaseUrl: 'postgres://postgres:postgres@127.0.0.1:1/postgres',
      cdcSlotName: `admin_dashboard_orders_cdc_cursor_test_${Date.now()}`
    })
  )

  try {
    const response = await runtime.app.request(`/api/orders?cursor=${'a'.repeat(513)}`)
    expect(response.status).toBe(400)
    expect(await response.json()).toEqual({ error: 'invalid_cursor' })
  } finally {
    console.error = consoleError
    await runtime.close()
  }
})

test('/api/orders rejects pagination cursors with non-canonical timestamps', async () => {
  const consoleError = console.error
  console.error = () => undefined
  const runtime = createApp(
    testConfig({
      replicaDatabaseUrl: 'postgres://postgres:postgres@127.0.0.1:1/postgres',
      cdcSlotName: `admin_dashboard_orders_cdc_cursor_shape_test_${Date.now()}`
    })
  )
  const cursor = toBase64Url(
    JSON.stringify({
      createdAt: '2026-05-04',
      id: '019df446-096f-7290-bf84-dc9dac9dd8af'
    })
  )

  try {
    const response = await runtime.app.request(`/api/orders?cursor=${cursor}`)
    expect(response.status).toBe(400)
    expect(await response.json()).toEqual({ error: 'invalid_cursor' })
  } finally {
    console.error = consoleError
    await runtime.close()
  }
})

test('/api/orders rejects retired lifecycle filters', async () => {
  const consoleError = console.error
  console.error = () => undefined
  const runtime = createApp(
    testConfig({
      replicaDatabaseUrl: 'postgres://postgres:postgres@127.0.0.1:1/postgres',
      cdcSlotName: `admin_dashboard_orders_cdc_filter_test_${Date.now()}`
    })
  )

  try {
    for (const filter of ['failed', 'manual_refund']) {
      const response = await runtime.app.request(`/api/orders?filter=${filter}`)
      expect(response.status).toBe(400)
      expect(await response.json()).toEqual({ error: 'invalid_order_filter' })
    }
  } finally {
    console.error = consoleError
    await runtime.close()
  }
})

test('order SSE queues lifecycle-filter removes that race with the initial snapshot', async () => {
  const controller = new AbortController()
  const orderId = '019df1c4-8d87-7c20-89a4-e76883e94a0f'
  let listener: ((event: OrderCdcEvent) => void | Promise<void>) | undefined
  let emittedCompletion = false
  const replicaRuntime = {
    pool: {
      query: async (sql: string) => {
        if (isMetricsQuery(sql)) {
          return {
            rows: [{ total: '1', active: '1', needs_attention: '0' }]
          }
        }
        if (sql.includes('FROM public.router_orders ro')) {
          if (!emittedCompletion) {
            emittedCompletion = true
            await listener?.({
              kind: 'upsert',
              order: order({ id: orderId, status: 'completed' }),
              total: 1,
              metrics: {
                total: 1,
                active: 0,
                needsAttention: 0
              },
              sort: 'created_at_desc'
            })
          }
          return {
            rows: [orderRow({ id: orderId, status: 'pending_funding' })]
          }
        }
        throw new Error(`unexpected query: ${sql}`)
      }
    } as unknown as Pool,
    close: async () => undefined
  } satisfies ReplicaDatabaseRuntime
  const broker = {
    subscribe: (next: (event: OrderCdcEvent) => void | Promise<void>) => {
      listener = next
      return () => {
        listener = undefined
      }
    }
  } as unknown as OrderCdcBroker

  const response = createOrderEventStream(
    controller.signal,
    replicaRuntime,
    100,
    'market_order',
    'in_progress',
    true,
    broker
  )
  const body = await readUntil(response, 'event: remove')
  controller.abort()

  expect(body).toContain('event: snapshot')
  expect(body).toContain('event: remove')
  expect(body).toContain(`"id":"${orderId}"`)
  expect(body).toContain('"analyticsChanged":true')
})

test('order SSE snapshot uses analytics metrics hook when available', async () => {
  const controller = new AbortController()
  const orderId = '019df446-096f-7290-bf84-dc9dac9dd8ae'
  let analyticsMetricsCalls = 0
  const replicaRuntime = {
    pool: {
      query: async (sql: string) => {
        if (isMetricsQuery(sql)) {
          throw new Error('SSE snapshot should not query replica metrics')
        }
        if (sql.includes('FROM public.router_orders ro')) {
          return {
            rows: [orderRow({ id: orderId, status: 'pending_funding' })]
          }
        }
        throw new Error(`unexpected query: ${sql}`)
      }
    } as unknown as Pool,
    close: async () => undefined
  } satisfies ReplicaDatabaseRuntime

  const response = createOrderEventStream(
    controller.signal,
    replicaRuntime,
    100,
    'market_order',
    undefined,
    true,
    null,
    async () => {
      analyticsMetricsCalls += 1
      return {
        total: 42,
        active: 7,
        needsAttention: 3
      }
    }
  )
  const body = await readUntil(response, 'event: snapshot')
  controller.abort()

  expect(analyticsMetricsCalls).toBe(1)
  expect(body).toContain('"total":42')
  expect(body).toContain('"active":7')
  expect(body).toContain('"needsAttention":3')
})

test('order SSE emits metrics for off-tab order updates without leaking rows', async () => {
  const controller = new AbortController()
  const orderId = '019df446-096f-7290-bf84-dc9dac9dd8aa'
  const replicaRuntime = {
    pool: {
      query: async () => {
        throw new Error('snapshot queries should not run for snapshot=false')
      }
    } as unknown as Pool,
    close: async () => undefined
  } satisfies ReplicaDatabaseRuntime
  const broker = {
    subscribe: (next: (event: OrderCdcEvent) => void | Promise<void>) => {
      queueMicrotask(() => {
        void next({
          kind: 'upsert',
          order: order({ id: orderId, status: 'completed', orderType: 'limit_order' }),
          total: 7,
          metrics: {
            total: 7,
            active: 1,
            needsAttention: 2
          },
          sort: 'created_at_desc'
        })
      })
      return () => undefined
    }
  } as unknown as OrderCdcBroker

  const response = createOrderEventStream(
    controller.signal,
    replicaRuntime,
    100,
    'market_order',
    'firehose',
    false,
    broker
  )
  const body = await readUntil(response, 'event: metrics')
  controller.abort()

  expect(body).toContain('event: metrics')
  expect(body).toContain('"total":7')
  expect(body).toContain('"active":1')
  expect(body).toContain('"analyticsChanged":true')
  expect(body).not.toContain('event: upsert')
  expect(body).not.toContain(orderId)
})

test('order SSE forwards analytics change hints even when updated status is not completed', async () => {
  const controller = new AbortController()
  const orderId = '019df446-096f-7290-bf84-dc9dac9dd8ab'
  const replicaRuntime = {
    pool: {
      query: async () => {
        throw new Error('snapshot queries should not run for snapshot=false')
      }
    } as unknown as Pool,
    close: async () => undefined
  } satisfies ReplicaDatabaseRuntime
  const broker = {
    subscribe: (next: (event: OrderCdcEvent) => void | Promise<void>) => {
      queueMicrotask(() => {
        void next({
          kind: 'upsert',
          order: order({
            id: orderId,
            status: 'manual_intervention_required',
            orderType: 'market_order'
          }),
          total: 7,
          metrics: {
            total: 7,
            active: 1,
            needsAttention: 2
          },
          analyticsChanged: true,
          sort: 'created_at_desc'
        })
      })
      return () => undefined
    }
  } as unknown as OrderCdcBroker

  const response = createOrderEventStream(
    controller.signal,
    replicaRuntime,
    100,
    'market_order',
    'firehose',
    false,
    broker
  )
  const body = await readUntil(response, 'event: upsert')
  controller.abort()

  expect(body).toContain('event: upsert')
  expect(body).toContain('"analyticsChanged":true')
  expect(body).toContain(orderId)
})

test('order SSE closes instead of buffering an unbounded pre-snapshot backlog', async () => {
  const controller = new AbortController()
  const orderId = '019df446-096f-7290-bf84-dc9dac9dd8ae'
  const replicaRuntime = {
    pool: {
      query: async () => {
        await new Promise(() => undefined)
        return { rows: [] }
      }
    } as unknown as Pool,
    close: async () => undefined
  } satisfies ReplicaDatabaseRuntime
  const broker = {
    subscribe: (next: (event: OrderCdcEvent) => void | Promise<void>) => {
      for (let index = 0; index <= MAX_SSE_PENDING_EVENTS; index += 1) {
        void next({
          kind: 'upsert',
          order: order({ id: orderId, status: 'pending_funding' }),
          total: MAX_SSE_PENDING_EVENTS + 1,
          metrics: {
            total: MAX_SSE_PENDING_EVENTS + 1,
            active: MAX_SSE_PENDING_EVENTS + 1,
            needsAttention: 0
          },
          sort: 'created_at_desc'
        })
      }
      return () => undefined
    }
  } as unknown as OrderCdcBroker

  const response = createOrderEventStream(
    controller.signal,
    replicaRuntime,
    100,
    'market_order',
    'firehose',
    true,
    broker
  )
  const body = await readUntil(response, 'order stream snapshot backlog exceeded')
  controller.abort()

  expect(body).toContain('event: error')
  expect(body).toContain('order stream snapshot backlog exceeded')
})

test('order SSE closes slow clients after the post-snapshot backpressure cap', async () => {
  const controller = new AbortController()
  const orderId = '019df446-096f-7290-bf84-dc9dac9dd8b0'
  let listener: ((event: OrderCdcEvent) => void | Promise<void>) | undefined
  let unsubscribed = false
  const replicaRuntime = {
    pool: {
      query: async () => {
        throw new Error('snapshot queries should not run for snapshot=false')
      }
    } as unknown as Pool,
    close: async () => undefined
  } satisfies ReplicaDatabaseRuntime
  const broker = {
    subscribe: (next: (event: OrderCdcEvent) => void | Promise<void>) => {
      listener = next
      return () => {
        unsubscribed = true
        listener = undefined
      }
    }
  } as unknown as OrderCdcBroker

  const response = createOrderEventStream(
    controller.signal,
    replicaRuntime,
    100,
    undefined,
    'firehose',
    false,
    broker
  )

  await Promise.resolve()
  for (let index = 0; index <= MAX_SSE_BACKPRESSURE_CHUNKS + 2; index += 1) {
    await listener?.({
      kind: 'upsert',
      order: order({ id: orderId, status: 'pending_funding' }),
      total: 1,
      metrics: {
        total: 1,
        active: 1,
        needsAttention: 0
      },
      sort: 'created_at_desc'
    })
  }

  await waitFor(() => unsubscribed)
  controller.abort()
  await response.body?.cancel().catch(() => undefined)

  expect(unsubscribed).toBe(true)
})

test('order SSE removes abort listeners when closed without request abort', async () => {
  const controller = new AbortController()
  const signal = controller.signal as AbortSignal & {
    addEventListener: EventTarget['addEventListener']
    removeEventListener: EventTarget['removeEventListener']
  }
  const originalAddEventListener = signal.addEventListener.bind(signal)
  const originalRemoveEventListener = signal.removeEventListener.bind(signal)
  let abortListeners = 0
  signal.addEventListener = ((type, listener, options) => {
    if (type === 'abort') abortListeners += 1
    return originalAddEventListener(type, listener, options)
  }) as EventTarget['addEventListener']
  signal.removeEventListener = ((type, listener, options) => {
    if (type === 'abort') abortListeners -= 1
    return originalRemoveEventListener(type, listener, options)
  }) as EventTarget['removeEventListener']
  const replicaRuntime = {
    pool: {
      query: async () => {
        throw new Error('snapshot queries should not run for snapshot=false')
      }
    } as unknown as Pool,
    close: async () => undefined
  } satisfies ReplicaDatabaseRuntime
  const broker = {
    subscribe: () => () => undefined
  } as unknown as OrderCdcBroker

  try {
    const response = createOrderEventStream(
      signal,
      replicaRuntime,
      100,
      undefined,
      'firehose',
      false,
      broker
    )
    const reader = response.body?.getReader()
    if (!reader) throw new Error('missing SSE reader')

    await reader.read()
    await waitFor(() => abortListeners >= 2)
    await reader.cancel()
    await waitFor(() => abortListeners === 0)

    expect(abortListeners).toBe(0)
  } finally {
    signal.addEventListener = originalAddEventListener
    signal.removeEventListener = originalRemoveEventListener
    controller.abort()
  }
})

test('order SSE sanitizes internal snapshot failures', async () => {
  const controller = new AbortController()
  const replicaRuntime = {
    pool: {
      query: async () => {
        throw new Error(
          'database failed with secret postgres://admin:password@replica/router'
        )
      }
    } as unknown as Pool,
    close: async () => undefined
  } satisfies ReplicaDatabaseRuntime
  const broker = {
    subscribe: () => () => undefined
  } as unknown as OrderCdcBroker
  const consoleError = console.error
  console.error = () => undefined

  try {
    const response = createOrderEventStream(
      controller.signal,
      replicaRuntime,
      100,
      undefined,
      'firehose',
      true,
      broker
    )
    const body = await readUntil(response, 'order stream failed')
    controller.abort()

    expect(body).toContain('event: error')
    expect(body).toContain('order stream failed')
    expect(body).not.toContain('postgres://admin:password@replica/router')
  } finally {
    console.error = consoleError
  }
})

test('fetchOrderPage falls back to replica metrics when analytics metrics fail', async () => {
  const orderId = '019df446-096f-7290-bf84-dc9dac9dd8af'
  const replicaRuntime = {
    pool: {
      query: async (sql: string) => {
        if (isMetricsQuery(sql)) {
          return {
            rows: [{ total: '1', active: '1', needs_attention: '0' }]
          }
        }
        if (sql.includes('FROM public.router_orders ro')) {
          return {
            rows: [orderRow({ id: orderId, status: 'pending_funding' })]
          }
        }
        throw new Error(`unexpected query: ${sql}`)
      }
    } as unknown as Pool,
    close: async () => undefined
  } satisfies ReplicaDatabaseRuntime
  const consoleError = console.error
  console.error = () => undefined

  try {
    const page = await fetchOrderPage(
      replicaRuntime,
      100,
      undefined,
      undefined,
      undefined,
      true,
      async () => {
        throw new Error('analytics database unavailable')
      }
    )

    expect(page.orders.map((order) => order.id)).toEqual([orderId])
    expect(page.metrics).toEqual({
      total: 1,
      active: 1,
      needsAttention: 0
    })
  } finally {
    console.error = consoleError
  }
})

test('parseAnalyticsRange accepts every fixed dashboard window and bucket size', () => {
  const to = new Date('2026-05-05T00:00:00.000Z')
  const windows = [
    { amount: 6, unit: 'hour' },
    { amount: 1, unit: 'day' },
    { amount: 7, unit: 'day' },
    { amount: 30, unit: 'day' },
    { amount: 90, unit: 'day' }
  ] as const
  const bucketSizes = ['five_minute', 'hour', 'day'] as const

  for (const window of windows) {
    const from = new Date(to)
    if (window.unit === 'hour') from.setUTCHours(from.getUTCHours() - window.amount)
    else if (window.amount === 1) from.setUTCHours(from.getUTCHours() - 24)
    else from.setUTCDate(from.getUTCDate() - window.amount)

    for (const bucketSize of bucketSizes) {
      expect(
        parseAnalyticsRange(from.toISOString(), to.toISOString(), bucketSize)
      ).not.toBeInstanceOf(Response)
    }
  }
})

test('parseAnalyticsRange rejects oversized bucket windows', async () => {
  const rejected = parseAnalyticsRange(
    '2026-05-01T00:00:00.000Z',
    '2027-08-01T00:01:00.000Z',
    'five_minute'
  )
  expect(rejected).toBeInstanceOf(Response)
  expect((rejected as Response).status).toBe(400)
  await expect((rejected as Response).json()).resolves.toEqual({
    error: 'analytics_range_too_large'
  })
})

test('parseAnalyticsRange rejects oversized timestamp strings', async () => {
  const rejected = parseAnalyticsRange(
    `${'2026-05-01T00:00:00.000Z'}${'0'.repeat(65)}`,
    '2026-05-08T00:00:00.000Z',
    'hour'
  )

  expect(rejected).toBeInstanceOf(Response)
  expect((rejected as Response).status).toBe(400)
  await expect((rejected as Response).json()).resolves.toEqual({
    error: 'invalid_analytics_range'
  })
})

test('parseAnalyticsRange rejects non-canonical timestamp strings', async () => {
  const rejected = parseAnalyticsRange(
    '2026-05-01',
    '2026-05-08T00:00:00.000Z',
    'day'
  )

  expect(rejected).toBeInstanceOf(Response)
  expect((rejected as Response).status).toBe(400)
  await expect((rejected as Response).json()).resolves.toEqual({
    error: 'invalid_analytics_range'
  })
})

test('lifecycle filters keep refunded orders out of the needs-attention tab', () => {
  expect(
    orderMatchesLifecycleFilter(
      order({
        id: '019df446-096f-7290-bf84-dc9dac9dd8ab',
        status: 'refunded'
      }),
      'needs_attention'
    )
  ).toBe(false)
  expect(
    orderMatchesLifecycleFilter(
      order({
        id: '019df446-096f-7290-bf84-dc9dac9dd8ab',
        status: 'refunded'
      }),
      'refunded'
    )
  ).toBe(true)
  expect(
    orderMatchesLifecycleFilter(
      order({
        id: '019df446-096f-7290-bf84-dc9dac9dd8ac',
        status: 'refund_required'
      }),
      'needs_attention'
    )
  ).toBe(true)
})

test('lifecycle filters treat manual intervention as needs-attention', () => {
  const manualInterventionOrder = order({
    id: '019df446-096f-7290-bf84-dc9dac9dd8ad',
    status: 'manual_intervention_required'
  })

  expect(
    orderMatchesLifecycleFilter(manualInterventionOrder, 'needs_attention')
  ).toBe(true)
  expect(
    orderMatchesLifecycleFilter(manualInterventionOrder, 'in_progress')
  ).toBe(false)
})

test('lifecycle filters keep expired orders in the dedicated expired tab', () => {
  const expiredOrder = order({
    id: '019df446-096f-7290-bf84-dc9dac9dd8ae',
    status: 'expired'
  })

  expect(
    orderMatchesLifecycleFilter(expiredOrder, 'needs_attention')
  ).toBe(false)
  expect(orderMatchesLifecycleFilter(expiredOrder, 'expired')).toBe(true)
  expect(orderMatchesLifecycleFilter(expiredOrder, 'in_progress')).toBe(false)
})

async function readUntil(response: Response, expected: string) {
  const reader = response.body?.getReader()
  if (!reader) throw new Error('missing response body')
  let body = ''
  for (;;) {
    const result = await reader.read()
    if (result.done) return body
    body += new TextDecoder().decode(result.value)
    if (body.includes(expected)) return body
  }
}

async function waitFor(condition: () => boolean) {
  for (let index = 0; index < 20; index += 1) {
    if (condition()) return
    await new Promise((resolve) => setTimeout(resolve, 1))
  }
  throw new Error('condition was not met')
}

function isMetricsQuery(sql: string) {
  return sql.includes('COUNT(*) FILTER') && sql.includes('FROM public.router_orders')
}

function toBase64Url(value: string): string {
  return btoa(value).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/u, '')
}

function testConfig(overrides: Partial<AdminDashboardConfig> = {}): AdminDashboardConfig {
  return {
    production: false,
    hostedEnvironment: false,
    host: '127.0.0.1',
    port: 3000,
    webOrigin: 'http://localhost:5173',
    authBaseUrl: 'http://localhost:3000',
    trustedOrigins: ['http://localhost:3000', 'http://localhost:5173'],
    betterAuthSecret: 'test',
    secureCookies: false,
    orderLimit: 100,
    cdcSlotName: 'admin_dashboard_orders_cdc_test',
    cdcPublicationName: 'router_cdc_publication',
    cdcMessagePrefix: 'rift.router.change',
    allowInsecureDevAuthBypass: false,
    serveStatic: false,
    version: 'test',
    missingAuthConfig: [],
    ...overrides
  }
}

function orderRow({
  id,
  status,
  orderType = 'market_order'
}: {
  id: string
  status: string
  orderType?: 'market_order' | 'limit_order'
}) {
  const now = new Date('2026-05-04T00:00:00.000Z')
  return {
    id,
    order_type: orderType,
    status,
    created_at: now,
    updated_at: now,
    source_chain_id: 'evm:1',
    source_asset_id: 'native',
    destination_chain_id: 'evm:8453',
    destination_asset_id: 'native',
    recipient_address: '0x1111111111111111111111111111111111111111',
    refund_address: '0x2222222222222222222222222222222222222222',
    action_timeout_at: now,
    workflow_trace_id: null,
    workflow_parent_span_id: null,
    quote_id: null,
    quote_provider_id: null,
    order_kind: null,
    quoted_amount_in: null,
    quoted_amount_out: null,
    min_amount_out: null,
    max_amount_in: null,
    slippage_bps: null,
    quote_expires_at: null,
    provider_quote: {},
    quote_usd_valuation: {},
    execution_legs: [],
    execution_steps: [
      {
        id: '019df1c4-a8ac-7731-b596-a24603580d4f',
        stepIndex: 0,
        stepType: 'wait_for_deposit',
        provider: 'internal',
        status: 'completed',
        txHash: '0xabc',
        request: {},
        response: {},
        details: {},
        error: {},
        usdValuation: {},
        createdAt: now.toISOString(),
        updatedAt: now.toISOString()
      }
    ],
    provider_operations: [],
    funding_tx_hash: '0xabc'
  }
}

function order(input: {
  id: string
  status: string
  orderType?: 'market_order' | 'limit_order'
}): OrderFirehoseRow {
  const row = orderRow(input)
  return {
    id: row.id,
    detailLevel: 'summary',
    orderType: row.order_type,
    status: row.status,
    createdAt: row.created_at.toISOString(),
    updatedAt: row.updated_at.toISOString(),
    fundingTxHash: row.funding_tx_hash,
    source: {
      chainId: row.source_chain_id,
      assetId: row.source_asset_id
    },
    destination: {
      chainId: row.destination_chain_id,
      assetId: row.destination_asset_id
    },
    recipientAddress: row.recipient_address,
    refundAddress: row.refund_address,
    actionTimeoutAt: row.action_timeout_at.toISOString(),
    executionLegs: [],
    executionSteps: row.execution_steps,
    providerOperations: [],
    progress: {
      totalStages: 1,
      completedStages: row.status === 'completed' ? 1 : 0,
      failedStages: 0
    }
  }
}
