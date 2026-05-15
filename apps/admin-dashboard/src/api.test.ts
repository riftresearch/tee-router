import { expect, test } from 'bun:test'

import {
  fetchMe,
  fetchOrders,
  fetchVolumeAnalytics,
  parseMetricsEvent,
  parseRemoveEvent,
  parseSnapshotEvent,
  parseUpsertEvent
} from './api'

test('SSE parsers reject malformed JSON payloads with controlled errors', () => {
  expect(() => parseSnapshotEvent(messageEvent('{'))).toThrow(
    'Invalid snapshot stream JSON payload'
  )
})

test('SSE parsers reject invalid top-level payload shapes', () => {
  expect(() => parseSnapshotEvent(messageEvent('{}'))).toThrow(
    'Invalid order snapshot stream payload'
  )
  expect(() => parseUpsertEvent(messageEvent('{"order":[]}'))).toThrow(
    'Invalid order upsert stream payload'
  )
  expect(() => parseRemoveEvent(messageEvent('{"id":123}'))).toThrow(
    'Invalid order remove stream payload'
  )
  expect(() =>
    parseSnapshotEvent(
      messageEvent('{"orders":[],"metrics":{"total":"0","active":0,"needsAttention":0}}')
    )
  ).toThrow('Invalid order snapshot stream payload')
  expect(() => parseMetricsEvent(messageEvent('{"analyticsChanged":"yes"}'))).toThrow(
    'Invalid order metrics stream payload'
  )
  expect(() =>
    parseMetricsEvent(
      messageEvent('{"metrics":{"total":1.1,"active":0,"needsAttention":0}}')
    )
  ).toThrow('Invalid order metrics stream payload')
  expect(() =>
    parseUpsertEvent(
      messageEvent(JSON.stringify({ order: orderEvent(), analyticsChanged: 'yes' }))
    )
  ).toThrow('Invalid order upsert stream payload')
  expect(() =>
    parseUpsertEvent(
      messageEvent(
        JSON.stringify({
          order: {
            ...orderEvent(),
            progress: {
              totalStages: 1,
              completedStages: 2,
              failedStages: 0
            }
          }
        })
      )
    )
  ).toThrow('Invalid order upsert stream payload')
  expect(() =>
    parseUpsertEvent(
      messageEvent(
        JSON.stringify({
          order: {
            ...orderEvent(),
            createdAt: '2026-05-04'
          }
        })
      )
    )
  ).toThrow('Invalid order upsert stream payload')
})

test('SSE parsers accept well-formed stream payload envelopes', () => {
  const snapshot = parseSnapshotEvent(
    messageEvent(
      '{"orders":[],"metrics":{"total":0,"active":0,"needsAttention":0},"sort":"created_at_desc"}'
    )
  )
  expect(snapshot.orders).toEqual([])
  expect(snapshot.metrics).toEqual({ total: 0, active: 0, needsAttention: 0 })
  expect(snapshot.sort).toBe('created_at_desc')

  const upsert = parseUpsertEvent(
    messageEvent(
      JSON.stringify({
        order: orderEvent(),
        analyticsChanged: true,
        sort: 'created_at_desc'
      })
    )
  )
  expect(upsert.order.id).toBe('order-id')
  expect(upsert.analyticsChanged).toBe(true)
  expect(upsert.sort).toBe('created_at_desc')

  expect(parseRemoveEvent(messageEvent('{"id":"order-id"}'))).toEqual({
    id: 'order-id'
  })
  expect(parseMetricsEvent(messageEvent('{"analyticsChanged":true}'))).toEqual({
    analyticsChanged: true
  })
})

test('REST fetchers reject oversized successful response bodies', async () => {
  const originalFetch = globalThis.fetch
  globalThis.fetch = (async () =>
    new Response('x'.repeat(8 * 1024 * 1024 + 1), {
      status: 200
    })) as unknown as typeof fetch

  try {
    await expect(fetchOrders()).rejects.toThrow('orders response exceeded')
  } finally {
    globalThis.fetch = originalFetch
  }
})

test('REST fetchers reject invalid successful payload shapes', async () => {
  const originalFetch = globalThis.fetch
  globalThis.fetch = (async () =>
    Response.json({
      bucketSize: 'five_minute',
      orderType: 'all',
      from: '2026-05-04T00:00:00.000Z',
      to: '2026-05-05T00:00:00.000Z',
      buckets: 'not-an-array'
    })) as unknown as typeof fetch

  try {
    await expect(
      fetchVolumeAnalytics({
        bucketSize: 'five_minute',
        orderType: 'all',
        from: '2026-05-04T00:00:00.000Z',
        to: '2026-05-05T00:00:00.000Z'
      })
    ).rejects.toThrow('Invalid volume analytics response payload')
  } finally {
    globalThis.fetch = originalFetch
  }
})

test('volume analytics fetcher calls API for every fixed window and bucket size', async () => {
  const originalFetch = globalThis.fetch
  const urls: string[] = []
  globalThis.fetch = (async (input: RequestInfo | URL) => {
    urls.push(String(input))
    const url = new URL(String(input), 'http://localhost')
    return Response.json({
      bucketSize: url.searchParams.get('bucketSize'),
      orderType: url.searchParams.get('orderType'),
      from: url.searchParams.get('from'),
      to: url.searchParams.get('to'),
      buckets: []
    })
  }) as unknown as typeof fetch

  const to = new Date('2026-05-05T00:00:00.000Z')
  const windows = [
    { amount: 6, unit: 'hour' },
    { amount: 1, unit: 'day' },
    { amount: 7, unit: 'day' },
    { amount: 30, unit: 'day' },
    { amount: 90, unit: 'day' }
  ] as const
  const bucketSizes = ['five_minute', 'hour', 'day'] as const

  try {
    for (const window of windows) {
      const from = new Date(to)
      if (window.unit === 'hour') from.setUTCHours(from.getUTCHours() - window.amount)
      else if (window.amount === 1) from.setUTCHours(from.getUTCHours() - 24)
      else from.setUTCDate(from.getUTCDate() - window.amount)

      for (const bucketSize of bucketSizes) {
        await expect(
          fetchVolumeAnalytics({
            bucketSize,
            orderType: 'all',
            from: from.toISOString(),
            to: to.toISOString()
          })
        ).resolves.toMatchObject({
          bucketSize,
          orderType: 'all',
          from: from.toISOString(),
          to: to.toISOString(),
          buckets: []
        })
      }
    }

    expect(urls).toHaveLength(15)
    expect(urls.some((url) => url.includes('bucketSize=hour'))).toBe(true)
    expect(urls.some((url) => url.includes('bucketSize=five_minute'))).toBe(true)
  } finally {
    globalThis.fetch = originalFetch
  }
})

test('fetchMe accepts auth setup error envelopes', async () => {
  const originalFetch = globalThis.fetch
  globalThis.fetch = (async () =>
    Response.json(
      {
        authenticated: false,
        authorized: false,
        user: null,
        allowedEmails: ['cliff@rift.trade'],
        analyticsConfigured: false,
        missingAuthConfig: ['BETTER_AUTH_SECRET']
      },
      { status: 503 }
    )) as unknown as typeof fetch

  try {
    await expect(fetchMe()).resolves.toMatchObject({
      authenticated: false,
      authorized: false,
      user: null,
      missingAuthConfig: ['BETTER_AUTH_SECRET']
    })
  } finally {
    globalThis.fetch = originalFetch
  }
})

function messageEvent(data: string): MessageEvent<string> {
  return { data } as MessageEvent<string>
}

function orderEvent() {
  return {
    id: 'order-id',
    detailLevel: 'summary',
    orderType: 'market_order',
    status: 'pending_funding',
    createdAt: '2026-05-04T00:00:00.000Z',
    updatedAt: '2026-05-04T00:00:00.000Z',
    source: {
      chainId: 'evm:1',
      assetId: 'native'
    },
    destination: {
      chainId: 'evm:8453',
      assetId: 'native'
    },
    recipientAddress: '0x1111111111111111111111111111111111111111',
    refundAddress: '0x2222222222222222222222222222222222222222',
    actionTimeoutAt: '2026-05-04T00:10:00.000Z',
    executionLegs: [],
    executionSteps: [],
    providerOperations: [],
    progress: {
      totalStages: 1,
      completedStages: 0,
      failedStages: 0
    }
  }
}
