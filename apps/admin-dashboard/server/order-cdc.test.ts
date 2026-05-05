import { expect, test } from 'bun:test'
import type { Pool } from 'pg'

import type { AdminDashboardConfig } from './config'
import {
  ensureCdcSlot,
  MAX_CDC_ORDER_FETCH_BATCH_SIZE,
  MAX_CDC_PENDING_ORDER_IDS,
  OrderCdcBroker
} from './order-cdc'
import type { OrderCdcEvent } from './order-cdc'

test('OrderCdcBroker requeues pending order ids when a flush fails', async () => {
  const orderId = '019df1c4-8d87-7c20-89a4-e76883e94a0f'
  const pool = {
    query: async () => {
      throw new Error('replica temporarily unavailable')
    }
  } as unknown as Pool
  const broker = new OrderCdcBroker(pool, testConfig)
  const brokerInternals = broker as unknown as {
    pendingOrderIds: Set<string>
    pendingAckLsn: string | undefined
    service: FakeReplicationService
    flushPendingOrders: () => Promise<void>
  }
  let acknowledgedLsn: string | undefined
  brokerInternals.pendingAckLsn = '0/10'
  brokerInternals.service = {
    acknowledge: async (lsn: string) => {
      acknowledgedLsn = lsn
      return true
    },
    destroy: async () => undefined
  }
  const consoleError = console.error
  console.error = () => undefined
  broker.subscribe(() => undefined)

  try {
    brokerInternals.pendingOrderIds.add(orderId)
    await brokerInternals.flushPendingOrders()
    expect(brokerInternals.pendingOrderIds.has(orderId)).toBe(true)
    expect(brokerInternals.pendingAckLsn).toBe('0/10')
    expect(acknowledgedLsn).toBeUndefined()
  } finally {
    console.error = consoleError
    await broker.close()
  }
})

test('OrderCdcBroker manually acknowledges irrelevant CDC messages after flush', async () => {
  const broker = new OrderCdcBroker({} as Pool, testConfig)
  const brokerInternals = broker as unknown as {
    pendingAckLsn: string | undefined
    service: FakeReplicationService
    flushPendingOrders: () => Promise<void>
  }
  let acknowledgedLsn: string | undefined
  brokerInternals.pendingAckLsn = '0/20'
  brokerInternals.service = {
    acknowledge: async (lsn: string) => {
      acknowledgedLsn = lsn
      return true
    },
    destroy: async () => undefined
  }

  try {
    await brokerInternals.flushPendingOrders()
    expect(acknowledgedLsn).toBe('0/20')
    expect(brokerInternals.pendingAckLsn).toBeUndefined()
  } finally {
    await broker.close()
  }
})

test('OrderCdcBroker retries acknowledgement failures for empty CDC batches', async () => {
  const broker = new OrderCdcBroker({} as Pool, testConfig)
  const brokerInternals = broker as unknown as {
    pendingAckLsn: string | undefined
    flushTimer: ReturnType<typeof setTimeout> | undefined
    service: FakeReplicationService
    flushPendingOrders: () => Promise<void>
  }
  let ackAttempts = 0
  brokerInternals.pendingAckLsn = '0/21'
  brokerInternals.service = {
    acknowledge: async () => {
      ackAttempts += 1
      if (ackAttempts === 1) throw new Error('replication connection closed')
      return true
    },
    destroy: async () => undefined
  }
  const consoleError = console.error
  console.error = () => undefined

  try {
    await brokerInternals.flushPendingOrders()
    if (brokerInternals.flushTimer) clearTimeout(brokerInternals.flushTimer)

    expect(ackAttempts).toBe(1)
    expect(brokerInternals.pendingAckLsn).toBe('0/21')

    await brokerInternals.flushPendingOrders()

    expect(ackAttempts).toBe(2)
    expect(brokerInternals.pendingAckLsn).toBeUndefined()
  } finally {
    console.error = consoleError
    await broker.close()
  }
})

test('OrderCdcBroker requeues pending order ids when rows are not yet visible', async () => {
  const orderId = '019df1c4-8d87-7c20-89a4-e76883e94a0f'
  const pool = {
    query: async (sql: string) => {
      if (isMetricsQuery(sql)) {
        return {
          rows: [{ total: '1', active: '1', needs_attention: '0' }]
        }
      }
      if (sql.includes('WHERE ro.id = ANY($1::uuid[])')) {
        return { rows: [] }
      }
      throw new Error(`unexpected query: ${sql}`)
    }
  } as unknown as Pool
  const broker = new OrderCdcBroker(pool, testConfig)
  const brokerInternals = broker as unknown as {
    pendingOrderIds: Set<string>
    pendingAckLsn: string | undefined
    service: FakeReplicationService
    flushPendingOrders: () => Promise<void>
  }
  let acknowledgedLsn: string | undefined
  brokerInternals.pendingAckLsn = '0/30'
  brokerInternals.service = {
    acknowledge: async (lsn: string) => {
      acknowledgedLsn = lsn
      return true
    },
    destroy: async () => undefined
  }
  const consoleError = console.error
  console.error = () => undefined
  broker.subscribe(() => undefined)

  try {
    brokerInternals.pendingOrderIds.add(orderId)
    await brokerInternals.flushPendingOrders()
    expect(brokerInternals.pendingOrderIds.has(orderId)).toBe(true)
    expect(brokerInternals.pendingAckLsn).toBe('0/30')
    expect(acknowledgedLsn).toBeUndefined()
  } finally {
    console.error = consoleError
    await broker.close()
  }
})

test('OrderCdcBroker restarts without ack when pending CDC backlog is too large', async () => {
  const broker = new OrderCdcBroker({} as Pool, testConfig)
  const service = createFakeService()
  const brokerInternals = broker as unknown as {
    pendingOrderIds: Set<string>
    pendingAckLsn: string | undefined
    service: FakeReplicationService | undefined
    flushTimer: ReturnType<typeof setTimeout> | undefined
    getHealth: () => { streaming: boolean; lastErrorAt?: string }
    handleReplicationMessage: (lsn: string, message: unknown) => Promise<void>
  }
  brokerInternals.service = service
  const consoleError = console.error
  console.error = () => undefined

  try {
    for (let index = 0; index <= MAX_CDC_PENDING_ORDER_IDS; index += 1) {
      const orderId = indexedUuid(index)
      await brokerInternals.handleReplicationMessage(
        `0/${(index + 1).toString(16)}`,
        routerCdcMessage({
          table: 'router_orders',
          op: 'UPDATE',
          id: orderId,
          orderId,
          orderUpdatedAt: '2026-05-04T06:00:00.000Z',
          eventUpdatedAt: '2026-05-04T06:00:00.000Z'
        })
      )
    }

    expect(service.destroyCalls).toBe(1)
    expect(brokerInternals.service).toBeUndefined()
    expect(brokerInternals.pendingOrderIds.size).toBe(0)
    expect(brokerInternals.pendingAckLsn).toBeUndefined()
    expect(brokerInternals.flushTimer).toBeUndefined()
    const health = brokerInternals.getHealth()
    expect(health.streaming).toBe(false)
    expect(Date.parse(health.lastErrorAt ?? '')).toBeGreaterThan(0)
  } finally {
    console.error = consoleError
    await broker.close()
  }
})

test('OrderCdcBroker acknowledges router order deletes and dispatches remove events', async () => {
  const orderId = '019df1c4-8d87-7c20-89a4-e76883e94a0f'
  const pool = createMetricsOnlyPool()
  const broker = new OrderCdcBroker(pool, testConfig)
  const brokerInternals = broker as unknown as {
    pendingRemoveOrders: Map<string, string | undefined>
    pendingAckLsn: string | undefined
    service: FakeReplicationService
    flushPendingOrders: () => Promise<void>
  }
  let acknowledgedLsn: string | undefined
  const removedEvents: { id: string; sourceUpdatedAt?: string }[] = []
  brokerInternals.pendingAckLsn = '0/35'
  brokerInternals.service = {
    acknowledge: async (lsn: string) => {
      acknowledgedLsn = lsn
      return true
    },
    destroy: async () => undefined
  }
  broker.subscribe((event) => {
    if (event.kind === 'remove') {
      removedEvents.push({ id: event.id, sourceUpdatedAt: event.sourceUpdatedAt })
    }
  })

  try {
    brokerInternals.pendingRemoveOrders.set(
      orderId,
      '2026-05-04T06:00:00.000Z'
    )
    await brokerInternals.flushPendingOrders()
    expect(brokerInternals.pendingRemoveOrders.has(orderId)).toBe(false)
    expect(removedEvents).toEqual([
      { id: orderId, sourceUpdatedAt: '2026-05-04T06:00:00.000Z' }
    ])
    expect(acknowledgedLsn).toBe('0/35')
  } finally {
    await broker.close()
  }
})

test('OrderCdcBroker lets deletes dominate stale pending upserts in the same flush', async () => {
  const orderId = '019df1c4-8d87-7c20-89a4-e76883e94a0f'
  const pool = createMetricsOnlyPool()
  const broker = new OrderCdcBroker(pool, testConfig)
  const brokerInternals = broker as unknown as {
    pendingOrderIds: Set<string>
    pendingRemoveOrders: Map<string, string | undefined>
    pendingAckLsn: string | undefined
    service: FakeReplicationService
    flushPendingOrders: () => Promise<void>
  }
  let acknowledgedLsn: string | undefined
  const events: string[] = []
  brokerInternals.pendingAckLsn = '0/37'
  brokerInternals.service = {
    acknowledge: async (lsn: string) => {
      acknowledgedLsn = lsn
      return true
    },
    destroy: async () => undefined
  }
  broker.subscribe((event) => {
    events.push(`${event.kind}:${event.kind === 'remove' ? event.id : event.order.id}`)
  })

  try {
    brokerInternals.pendingOrderIds.add(orderId)
    brokerInternals.pendingRemoveOrders.set(orderId, undefined)
    await brokerInternals.flushPendingOrders()
    expect(brokerInternals.pendingOrderIds.has(orderId)).toBe(false)
    expect(brokerInternals.pendingRemoveOrders.has(orderId)).toBe(false)
    expect(events).toEqual([`remove:${orderId}`])
    expect(acknowledgedLsn).toBe('0/37')
  } finally {
    await broker.close()
  }
})

test('OrderCdcBroker preserves router-order delete source timestamps', async () => {
  const orderId = '019df1c4-8d87-7c20-89a4-e76883e94a0f'
  const broker = new OrderCdcBroker({} as Pool, testConfig)
  const brokerInternals = broker as unknown as {
    pendingRemoveOrders: Map<string, string | undefined>
    flushTimer: ReturnType<typeof setTimeout> | undefined
    handleReplicationMessage: (lsn: string, message: unknown) => Promise<void>
  }

  try {
    await brokerInternals.handleReplicationMessage('0/38', {
      tag: 'message',
      prefix: testConfig.cdcMessagePrefix,
      content: new TextEncoder().encode(
        JSON.stringify({
          version: 1,
          schema: 'public',
          table: 'router_orders',
          op: 'DELETE',
          id: orderId,
          orderId,
          orderUpdatedAt: '2026-05-04T06:00:00.000Z',
          eventUpdatedAt: '2026-05-04T06:00:00.000Z'
        })
      )
    })
    if (brokerInternals.flushTimer) clearTimeout(brokerInternals.flushTimer)

    expect(brokerInternals.pendingRemoveOrders.get(orderId)).toBe(
      '2026-05-04T06:00:00.000Z'
    )
  } finally {
    await broker.close()
  }
})

test('OrderCdcBroker keeps newest source timestamp across cascaded deletes', async () => {
  const orderId = '019df1c4-8d87-7c20-89a4-e76883e94a0f'
  const broker = new OrderCdcBroker({} as Pool, testConfig)
  const brokerInternals = broker as unknown as {
    pendingRemoveOrders: Map<string, string | undefined>
    flushTimer: ReturnType<typeof setTimeout> | undefined
    handleReplicationMessage: (lsn: string, message: unknown) => Promise<void>
  }

  try {
    await brokerInternals.handleReplicationMessage('0/38', {
      tag: 'message',
      prefix: testConfig.cdcMessagePrefix,
      content: new TextEncoder().encode(
        JSON.stringify({
          version: 1,
          schema: 'public',
          table: 'order_execution_steps',
          op: 'DELETE',
          id: '019df1c4-a8ac-7731-b596-a24603580d4f',
          orderId,
          eventUpdatedAt: '2026-05-04T06:02:00.000Z'
        })
      )
    })
    await brokerInternals.handleReplicationMessage('0/39', {
      tag: 'message',
      prefix: testConfig.cdcMessagePrefix,
      content: new TextEncoder().encode(
        JSON.stringify({
          version: 1,
          schema: 'public',
          table: 'router_orders',
          op: 'DELETE',
          id: orderId,
          orderId,
          orderUpdatedAt: '2026-05-04T06:00:00.000Z',
          eventUpdatedAt: '2026-05-04T06:00:00.000Z'
        })
      )
    })
    if (brokerInternals.flushTimer) clearTimeout(brokerInternals.flushTimer)

    expect(brokerInternals.pendingRemoveOrders.get(orderId)).toBe(
      '2026-05-04T06:02:00.000Z'
    )
  } finally {
    await broker.close()
  }
})

test('OrderCdcBroker keeps newest source timestamp when child delete follows router delete', async () => {
  const orderId = '019df1c4-8d87-7c20-89a4-e76883e94a0f'
  const broker = new OrderCdcBroker({} as Pool, testConfig)
  const brokerInternals = broker as unknown as {
    pendingRemoveOrders: Map<string, string | undefined>
    flushTimer: ReturnType<typeof setTimeout> | undefined
    handleReplicationMessage: (lsn: string, message: unknown) => Promise<void>
  }

  try {
    await brokerInternals.handleReplicationMessage('0/38', {
      tag: 'message',
      prefix: testConfig.cdcMessagePrefix,
      content: new TextEncoder().encode(
        JSON.stringify({
          version: 1,
          schema: 'public',
          table: 'router_orders',
          op: 'DELETE',
          id: orderId,
          orderId,
          orderUpdatedAt: '2026-05-04T06:00:00.000Z',
          eventUpdatedAt: '2026-05-04T06:00:00.000Z'
        })
      )
    })
    await brokerInternals.handleReplicationMessage('0/39', {
      tag: 'message',
      prefix: testConfig.cdcMessagePrefix,
      content: new TextEncoder().encode(
        JSON.stringify({
          version: 1,
          schema: 'public',
          table: 'order_execution_steps',
          op: 'DELETE',
          id: '019df1c4-a8ac-7731-b596-a24603580d4f',
          orderId,
          eventUpdatedAt: '2026-05-04T06:02:00.000Z'
        })
      )
    })
    if (brokerInternals.flushTimer) clearTimeout(brokerInternals.flushTimer)

    expect(brokerInternals.pendingRemoveOrders.get(orderId)).toBe(
      '2026-05-04T06:02:00.000Z'
    )
  } finally {
    await broker.close()
  }
})

test('OrderCdcBroker requests snapshot backfill for unsupported CDC payload versions and tables', async () => {
  const orderId = '019df1c4-8d87-7c20-89a4-e76883e94a0f'
  const broker = new OrderCdcBroker({} as Pool, testConfig)
  let backfillRequests = 0
  broker.subscribeSnapshotBackfillRequired(() => {
    backfillRequests += 1
  })
  const brokerInternals = broker as unknown as {
    pendingOrderIds: Set<string>
    pendingAckLsn: string | undefined
    pendingSnapshotBackfillRequired: boolean
    flushTimer: ReturnType<typeof setTimeout> | undefined
    service: FakeReplicationService
    handleReplicationMessage: (lsn: string, message: unknown) => Promise<void>
    flushPendingOrders: () => Promise<void>
  }
  let acknowledgedLsn: string | undefined
  brokerInternals.service = {
    acknowledge: async (lsn: string) => {
      acknowledgedLsn = lsn
      return true
    },
    destroy: async () => undefined
  }

  try {
    await brokerInternals.handleReplicationMessage('0/38', {
      tag: 'message',
      prefix: testConfig.cdcMessagePrefix,
      content: new TextEncoder().encode(
        JSON.stringify({
          version: 2,
          schema: 'public',
          table: 'router_orders',
          op: 'UPDATE',
          orderId
        })
      )
    })
    await brokerInternals.handleReplicationMessage('0/39', {
      tag: 'message',
      prefix: testConfig.cdcMessagePrefix,
      content: new TextEncoder().encode(
        JSON.stringify({
          version: 1,
          schema: 'public',
          table: 'unrelated_table',
          op: 'UPDATE',
          orderId
        })
      )
    })
    await brokerInternals.handleReplicationMessage('0/3a', {
      tag: 'message',
      prefix: testConfig.cdcMessagePrefix,
      content: new TextEncoder().encode(
        JSON.stringify({
          version: '1',
          schema: 'public',
          table: 'router_orders',
          op: 'UPDATE',
          id: orderId,
          orderId
        })
      )
    })
    if (brokerInternals.flushTimer) clearTimeout(brokerInternals.flushTimer)
    await brokerInternals.flushPendingOrders()

    expect(brokerInternals.pendingOrderIds.size).toBe(0)
    expect(brokerInternals.pendingSnapshotBackfillRequired).toBe(false)
    expect(brokerInternals.pendingAckLsn).toBeUndefined()
    expect(backfillRequests).toBe(1)
    expect(acknowledgedLsn).toBe('0/3a')
  } finally {
    await broker.close()
  }
})

test('OrderCdcBroker withholds acknowledgement until required snapshot backfill succeeds', async () => {
  const orderId = '019df1c4-8d87-7c20-89a4-e76883e94a0f'
  const broker = new OrderCdcBroker({} as Pool, testConfig)
  const brokerInternals = broker as unknown as {
    pendingAckLsn: string | undefined
    pendingSnapshotBackfillRequired: boolean
    flushTimer: ReturnType<typeof setTimeout> | undefined
    service: FakeReplicationService
    handleReplicationMessage: (lsn: string, message: unknown) => Promise<void>
    flushPendingOrders: () => Promise<void>
  }
  let failBackfill = true
  let backfillAttempts = 0
  let acknowledgedLsn: string | undefined
  broker.subscribeSnapshotBackfillRequired(() => {
    backfillAttempts += 1
    if (failBackfill) throw new Error('analytics database unavailable')
  })
  brokerInternals.service = {
    acknowledge: async (lsn: string) => {
      acknowledgedLsn = lsn
      return true
    },
    destroy: async () => undefined
  }
  const consoleError = console.error
  console.error = () => undefined

  try {
    await brokerInternals.handleReplicationMessage('0/4b', {
      tag: 'message',
      prefix: testConfig.cdcMessagePrefix,
      content: new TextEncoder().encode(
        JSON.stringify({
          version: 2,
          schema: 'public',
          table: 'router_orders',
          op: 'UPDATE',
          id: orderId,
          orderId
        })
      )
    })
    if (brokerInternals.flushTimer) clearTimeout(brokerInternals.flushTimer)

    await brokerInternals.flushPendingOrders()
    if (brokerInternals.flushTimer) clearTimeout(brokerInternals.flushTimer)

    expect(backfillAttempts).toBe(1)
    expect(brokerInternals.pendingSnapshotBackfillRequired).toBe(true)
    expect(brokerInternals.pendingAckLsn).toBe('0/4b')
    expect(acknowledgedLsn).toBeUndefined()

    failBackfill = false
    await brokerInternals.flushPendingOrders()

    expect(backfillAttempts).toBe(2)
    expect(brokerInternals.pendingSnapshotBackfillRequired).toBe(false)
    expect(brokerInternals.pendingAckLsn).toBeUndefined()
    expect(acknowledgedLsn).toBe('0/4b')
  } finally {
    console.error = consoleError
    await broker.close()
  }
})

test('OrderCdcBroker requests snapshot backfill for oversized CDC payloads before queuing work', async () => {
  const broker = new OrderCdcBroker({} as Pool, testConfig)
  let backfillRequests = 0
  broker.subscribeSnapshotBackfillRequired(() => {
    backfillRequests += 1
  })
  const brokerInternals = broker as unknown as {
    pendingOrderIds: Set<string>
    pendingAckLsn: string | undefined
    pendingSnapshotBackfillRequired: boolean
    flushTimer: ReturnType<typeof setTimeout> | undefined
    service: FakeReplicationService
    handleReplicationMessage: (lsn: string, message: unknown) => Promise<void>
    flushPendingOrders: () => Promise<void>
  }
  let acknowledgedLsn: string | undefined
  brokerInternals.service = {
    acknowledge: async (lsn: string) => {
      acknowledgedLsn = lsn
      return true
    },
    destroy: async () => undefined
  }

  try {
    await brokerInternals.handleReplicationMessage('0/39', {
      tag: 'message',
      prefix: testConfig.cdcMessagePrefix,
      content: new Uint8Array(16 * 1024 + 1)
    })
    if (brokerInternals.flushTimer) clearTimeout(brokerInternals.flushTimer)
    await brokerInternals.flushPendingOrders()

    expect(brokerInternals.pendingOrderIds.size).toBe(0)
    expect(brokerInternals.pendingSnapshotBackfillRequired).toBe(false)
    expect(brokerInternals.pendingAckLsn).toBeUndefined()
    expect(backfillRequests).toBe(1)
    expect(acknowledgedLsn).toBe('0/39')
  } finally {
    await broker.close()
  }
})

test('OrderCdcBroker requests snapshot backfill for CDC payloads with invalid ids and timestamps', async () => {
  const orderId = '019df1c4-8d87-7c20-89a4-e76883e94a0f'
  const broker = new OrderCdcBroker({} as Pool, testConfig)
  let backfillRequests = 0
  broker.subscribeSnapshotBackfillRequired(() => {
    backfillRequests += 1
  })
  const brokerInternals = broker as unknown as {
    pendingOrderIds: Set<string>
    pendingRefreshOrRemoveOrders: Map<string, string | undefined>
    pendingAckLsn: string | undefined
    pendingSnapshotBackfillRequired: boolean
    flushTimer: ReturnType<typeof setTimeout> | undefined
    service: FakeReplicationService
    handleReplicationMessage: (lsn: string, message: unknown) => Promise<void>
    flushPendingOrders: () => Promise<void>
  }
  let acknowledgedLsn: string | undefined
  brokerInternals.service = {
    acknowledge: async (lsn: string) => {
      acknowledgedLsn = lsn
      return true
    },
    destroy: async () => undefined
  }

  try {
    await brokerInternals.handleReplicationMessage('0/40', {
      tag: 'message',
      prefix: testConfig.cdcMessagePrefix,
      content: new TextEncoder().encode(
        JSON.stringify({
          version: 1,
          schema: 'public',
          table: 'router_orders',
          op: 'UPDATE',
          id: orderId,
          orderId: 'not-a-uuid'
        })
      )
    })
    await brokerInternals.handleReplicationMessage('0/41', {
      tag: 'message',
      prefix: testConfig.cdcMessagePrefix,
      content: new TextEncoder().encode(
        JSON.stringify({
          version: 1,
          schema: 'public',
          table: 'order_execution_steps',
          op: 'DELETE',
          id: '019df1c4-a8ac-7731-b596-a24603580d4f',
          orderId,
          eventUpdatedAt: 'not-a-timestamp'
        })
      )
    })
    await brokerInternals.handleReplicationMessage('0/42', {
      tag: 'message',
      prefix: testConfig.cdcMessagePrefix,
      content: new TextEncoder().encode(
        JSON.stringify({
          version: 1,
          schema: 'public',
          table: 'order_execution_steps',
          op: 'DELETE',
          id: '019df1c4-a8ac-7731-b596-a24603580d4f',
          orderId,
          eventUpdatedAt: '2026-05-04'
        })
      )
    })
    if (brokerInternals.flushTimer) clearTimeout(brokerInternals.flushTimer)
    await brokerInternals.flushPendingOrders()

    expect(brokerInternals.pendingOrderIds.size).toBe(0)
    expect(brokerInternals.pendingRefreshOrRemoveOrders.size).toBe(0)
    expect(brokerInternals.pendingSnapshotBackfillRequired).toBe(false)
    expect(brokerInternals.pendingAckLsn).toBeUndefined()
    expect(backfillRequests).toBe(1)
    expect(acknowledgedLsn).toBe('0/42')
  } finally {
    await broker.close()
  }
})

test('OrderCdcBroker requests snapshot backfill for CDC payloads with null required identity fields', async () => {
  const orderId = '019df1c4-8d87-7c20-89a4-e76883e94a0f'
  const stepId = '019df1c4-a8ac-7731-b596-a24603580d4f'
  const broker = new OrderCdcBroker({} as Pool, testConfig)
  let backfillRequests = 0
  broker.subscribeSnapshotBackfillRequired(() => {
    backfillRequests += 1
  })
  const brokerInternals = broker as unknown as {
    pendingOrderIds: Set<string>
    pendingAckLsn: string | undefined
    pendingSnapshotBackfillRequired: boolean
    flushTimer: ReturnType<typeof setTimeout> | undefined
    service: FakeReplicationService
    handleReplicationMessage: (lsn: string, message: unknown) => Promise<void>
    flushPendingOrders: () => Promise<void>
  }
  let acknowledgedLsn: string | undefined
  brokerInternals.service = {
    acknowledge: async (lsn: string) => {
      acknowledgedLsn = lsn
      return true
    },
    destroy: async () => undefined
  }

  try {
    await brokerInternals.handleReplicationMessage(
      '0/43',
      routerCdcMessage({
        table: 'order_execution_steps',
        op: 'UPDATE',
        id: stepId,
        orderId: null,
        eventUpdatedAt: '2026-05-04T06:00:00.000Z'
      })
    )
    await brokerInternals.handleReplicationMessage(
      '0/44',
      routerCdcMessage({
        table: 'order_execution_steps',
        op: 'UPDATE',
        id: stepId,
        orderId,
        eventUpdatedAt: null
      })
    )
    await brokerInternals.handleReplicationMessage(
      '0/45',
      routerCdcMessage({
        table: 'router_orders',
        op: 'UPDATE',
        id: orderId,
        orderId,
        orderUpdatedAt: null,
        eventUpdatedAt: '2026-05-04T06:00:00.000Z'
      })
    )
    if (brokerInternals.flushTimer) clearTimeout(brokerInternals.flushTimer)
    await brokerInternals.flushPendingOrders()

    expect(brokerInternals.pendingOrderIds.size).toBe(0)
    expect(brokerInternals.pendingSnapshotBackfillRequired).toBe(false)
    expect(brokerInternals.pendingAckLsn).toBeUndefined()
    expect(backfillRequests).toBe(1)
    expect(acknowledgedLsn).toBe('0/45')
  } finally {
    await broker.close()
  }
})

test('OrderCdcBroker ignores supported CDC messages that are not associated with an order', async () => {
  const broker = new OrderCdcBroker({} as Pool, testConfig)
  let backfillRequests = 0
  broker.subscribeSnapshotBackfillRequired(() => {
    backfillRequests += 1
  })
  const brokerInternals = broker as unknown as {
    pendingOrderIds: Set<string>
    pendingAckLsn: string | undefined
    flushTimer: ReturnType<typeof setTimeout> | undefined
    handleReplicationMessage: (lsn: string, message: unknown) => Promise<void>
  }

  try {
    await brokerInternals.handleReplicationMessage(
      '0/46',
      routerCdcMessage({
        table: 'market_order_quotes',
        op: 'INSERT',
        id: '019df1c4-b9df-7661-a0f3-8d8047dacfe3',
        orderId: null,
        eventUpdatedAt: '2026-05-04T06:00:00.000Z'
      })
    )
    await brokerInternals.handleReplicationMessage(
      '0/47',
      routerCdcMessage({
        table: 'custody_vaults',
        op: 'UPDATE',
        id: '019df1c4-bc3d-7a81-bb7d-9e3a753d73f1',
        orderId: null,
        eventUpdatedAt: '2026-05-04T06:00:01.000Z'
      })
    )
    if (brokerInternals.flushTimer) clearTimeout(brokerInternals.flushTimer)

    expect(brokerInternals.pendingOrderIds.size).toBe(0)
    expect(brokerInternals.pendingAckLsn).toBe('0/47')
    expect(backfillRequests).toBe(0)
  } finally {
    await broker.close()
  }
})

test('OrderCdcBroker treats deleted related rows with missing parent as remove events', async () => {
  const orderId = '019df1c4-8d87-7c20-89a4-e76883e94a0f'
  const pool = {
    query: async (sql: string) => {
      if (isMetricsQuery(sql)) {
        return {
          rows: [{ total: '0', active: '0', needs_attention: '0' }]
        }
      }
      if (sql.includes('WHERE ro.id = ANY($1::uuid[])')) {
        return { rows: [] }
      }
      throw new Error(`unexpected query: ${sql}`)
    }
  } as unknown as Pool
  const broker = new OrderCdcBroker(pool, testConfig)
  const brokerInternals = broker as unknown as {
    pendingRefreshOrRemoveOrders: Map<string, string | undefined>
    pendingAckLsn: string | undefined
    service: FakeReplicationService
    flushPendingOrders: () => Promise<void>
  }
  let acknowledgedLsn: string | undefined
  const removedOrderIds: string[] = []
  brokerInternals.pendingAckLsn = '0/36'
  brokerInternals.service = {
    acknowledge: async (lsn: string) => {
      acknowledgedLsn = lsn
      return true
    },
    destroy: async () => undefined
  }
  broker.subscribe((event) => {
    if (event.kind === 'remove') removedOrderIds.push(event.id)
  })

  try {
    brokerInternals.pendingRefreshOrRemoveOrders.set(orderId, undefined)
    await brokerInternals.flushPendingOrders()
    expect(brokerInternals.pendingRefreshOrRemoveOrders.has(orderId)).toBe(false)
    expect(removedOrderIds).toEqual([orderId])
    expect(acknowledgedLsn).toBe('0/36')
  } finally {
    await broker.close()
  }
})

test('OrderCdcBroker uses related-row delete timestamps as an upsert freshness floor', async () => {
  const orderId = '019df1c4-8d87-7c20-89a4-e76883e94a0f'
  const pool = createFlushablePool(orderId)
  const broker = new OrderCdcBroker(pool, testConfig)
  const brokerInternals = broker as unknown as {
    pendingRefreshOrRemoveOrders: Map<string, string | undefined>
    pendingAckLsn: string | undefined
    service: FakeReplicationService
    flushPendingOrders: () => Promise<void>
  }
  const events: { updatedAt: string }[] = []
  brokerInternals.pendingAckLsn = '0/39'
  brokerInternals.service = {
    acknowledge: async () => true,
    destroy: async () => undefined
  }
  broker.subscribe((event) => {
    if (event.kind === 'upsert') events.push({ updatedAt: event.order.updatedAt })
  })

  try {
    brokerInternals.pendingRefreshOrRemoveOrders.set(
      orderId,
      '2026-05-04T06:00:00.000Z'
    )
    await brokerInternals.flushPendingOrders()

    expect(events).toEqual([{ updatedAt: '2026-05-04T06:00:00.000Z' }])
  } finally {
    await broker.close()
  }
})

test('OrderCdcBroker best-effort listener failures do not block acknowledgements', async () => {
  const orderId = '019df1c4-8d87-7c20-89a4-e76883e94a0f'
  const pool = createFlushablePool(orderId)
  const broker = new OrderCdcBroker(pool, testConfig)
  const brokerInternals = broker as unknown as {
    pendingOrderIds: Set<string>
    pendingAckLsn: string | undefined
    service: FakeReplicationService
    flushPendingOrders: () => Promise<void>
  }
  let acknowledgedLsn: string | undefined
  brokerInternals.pendingAckLsn = '0/40'
  brokerInternals.service = {
    acknowledge: async (lsn: string) => {
      acknowledgedLsn = lsn
      return true
    },
    destroy: async () => undefined
  }
  const consoleError = console.error
  console.error = () => undefined
  broker.subscribe(() => {
    throw new Error('browser stream closed')
  })

  try {
    brokerInternals.pendingOrderIds.add(orderId)
    await brokerInternals.flushPendingOrders()
    expect(brokerInternals.pendingOrderIds.has(orderId)).toBe(false)
    expect(brokerInternals.pendingAckLsn).toBeUndefined()
    expect(acknowledgedLsn).toBe('0/40')
  } finally {
    console.error = consoleError
    await broker.close()
  }
})

test('OrderCdcBroker skips global metrics when subscribers opt out', async () => {
  const orderId = '019df1c4-8d87-7c20-89a4-e76883e94a0f'
  const pool = {
    query: async (sql: string) => {
      if (isMetricsQuery(sql)) {
        throw new Error('metrics query should not run')
      }
      if (sql.includes('WHERE ro.id = ANY($1::uuid[])')) {
        return { rows: [orderRow(orderId)] }
      }
      throw new Error(`unexpected query: ${sql}`)
    }
  } as unknown as Pool
  const broker = new OrderCdcBroker(pool, testConfig)
  const brokerInternals = broker as unknown as {
    pendingOrderIds: Set<string>
    pendingAckLsn: string | undefined
    service: FakeReplicationService
    flushPendingOrders: () => Promise<void>
  }
  let acknowledgedLsn: string | undefined
  const events: unknown[] = []
  brokerInternals.pendingAckLsn = '0/45'
  brokerInternals.service = {
    acknowledge: async (lsn: string) => {
      acknowledgedLsn = lsn
      return true
    },
    destroy: async () => undefined
  }
  broker.subscribe(
    (event) => {
      events.push(event)
    },
    { includeMetrics: false }
  )

  try {
    brokerInternals.pendingOrderIds.add(orderId)
    await brokerInternals.flushPendingOrders()
    expect(events).toHaveLength(1)
    expect(events[0]).toMatchObject({
      kind: 'upsert',
      order: { id: orderId },
      sort: 'created_at_desc'
    })
    expect((events[0] as { metrics?: unknown }).metrics).toBeUndefined()
    expect((events[0] as { total?: unknown }).total).toBeUndefined()
    expect(acknowledgedLsn).toBe('0/45')
  } finally {
    await broker.close()
  }
})

test('OrderCdcBroker updates derived stores before fetching custom metrics', async () => {
  const orderId = '019df1c4-8d87-7c20-89a4-e76883e94a0f'
  const pool = {
    query: async (sql: string) => {
      if (isMetricsQuery(sql)) {
        throw new Error('replica metrics query should not run')
      }
      if (sql.includes('WHERE ro.id = ANY($1::uuid[])')) {
        return { rows: [orderRow(orderId)] }
      }
      throw new Error(`unexpected query: ${sql}`)
    }
  } as unknown as Pool
  const calls: string[] = []
  let derivedTotal = 0
  const broker = new OrderCdcBroker(pool, testConfig, {
    beforeDispatch: async ({ upserts, removes }) => {
      calls.push('beforeDispatch')
      derivedTotal += upserts.length
      derivedTotal -= removes.length
    },
    fetchMetrics: async () => {
      calls.push('fetchMetrics')
      return {
        total: derivedTotal,
        active: derivedTotal,
        needsAttention: 0
      }
    }
  })
  const brokerInternals = broker as unknown as {
    pendingOrderIds: Set<string>
    pendingAckLsn: string | undefined
    service: FakeReplicationService
    flushPendingOrders: () => Promise<void>
  }
  const events: unknown[] = []
  brokerInternals.pendingAckLsn = '0/46'
  brokerInternals.service = {
    acknowledge: async () => true,
    destroy: async () => undefined
  }
  broker.subscribe((event) => {
    events.push(event)
  })

  try {
    brokerInternals.pendingOrderIds.add(orderId)
    await brokerInternals.flushPendingOrders()
    expect(calls).toEqual(['beforeDispatch', 'fetchMetrics'])
    expect(events[0]).toMatchObject({
      kind: 'upsert',
      order: { id: orderId },
      total: 1,
      metrics: {
        total: 1,
        active: 1,
        needsAttention: 0
      }
    })
  } finally {
    await broker.close()
  }
})

test('OrderCdcBroker acknowledges CDC batches without querying when there are no listeners', async () => {
  const orderId = '019df1c4-8d87-7c20-89a4-e76883e94a0f'
  let queryCount = 0
  const pool = {
    query: async () => {
      queryCount += 1
      throw new Error('no queries should run')
    }
  } as unknown as Pool
  const broker = new OrderCdcBroker(pool, testConfig)
  const brokerInternals = broker as unknown as {
    pendingOrderIds: Set<string>
    pendingAckLsn: string | undefined
    service: FakeReplicationService
    flushPendingOrders: () => Promise<void>
  }
  let acknowledgedLsn: string | undefined
  brokerInternals.pendingAckLsn = '0/47'
  brokerInternals.service = {
    acknowledge: async (lsn: string) => {
      acknowledgedLsn = lsn
      return true
    },
    destroy: async () => undefined
  }

  try {
    brokerInternals.pendingOrderIds.add(orderId)
    await brokerInternals.flushPendingOrders()
    expect(queryCount).toBe(0)
    expect(brokerInternals.pendingOrderIds.has(orderId)).toBe(false)
    expect(brokerInternals.pendingAckLsn).toBeUndefined()
    expect(acknowledgedLsn).toBe('0/47')
  } finally {
    await broker.close()
  }
})

test('OrderCdcBroker updates derived stores even when there are no SSE listeners', async () => {
  const orderId = '019df1c4-8d87-7c20-89a4-e76883e94a0f'
  const pool = {
    query: async (sql: string) => {
      if (isMetricsQuery(sql)) {
        throw new Error('metrics query should not run')
      }
      if (sql.includes('WHERE ro.id = ANY($1::uuid[])')) {
        return { rows: [orderRow(orderId)] }
      }
      throw new Error(`unexpected query: ${sql}`)
    }
  } as unknown as Pool
  const derivedChanges: string[] = []
  const broker = new OrderCdcBroker(pool, testConfig, {
    beforeDispatch: async ({ upserts, removes }) => {
      for (const order of upserts) derivedChanges.push(`upsert:${order.id}`)
      for (const order of removes) derivedChanges.push(`remove:${order.id}`)
    }
  })
  const brokerInternals = broker as unknown as {
    pendingOrderIds: Set<string>
    pendingAckLsn: string | undefined
    service: FakeReplicationService
    flushPendingOrders: () => Promise<void>
  }
  let acknowledgedLsn: string | undefined
  brokerInternals.pendingAckLsn = '0/48'
  brokerInternals.service = {
    acknowledge: async (lsn: string) => {
      acknowledgedLsn = lsn
      return true
    },
    destroy: async () => undefined
  }

  try {
    brokerInternals.pendingOrderIds.add(orderId)
    await brokerInternals.flushPendingOrders()
    expect(derivedChanges).toEqual([`upsert:${orderId}`])
    expect(brokerInternals.pendingOrderIds.has(orderId)).toBe(false)
    expect(brokerInternals.pendingAckLsn).toBeUndefined()
    expect(acknowledgedLsn).toBe('0/48')
  } finally {
    await broker.close()
  }
})

test('OrderCdcBroker batches large order summary fetches before acknowledgement', async () => {
  const orderIds = Array.from(
    { length: MAX_CDC_ORDER_FETCH_BATCH_SIZE + 1 },
    (_, index) => indexedUuid(index)
  )
  const fetchedBatches: string[][] = []
  const pool = {
    query: async (sql: string, params?: unknown[]) => {
      if (isMetricsQuery(sql)) {
        throw new Error('metrics query should not run')
      }
      if (sql.includes('WHERE ro.id = ANY($1::uuid[])')) {
        const ids = params?.[0]
        if (!Array.isArray(ids)) throw new Error('expected uuid array param')
        fetchedBatches.push(ids)
        return { rows: ids.map((id) => orderRow(String(id))) }
      }
      throw new Error(`unexpected query: ${sql}`)
    }
  } as unknown as Pool
  const broker = new OrderCdcBroker(pool, testConfig, {
    beforeDispatch: async () => undefined
  })
  const brokerInternals = broker as unknown as {
    pendingOrderIds: Set<string>
    pendingAckLsn: string | undefined
    service: FakeReplicationService
    flushPendingOrders: () => Promise<void>
  }
  let acknowledgedLsn: string | undefined
  brokerInternals.pendingAckLsn = '0/4a'
  brokerInternals.service = {
    acknowledge: async (lsn: string) => {
      acknowledgedLsn = lsn
      return true
    },
    destroy: async () => undefined
  }

  try {
    for (const orderId of orderIds) brokerInternals.pendingOrderIds.add(orderId)
    await brokerInternals.flushPendingOrders()
    expect(fetchedBatches.map((batch) => batch.length)).toEqual([
      MAX_CDC_ORDER_FETCH_BATCH_SIZE,
      1
    ])
    expect(brokerInternals.pendingOrderIds.size).toBe(0)
    expect(acknowledgedLsn).toBe('0/4a')
  } finally {
    await broker.close()
  }
})

test('OrderCdcBroker derived-store failures request backfill and withhold acknowledgement', async () => {
  const orderId = '019df1c4-8d87-7c20-89a4-e76883e94a0f'
  const pool = {
    query: async (sql: string) => {
      if (isMetricsQuery(sql)) {
        throw new Error('metrics query should not run')
      }
      if (sql.includes('WHERE ro.id = ANY($1::uuid[])')) {
        return { rows: [orderRow(orderId)] }
      }
      throw new Error(`unexpected query: ${sql}`)
    }
  } as unknown as Pool
  const broker = new OrderCdcBroker(pool, testConfig, {
    beforeDispatch: async () => {
      throw new Error('analytics database unavailable')
    }
  })
  const brokerInternals = broker as unknown as {
    pendingOrderIds: Set<string>
    pendingAckLsn: string | undefined
    service: FakeReplicationService
    flushPendingOrders: () => Promise<void>
  }
  let acknowledgedLsn: string | undefined
  let snapshotRequests = 0
  const unsubscribe = broker.subscribeSnapshotBackfillRequired(() => {
    snapshotRequests += 1
  })
  brokerInternals.pendingAckLsn = '0/49'
  brokerInternals.service = {
    acknowledge: async (lsn: string) => {
      acknowledgedLsn = lsn
      return true
    },
    destroy: async () => undefined
  }
  const consoleError = console.error
  console.error = () => undefined

  try {
    brokerInternals.pendingOrderIds.add(orderId)
    await brokerInternals.flushPendingOrders()
    expect(snapshotRequests).toBe(1)
    expect(brokerInternals.pendingOrderIds.has(orderId)).toBe(true)
    expect(brokerInternals.pendingAckLsn).toBe('0/49')
    expect(acknowledgedLsn).toBeUndefined()
  } finally {
    console.error = consoleError
    unsubscribe()
    await broker.close()
  }
})

test('OrderCdcBroker falls back to replica metrics when custom metrics fail', async () => {
  const orderId = '019df1c4-8d87-7c20-89a4-e76883e94a0f'
  const pool = createFlushablePool(orderId)
  const broker = new OrderCdcBroker(pool, testConfig, {
    fetchMetrics: async () => {
      throw new Error('analytics database unavailable')
    }
  })
  const brokerInternals = broker as unknown as {
    pendingOrderIds: Set<string>
    pendingAckLsn: string | undefined
    service: FakeReplicationService
    flushPendingOrders: () => Promise<void>
  }
  let acknowledgedLsn: string | undefined
  let snapshotRequests = 0
  const events: unknown[] = []
  const unsubscribeBackfill = broker.subscribeSnapshotBackfillRequired(() => {
    snapshotRequests += 1
  })
  broker.subscribe((event) => {
    events.push(event)
  })
  brokerInternals.pendingAckLsn = '0/51'
  brokerInternals.service = {
    acknowledge: async (lsn: string) => {
      acknowledgedLsn = lsn
      return true
    },
    destroy: async () => undefined
  }
  const consoleError = console.error
  console.error = () => undefined

  try {
    brokerInternals.pendingOrderIds.add(orderId)
    await brokerInternals.flushPendingOrders()
    expect(snapshotRequests).toBe(1)
    expect(events[0]).toMatchObject({
      kind: 'upsert',
      order: { id: orderId },
      total: 1,
      metrics: {
        total: 1,
        active: 0,
        needsAttention: 0
      }
    })
    expect(acknowledgedLsn).toBe('0/51')
  } finally {
    console.error = consoleError
    unsubscribeBackfill()
    await broker.close()
  }
})

test('OrderCdcBroker attaches analytics change results from the derived store', async () => {
  const changedOrderId = '019df1c4-8d87-7c20-89a4-e76883e94a0f'
  const unchangedOrderId = '019df1c4-8d87-7c20-89a4-e76883e94a10'
  const removedOrderId = '019df1c4-8d87-7c20-89a4-e76883e94a11'
  const pool = {
    query: async (sql: string) => {
      if (isMetricsQuery(sql)) {
        return {
          rows: [{ total: '3', active: '0', needs_attention: '0' }]
        }
      }
      if (sql.includes('WHERE ro.id = ANY($1::uuid[])')) {
        return { rows: [orderRow(changedOrderId), orderRow(unchangedOrderId)] }
      }
      throw new Error(`unexpected query: ${sql}`)
    }
  } as unknown as Pool
  const broker = new OrderCdcBroker(pool, testConfig, {
    beforeDispatch: async () => ({
      analyticsChangedOrderIds: [changedOrderId],
      analyticsChangedRemoveIds: [removedOrderId]
    })
  })
  const brokerInternals = broker as unknown as {
    pendingOrderIds: Set<string>
    pendingRemoveOrders: Map<string, string | undefined>
    pendingAckLsn: string | undefined
    service: FakeReplicationService
    flushPendingOrders: () => Promise<void>
  }
  const events: OrderCdcEvent[] = []
  let acknowledgedLsn: string | undefined
  broker.subscribe((event) => {
    events.push(event)
  })
  brokerInternals.pendingAckLsn = '0/52'
  brokerInternals.service = {
    acknowledge: async (lsn: string) => {
      acknowledgedLsn = lsn
      return true
    },
    destroy: async () => undefined
  }

  try {
    brokerInternals.pendingOrderIds.add(changedOrderId)
    brokerInternals.pendingOrderIds.add(unchangedOrderId)
    brokerInternals.pendingRemoveOrders.set(removedOrderId, undefined)
    await brokerInternals.flushPendingOrders()

    expect(events).toContainEqual(
      expect.objectContaining({
        kind: 'upsert',
        order: expect.objectContaining({ id: changedOrderId }),
        analyticsChanged: true
      })
    )
    const unchangedEvent = events.find(
      (event) => event.kind === 'upsert' && event.order.id === unchangedOrderId
    )
    expect(unchangedEvent).toBeDefined()
    expect(unchangedEvent?.analyticsChanged).toBeUndefined()
    expect(events).toContainEqual(
      expect.objectContaining({
        kind: 'remove',
        id: removedOrderId,
        analyticsChanged: true
      })
    )
    expect(acknowledgedLsn).toBe('0/52')
  } finally {
    await broker.close()
  }
})

test('OrderCdcBroker ack-required listener failures requeue and withhold acknowledgements', async () => {
  const orderId = '019df1c4-8d87-7c20-89a4-e76883e94a0f'
  const pool = createFlushablePool(orderId)
  const broker = new OrderCdcBroker(pool, testConfig)
  const brokerInternals = broker as unknown as {
    pendingOrderIds: Set<string>
    pendingAckLsn: string | undefined
    service: FakeReplicationService
    flushPendingOrders: () => Promise<void>
  }
  let acknowledgedLsn: string | undefined
  brokerInternals.pendingAckLsn = '0/50'
  brokerInternals.service = {
    acknowledge: async (lsn: string) => {
      acknowledgedLsn = lsn
      return true
    },
    destroy: async () => undefined
  }
  const consoleError = console.error
  console.error = () => undefined
  broker.subscribe(
    () => {
      throw new Error('analytics database unavailable')
    },
    { ackRequired: true }
  )

  try {
    brokerInternals.pendingOrderIds.add(orderId)
    await brokerInternals.flushPendingOrders()
    expect(brokerInternals.pendingOrderIds.has(orderId)).toBe(true)
    expect(brokerInternals.pendingAckLsn).toBe('0/50')
    expect(acknowledgedLsn).toBeUndefined()
  } finally {
    console.error = consoleError
    await broker.close()
  }
})

test('OrderCdcBroker retries remove-only batches after ack-required listener failure', async () => {
  const orderId = '019df1c4-8d87-7c20-89a4-e76883e94a0f'
  const pool = createMetricsOnlyPool()
  const broker = new OrderCdcBroker(pool, testConfig)
  const brokerInternals = broker as unknown as {
    pendingRemoveOrders: Map<string, string | undefined>
    pendingAckLsn: string | undefined
    service: FakeReplicationService
    flushPendingOrders: () => Promise<void>
    flushTimer: ReturnType<typeof setTimeout> | undefined
  }
  let acknowledgedLsn: string | undefined
  brokerInternals.pendingAckLsn = '0/55'
  brokerInternals.service = {
    acknowledge: async (lsn: string) => {
      acknowledgedLsn = lsn
      return true
    },
    destroy: async () => undefined
  }
  const consoleError = console.error
  console.error = () => undefined
  broker.subscribe(
    () => {
      throw new Error('analytics database unavailable')
    },
    { ackRequired: true }
  )

  try {
    brokerInternals.pendingRemoveOrders.set(orderId, undefined)
    await brokerInternals.flushPendingOrders()
    expect(brokerInternals.pendingRemoveOrders.has(orderId)).toBe(true)
    expect(brokerInternals.pendingAckLsn).toBe('0/55')
    expect(acknowledgedLsn).toBeUndefined()
    expect(brokerInternals.flushTimer).toBeDefined()
  } finally {
    console.error = consoleError
    await broker.close()
  }
})

test('OrderCdcBroker requests snapshot backfill when a started broker recreates its slot', async () => {
  const broker = new OrderCdcBroker({} as Pool, testConfig)
  const brokerInternals = broker as unknown as {
    ready: Promise<{ snapshotBackfillRequired: boolean }>
    getHealth: () => {
      startupResolved: boolean
      streaming: boolean
      lastStartedAt?: string
    }
    handleServiceStarted: (slotState: { created: boolean }) => void
  }
  let snapshotRequests = 0
  const unsubscribe = broker.subscribeSnapshotBackfillRequired(() => {
    snapshotRequests += 1
  })

  try {
    brokerInternals.handleServiceStarted({ created: false })
    await expect(brokerInternals.ready).resolves.toEqual({
      snapshotBackfillRequired: false
    })
    const health = brokerInternals.getHealth()
    expect(health.startupResolved).toBe(true)
    expect(health.streaming).toBe(true)
    expect(Date.parse(health.lastStartedAt ?? '')).toBeGreaterThan(0)
    expect(snapshotRequests).toBe(0)

    brokerInternals.handleServiceStarted({ created: false })
    await Promise.resolve()
    expect(snapshotRequests).toBe(0)

    brokerInternals.handleServiceStarted({ created: true })
    await Promise.resolve()
    expect(snapshotRequests).toBe(1)
  } finally {
    unsubscribe()
    await broker.close()
  }
})

test('OrderCdcBroker starts analytics backfill when initial CDC setup fails', async () => {
  const broker = new OrderCdcBroker({} as Pool, testConfig)
  const brokerInternals = broker as unknown as {
    ready: Promise<{ snapshotBackfillRequired: boolean }>
    handleServiceStarted: (slotState: { created: boolean }) => void
    resolveStartupWithSnapshotBackfill: () => void
  }
  let snapshotRequests = 0
  const unsubscribe = broker.subscribeSnapshotBackfillRequired(() => {
    snapshotRequests += 1
  })

  try {
    brokerInternals.resolveStartupWithSnapshotBackfill()
    await expect(brokerInternals.ready).resolves.toEqual({
      snapshotBackfillRequired: true
    })
    expect(snapshotRequests).toBe(0)

    brokerInternals.resolveStartupWithSnapshotBackfill()
    await Promise.resolve()
    expect(snapshotRequests).toBe(0)

    brokerInternals.handleServiceStarted({ created: true })
    await Promise.resolve()
    expect(snapshotRequests).toBe(1)
  } finally {
    unsubscribe()
    await broker.close()
  }
})

test('OrderCdcBroker tears down the active CDC service when the stream errors', async () => {
  const broker = new OrderCdcBroker({} as Pool, testConfig)
  const currentService = createFakeService()
  const staleService = createFakeService()
  const brokerInternals = broker as unknown as {
    service: FakeReplicationService | undefined
    getHealth: () => {
      streaming: boolean
      lastErrorAt?: string
    }
    handleServiceError: (service: FakeReplicationService, error: Error) => void
  }
  const consoleError = console.error
  console.error = () => undefined

  try {
    brokerInternals.service = currentService
    brokerInternals.handleServiceError(staleService, new Error('old stream'))
    await Promise.resolve()
    expect(staleService.destroyCalls).toBe(0)
    expect(brokerInternals.service).toBe(currentService)

    brokerInternals.handleServiceError(currentService, new Error('socket closed'))
    await Promise.resolve()
    expect(currentService.destroyCalls).toBe(1)
    expect(brokerInternals.service).toBeUndefined()
    const health = brokerInternals.getHealth()
    expect(health.streaming).toBe(false)
    expect(Date.parse(health.lastErrorAt ?? '')).toBeGreaterThan(0)
  } finally {
    console.error = consoleError
    await broker.close()
  }
})

test('OrderCdcBroker close unblocks pending CDC reconnect backoff', async () => {
  const broker = new OrderCdcBroker({} as Pool, testConfig)
  const brokerInternals = broker as unknown as {
    sleepForReconnectDelay: (ms: number) => Promise<void>
  }

  const sleeping = brokerInternals
    .sleepForReconnectDelay(30_000)
    .then(() => 'closed' as const)
  await broker.close()

  await expect(
    Promise.race([sleeping, timeoutResult(50, 'timeout' as const)])
  ).resolves.toBe('closed')
})

test('ensureCdcSlot recreates inactive lost CDC slots', async () => {
  const queries: string[] = []
  const pool = createCdcSlotPool({
    slot: {
      plugin: 'pgoutput',
      database: 'router',
      current_database: 'router'
    },
    healthColumns: ['active', 'wal_status', 'invalidation_reason'],
    health: {
      active: false,
      wal_status: 'lost',
      invalidation_reason: null
    },
    queries
  })
  const consoleError = console.error
  console.error = () => undefined

  try {
    await expect(ensureCdcSlot(pool, testConfig.cdcSlotName)).resolves.toEqual({
      created: true
    })
    const dropIndex = queries.findIndex((sql) =>
      sql.includes('pg_drop_replication_slot')
    )
    const createIndex = queries.findIndex((sql) =>
      sql.includes('pg_create_logical_replication_slot')
    )
    expect(dropIndex).toBeGreaterThanOrEqual(0)
    expect(createIndex).toBeGreaterThan(dropIndex)
  } finally {
    console.error = consoleError
  }
})

test('ensureCdcSlot rejects active invalid CDC slots instead of racing another consumer', async () => {
  const pool = createCdcSlotPool({
    slot: {
      plugin: 'pgoutput',
      database: 'router',
      current_database: 'router'
    },
    healthColumns: ['active', 'wal_status', 'invalidation_reason'],
    health: {
      active: true,
      wal_status: 'reserved',
      invalidation_reason: 'rows_removed'
    }
  })

  await expect(ensureCdcSlot(pool, testConfig.cdcSlotName)).rejects.toThrow(
    /invalid \(invalidation_reason=rows_removed\) but active/
  )
})

test('ensureCdcSlot tolerates older Postgres catalogs without optional slot-health columns', async () => {
  const queries: string[] = []
  const pool = createCdcSlotPool({
    slot: {
      plugin: 'pgoutput',
      database: 'router',
      current_database: 'router'
    },
    healthColumns: [],
    health: {
      active: null,
      wal_status: null,
      invalidation_reason: null
    },
    queries
  })

  await expect(ensureCdcSlot(pool, testConfig.cdcSlotName)).resolves.toEqual({
    created: false
  })
  expect(
    queries.some((sql) => sql.includes('pg_create_logical_replication_slot'))
  ).toBe(false)
})

type FakeReplicationService = {
  acknowledge: (lsn: string, ping?: boolean) => Promise<boolean>
  destroy: () => Promise<void>
  destroyCalls?: number
}

function createFakeService(): FakeReplicationService & { destroyCalls: number } {
  return {
    destroyCalls: 0,
    acknowledge: async () => true,
    destroy: async function () {
      this.destroyCalls += 1
    }
  }
}

function createCdcSlotPool({
  slot,
  healthColumns,
  health,
  queries
}: {
  slot:
    | {
        plugin: string | null
        database: string | null
        current_database: string
      }
    | undefined
  healthColumns: string[]
  health: {
    active: boolean | null
    wal_status: string | null
    invalidation_reason: string | null
  }
  queries?: string[]
}): Pool {
  return {
    query: async (sql: string) => {
      queries?.push(sql)
      if (
        sql.includes('FROM pg_catalog.pg_replication_slots') &&
        sql.includes('current_database()')
      ) {
        return { rows: slot ? [slot] : [] }
      }
      if (sql.includes('FROM pg_catalog.pg_attribute')) {
        return { rows: healthColumns.map((attname) => ({ attname })) }
      }
      if (
        sql.includes('FROM pg_catalog.pg_replication_slots') &&
        sql.includes('wal_status')
      ) {
        return { rows: [health] }
      }
      if (sql.includes('pg_drop_replication_slot')) {
        return { rows: [] }
      }
      if (sql.includes('pg_create_logical_replication_slot')) {
        return { rows: [{ slot_name: testConfig.cdcSlotName }] }
      }
      throw new Error(`unexpected query: ${sql}`)
    }
  } as unknown as Pool
}

function isMetricsQuery(sql: string) {
  return sql.includes('COUNT(*) FILTER') && sql.includes('FROM public.router_orders')
}

function createMetricsOnlyPool(): Pool {
  return {
    query: async (sql: string) => {
      if (isMetricsQuery(sql)) {
        return {
          rows: [{ total: '0', active: '0', needs_attention: '0' }]
        }
      }
      throw new Error(`unexpected query: ${sql}`)
    }
  } as unknown as Pool
}

function createFlushablePool(orderId: string): Pool {
  return {
    query: async (sql: string) => {
      if (isMetricsQuery(sql)) {
        return {
          rows: [{ total: '1', active: '0', needs_attention: '0' }]
        }
      }
      if (sql.includes('WHERE ro.id = ANY($1::uuid[])')) {
        return { rows: [orderRow(orderId)] }
      }
      throw new Error(`unexpected query: ${sql}`)
    }
  } as unknown as Pool
}

function timeoutResult<T>(ms: number, value: T): Promise<T> {
  return new Promise((resolve) => setTimeout(() => resolve(value), ms))
}

function routerCdcMessage(payload: Record<string, unknown>) {
  return {
    tag: 'message',
    prefix: testConfig.cdcMessagePrefix,
    content: new TextEncoder().encode(
      JSON.stringify({
        version: 1,
        schema: 'public',
        ...payload
      })
    )
  }
}

function indexedUuid(index: number) {
  return `019df1c4-8d87-7c20-89a4-${index.toString(16).padStart(12, '0')}`
}

function orderRow(id: string) {
  const now = new Date('2026-05-04T00:00:00.000Z')
  return {
    id,
    order_type: 'market_order',
    status: 'completed',
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
    execution_steps: [],
    provider_operations: [],
    funding_tx_hash: null
  }
}

const testConfig: AdminDashboardConfig = {
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
  missingAuthConfig: []
}
