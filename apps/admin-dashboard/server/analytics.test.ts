import { afterAll, describe, expect, test } from 'bun:test'
import { Client } from 'pg'
import type { Pool } from 'pg'

import {
  addSafeMetricCount,
  completedOrderContribution,
  isPositiveRawAmount,
  VolumeAnalyticsRuntime
} from './analytics'
import type { OrderFirehoseRow } from './orders'

const databaseUrl = process.env.ADMIN_DASHBOARD_ANALYTICS_TEST_DATABASE_URL

const describeWithDatabase = databaseUrl ? describe : describe.skip

test('checked metric addition rejects unsafe aggregate counts', () => {
  expect(addSafeMetricCount(1, 2, 'metric')).toBe(3)
  expect(() =>
    addSafeMetricCount(Number.MAX_SAFE_INTEGER, 1, 'metric')
  ).toThrow('metric exceeds JavaScript safe integer range')
})

test('analytics raw amount parsing rejects oversized decimal strings before BigInt', () => {
  expect(isPositiveRawAmount('1')).toBe(true)
  expect(isPositiveRawAmount('0')).toBe(false)
  expect(isPositiveRawAmount('9'.repeat(10_000))).toBe(false)
})

test('completed order volume contribution rejects malformed exact USD valuation', () => {
  const order = completedOrder({
    rawAmount: '1000000',
    amountUsdMicro: '1000000',
    completedAt: '2026-05-04T06:00:12.345Z'
  })

  order.executionLegs[0].usdValuation = {
    amounts: {
      actualInput: {
        asset: {
          chainId: 'evm:1',
          assetId: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
        },
        raw: '1000000',
        decimals: 6,
        unitUsdMicro: '1000000',
        amountUsdMicro: '9'.repeat(10_000)
      }
    }
  }

  expect(completedOrderContribution(order)).toBeUndefined()
})

test('completed order volume contribution bounds fallback valuation fields', () => {
  const order = completedOrder({
    rawAmount: '1000000',
    amountUsdMicro: '1000000',
    completedAt: '2026-05-04T06:00:12.345Z'
  })

  order.executionLegs[0].usdValuation = {
    amounts: {
      actualInput: {
        asset: {
          chainId: 'evm:1',
          assetId: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
        },
        raw: '1000000',
        decimals: 6,
        unitUsdMicro: '9'.repeat(10_000)
      }
    }
  }

  expect(completedOrderContribution(order)).toBeUndefined()
})

test('completed order volume contribution requires actual input amount evidence', () => {
  const order = completedOrder({
    rawAmount: '1000000',
    amountUsdMicro: '1000000',
    completedAt: '2026-05-04T06:00:12.345Z'
  })
  delete order.executionLegs[0].actualAmountIn

  expect(completedOrderContribution(order)).toBeUndefined()
})

test('completed order volume contribution falls back to planned input unit pricing', () => {
  const order = completedOrder({
    rawAmount: '999999',
    amountUsdMicro: '1000000',
    completedAt: '2026-05-04T06:00:12.345Z'
  })
  order.executionLegs[0].usdValuation = {
    amounts: {
      plannedInput: {
        asset: {
          chainId: 'evm:1',
          assetId: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
        },
        raw: '1000000',
        decimals: 6,
        unitUsdMicro: '1000000',
        amountUsdMicro: '1000000'
      }
    }
  }

  expect(completedOrderContribution(order)).toMatchObject({
    volumeUsdMicro: 999999n
  })
})

test('analytics runtime close wakes a sleeping backfill retry loop', async () => {
  const runtime = Object.create(VolumeAnalyticsRuntime.prototype) as {
    closed: boolean
    pool: { end(): Promise<void> }
    backfillRetryTimer?: ReturnType<typeof setTimeout>
    resolveBackfillRetrySleep?: () => void
    close(): Promise<void>
    sleepForBackfillRetry(ms: number): Promise<void>
  }
  runtime.closed = false
  runtime.pool = { end: async () => undefined }

  const sleeping = runtime.sleepForBackfillRetry(10_000)
  await eventually(async () => {
    expect(typeof runtime.resolveBackfillRetrySleep).toBe('function')
  })

  const closeStartedAt = Date.now()
  await runtime.close()
  await sleeping
  expect(Date.now() - closeStartedAt).toBeLessThan(100)
})

test('analytics startBackfill returns a promise covering queued backfill work', async () => {
  const releaseFirstRun = deferred<void>()
  const calls: Array<{ pool: string; fullSnapshot: boolean | undefined }> = []
  const runtime = Object.create(VolumeAnalyticsRuntime.prototype) as {
    closed: boolean
    backfillRunning: boolean
    pendingBackfill?: unknown
    backfillRunPromise?: Promise<void>
    startBackfill(replicaPool: Pool, options?: { fullSnapshot?: boolean }): Promise<void>
    runBackfill(
      replicaPool: Pool,
      options: { fullSnapshot?: boolean }
    ): Promise<void>
  }
  runtime.closed = false
  runtime.backfillRunning = false
  runtime.runBackfill = async (replicaPool, options) => {
    calls.push({
      pool: (replicaPool as unknown as { name: string }).name,
      fullSnapshot: options.fullSnapshot
    })
    if (calls.length === 1) {
      await releaseFirstRun.promise
    }
  }

  const first = runtime.startBackfill(
    { name: 'first' } as unknown as Pool,
    { fullSnapshot: false }
  )
  const second = runtime.startBackfill(
    { name: 'second' } as unknown as Pool,
    { fullSnapshot: true }
  )

  expect(second).toBe(first)
  releaseFirstRun.resolve()
  await first

  expect(calls).toEqual([
    { pool: 'first', fullSnapshot: false },
    { pool: 'second', fullSnapshot: true }
  ])
  expect(runtime.backfillRunning).toBe(false)
  expect(runtime.backfillRunPromise).toBeUndefined()
})

test('analytics backfill retry does not rerun a phase that already completed', async () => {
  const runtime = Object.create(VolumeAnalyticsRuntime.prototype) as {
    ready: Promise<void>
    closed: boolean
    runBackfill(replicaPool: Pool, options: { fullSnapshot?: boolean }): Promise<void>
    backfillVolumeOnce(
      replicaPool: Pool,
      options: { fullSnapshot?: boolean }
    ): Promise<void>
    backfillStatusOnce(
      replicaPool: Pool,
      options: { fullSnapshot?: boolean }
    ): Promise<void>
    sleepForBackfillRetry(ms: number): Promise<void>
  }
  runtime.ready = Promise.resolve()
  runtime.closed = false
  let volumeCalls = 0
  let statusCalls = 0
  runtime.backfillVolumeOnce = async () => {
    volumeCalls += 1
  }
  runtime.backfillStatusOnce = async () => {
    statusCalls += 1
    if (statusCalls === 1) throw new Error('status backfill failed once')
  }
  runtime.sleepForBackfillRetry = async () => undefined

  await runtime.runBackfill({} as Pool, { fullSnapshot: true })

  expect(volumeCalls).toBe(1)
  expect(statusCalls).toBe(2)
})

describeWithDatabase('VolumeAnalyticsRuntime', () => {
  const databases: string[] = []

  afterAll(async () => {
    if (!databaseUrl) return
    const admin = new Client({ connectionString: adminDatabaseUrl(databaseUrl) })
    await admin.connect()
    try {
      for (const database of databases.reverse()) {
        await admin.query(
          `DROP DATABASE IF EXISTS ${quoteIdentifier(database)} WITH (FORCE)`
        )
      }
    } finally {
      await admin.end()
    }
  })

  test('keeps bucket aggregates idempotent across duplicate, changed, and removed contributions', async () => {
    const testDatabaseUrl = await createTestDatabase(databaseUrl!)
    const runtime = new VolumeAnalyticsRuntime(testDatabaseUrl)
    try {
      const order = completedOrder({
        rawAmount: '100000000',
        amountUsdMicro: '100000000',
        completedAt: '2026-05-04T06:00:12.345Z'
      })

      await runtime.ingestOrders([order, order])
      await expectHourBucket(runtime, {
        volumeUsdMicro: '100000000',
        orderCount: 1
      })

      await runtime.ingestOrders([
        completedOrder({
          rawAmount: '200000000',
          amountUsdMicro: '200000000',
          completedAt: '2026-05-04T06:00:12.345Z'
        })
      ])
      await expectHourBucket(runtime, {
        volumeUsdMicro: '200000000',
        orderCount: 1
      })

      await runtime.ingestOrder({ ...order, status: 'failed' })
      const buckets = await runtime.fetchVolumeBuckets({
        bucketSize: 'hour',
        orderType: 'all',
        from: new Date('2026-05-04T00:00:00.000Z'),
        to: new Date('2026-05-05T00:00:00.000Z')
      })
      expect(buckets).toEqual([])

      await runtime.ingestOrder(order)
      await runtime.removeOrder(order.id)
      const bucketsAfterDelete = await runtime.fetchVolumeBuckets({
        bucketSize: 'hour',
        orderType: 'all',
        from: new Date('2026-05-04T00:00:00.000Z'),
        to: new Date('2026-05-05T00:00:00.000Z')
      })
      expect(bucketsAfterDelete).toEqual([])
    } finally {
      await runtime.close()
    }
  })

  test('keeps order status metrics idempotent across updates and deletes', async () => {
    const testDatabaseUrl = await createTestDatabase(databaseUrl!)
    const runtime = new VolumeAnalyticsRuntime(testDatabaseUrl)
    try {
      const baseOrder = completedOrder({
        rawAmount: '100000000',
        amountUsdMicro: '100000000',
        completedAt: '2026-05-04T06:00:12.345Z'
      })

      await runtime.ingestOrder({
        ...baseOrder,
        status: 'pending_funding',
        updatedAt: '2026-05-04T06:00:00.000Z'
      })
      await expectOrderMetrics(runtime, {
        total: 1,
        active: 1,
        needsAttention: 0
      })

      await runtime.ingestOrder({
        ...baseOrder,
        status: 'completed',
        updatedAt: '2026-05-04T06:01:00.000Z'
      })
      await expectOrderMetrics(runtime, {
        total: 1,
        active: 0,
        needsAttention: 0
      })

      await runtime.ingestOrder({
        ...baseOrder,
        status: 'manual_intervention_required',
        updatedAt: '2026-05-04T06:02:00.000Z'
      })
      await expectOrderMetrics(runtime, {
        total: 1,
        active: 0,
        needsAttention: 1
      })

      await runtime.ingestOrder({
        ...baseOrder,
        status: 'failed',
        updatedAt: '2026-05-04T06:00:30.000Z'
      })
      await expectOrderMetrics(runtime, {
        total: 1,
        active: 0,
        needsAttention: 1
      })

      await runtime.removeOrder(baseOrder.id, '2026-05-04T06:03:00.000Z')
      await expectOrderMetrics(runtime, {
        total: 0,
        active: 0,
        needsAttention: 0
      })

      await runtime.ingestOrder({
        ...baseOrder,
        status: 'manual_intervention_required',
        updatedAt: '2026-05-04T06:02:30.000Z'
      })
      await expectOrderMetrics(runtime, {
        total: 0,
        active: 0,
        needsAttention: 0
      })
    } finally {
      await runtime.close()
    }
  })

  test('migrates analytics indexes for bucket and snapshot pruning paths', async () => {
    const testDatabaseUrl = await createTestDatabase(databaseUrl!)
    const runtime = new VolumeAnalyticsRuntime(testDatabaseUrl)
    const client = new Client({ connectionString: testDatabaseUrl })
    try {
      await runtime.fetchOrderMetrics()
      await client.connect()
      const result = await client.query<{ indexname: string }>(`
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND tablename LIKE 'admin_%'
      `)
      const indexes = new Set(result.rows.map((row) => row.indexname))

      expect(indexes.has('idx_admin_volume_buckets_lookup')).toBe(false)
      expect(indexes.has('idx_admin_volume_buckets_type_lookup')).toBe(true)
      expect(
        indexes.has('idx_admin_volume_order_contributions_updated_at')
      ).toBe(true)
      expect(
        indexes.has('idx_admin_order_status_ingestion_state_stale_present')
      ).toBe(true)

      const columns = await client.query<{ data_type: string }>(`
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'admin_volume_buckets'
          AND column_name = 'order_count'
      `)
      expect(columns.rows[0]?.data_type).toBe('bigint')
    } finally {
      await client.end().catch(() => undefined)
      await runtime.close()
    }
  })

  test('ignores stale completed-order snapshots after newer CDC state', async () => {
    const testDatabaseUrl = await createTestDatabase(databaseUrl!)
    const runtime = new VolumeAnalyticsRuntime(testDatabaseUrl)
    try {
      const newer = completedOrder({
        rawAmount: '2000000',
        amountUsdMicro: '2000000',
        completedAt: '2026-05-04T06:00:12.345Z'
      })
      newer.updatedAt = '2026-05-04T06:01:00.000Z'
      const stale = completedOrder({
        rawAmount: '1000000',
        amountUsdMicro: '1000000',
        completedAt: '2026-05-04T06:00:12.345Z'
      })
      stale.updatedAt = '2026-05-04T06:00:00.000Z'

      await runtime.ingestOrder(newer)
      await runtime.ingestOrder(stale)

      await expectHourBucket(runtime, {
        volumeUsdMicro: '2000000',
        orderCount: 1
      })

      await runtime.ingestOrder({ ...stale, status: 'failed' })
      await expectHourBucket(runtime, {
        volumeUsdMicro: '2000000',
        orderCount: 1
      })
    } finally {
      await runtime.close()
    }
  })

  test('delete tombstones block stale backfill reinsertion', async () => {
    const testDatabaseUrl = await createTestDatabase(databaseUrl!)
    const runtime = new VolumeAnalyticsRuntime(testDatabaseUrl)
    try {
      const stale = completedOrder({
        rawAmount: '1000000',
        amountUsdMicro: '1000000',
        completedAt: '2020-01-01T06:00:12.345Z'
      })

      await runtime.removeOrder(stale.id, '2020-01-01T06:01:00.000Z')
      await runtime.ingestOrder(stale)

      const buckets = await runtime.fetchVolumeBuckets({
        bucketSize: 'hour',
        orderType: 'all',
        from: new Date('2020-01-01T00:00:00.000Z'),
        to: new Date('2020-01-02T00:00:00.000Z')
      })
      expect(buckets).toEqual([])
    } finally {
      await runtime.close()
    }
  })

  test('delete tombstones win equal source timestamp ties', async () => {
    const testDatabaseUrl = await createTestDatabase(databaseUrl!)
    const runtime = new VolumeAnalyticsRuntime(testDatabaseUrl)
    try {
      const stale = completedOrder({
        rawAmount: '1000000',
        amountUsdMicro: '1000000',
        completedAt: '2020-01-01T06:00:12.345Z'
      })
      stale.updatedAt = '2020-01-01T06:01:00.000Z'

      await runtime.removeOrder(stale.id, stale.updatedAt)
      await runtime.ingestOrder(stale)

      const buckets = await runtime.fetchVolumeBuckets({
        bucketSize: 'hour',
        orderType: 'all',
        from: new Date('2020-01-01T00:00:00.000Z'),
        to: new Date('2020-01-02T00:00:00.000Z')
      })
      expect(buckets).toEqual([])
    } finally {
      await runtime.close()
    }
  })

  test('persists completed-order backfill cursor and completion state', async () => {
    const testDatabaseUrl = await createTestDatabase(databaseUrl!)
    const runtime = new VolumeAnalyticsRuntime(testDatabaseUrl)
    const internals = runtime as unknown as BackfillStateInternals
    try {
      await expect(internals.readBackfillState()).resolves.toBeUndefined()

      const cursor = {
        createdAt: '2026-05-04T05:59:00.000Z',
        id: '019df1c4-8d87-7c20-89a4-e76883e94a0f'
      }
      await internals.saveBackfillCursor(cursor)
      await expect(internals.readBackfillState()).resolves.toMatchObject({
        completed: false,
        cursor_order_id: cursor.id
      })

      await internals.markBackfillComplete(cursor)
      await expect(internals.readBackfillState()).resolves.toMatchObject({
        completed: true,
        cursor_order_id: cursor.id
      })
    } finally {
      await runtime.close()
    }
  })

  test('ignores completed volume when actual input is zero', async () => {
    const testDatabaseUrl = await createTestDatabase(databaseUrl!)
    const runtime = new VolumeAnalyticsRuntime(testDatabaseUrl)
    try {
      await runtime.ingestOrder(
        completedOrder({
          rawAmount: '235958',
          actualAmountIn: '0',
          amountUsdMicro: '235958000',
          completedAt: '2026-05-04T06:00:12.345Z'
        })
      )
      const buckets = await runtime.fetchVolumeBuckets({
        bucketSize: 'hour',
        orderType: 'all',
        from: new Date('2026-05-04T00:00:00.000Z'),
        to: new Date('2026-05-05T00:00:00.000Z')
      })
      expect(buckets).toEqual([])
    } finally {
      await runtime.close()
    }
  })

  test('ignores malformed valuation decimals instead of crashing ingestion', async () => {
    const testDatabaseUrl = await createTestDatabase(databaseUrl!)
    const runtime = new VolumeAnalyticsRuntime(testDatabaseUrl)
    try {
      const order = completedOrder({
        rawAmount: '1000000',
        amountUsdMicro: '1000000',
        completedAt: '2026-05-04T06:00:12.345Z'
      })
      order.executionLegs[0].usdValuation = {
        amounts: {
          actualInput: {
            asset: {
              chainId: 'evm:1',
              assetId: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
            },
            raw: '1000000',
            decimals: -1,
            unitUsdMicro: '1000000'
          }
        }
      }

      await runtime.ingestOrder(order)
      const buckets = await runtime.fetchVolumeBuckets({
        bucketSize: 'hour',
        orderType: 'all',
        from: new Date('2026-05-04T00:00:00.000Z'),
        to: new Date('2026-05-05T00:00:00.000Z')
      })
      expect(buckets).toEqual([])
    } finally {
      await runtime.close()
    }
  })

  test('backfills completed orders through the real cursor loop', async () => {
    const testDatabaseUrl = await createTestDatabase(databaseUrl!)
    const runtime = new VolumeAnalyticsRuntime(testDatabaseUrl)
    const internals = runtime as unknown as BackfillStateInternals
    const rows = Array.from({ length: 251 }, (_, index) =>
      completedOrderDbRow({
        id: indexedUuid(index),
        createdAt: new Date(Date.UTC(2026, 4, 4, 8, 0, 0) - index * 1000),
        completedAt: new Date(Date.UTC(2026, 4, 4, 9, 0, 0) + index * 1000),
        amountUsdMicro: '1000000'
      })
    )
    try {
      await internals.backfillOnce(completedOrdersReplicaPool(rows))

      const buckets = await runtime.fetchVolumeBuckets({
        bucketSize: 'hour',
        orderType: 'all',
        from: new Date('2026-05-04T00:00:00.000Z'),
        to: new Date('2026-05-05T00:00:00.000Z')
      })
      expect(buckets).toEqual([
        {
          bucketStart: '2026-05-04T09:00:00.000Z',
          bucketSize: 'hour',
          orderType: 'all',
          volumeUsdMicro: '251000000',
          orderCount: 251
        }
      ])
      await expect(internals.readBackfillState()).resolves.toMatchObject({
        completed: true,
        cursor_order_id: indexedUuid(250)
      })
    } finally {
      await runtime.close()
    }
  })

  test('startup backfill reconciles even after a prior completed state', async () => {
    const testDatabaseUrl = await createTestDatabase(databaseUrl!)
    const runtime = new VolumeAnalyticsRuntime(testDatabaseUrl)
    const internals = runtime as unknown as BackfillStateInternals
    const orderId = indexedUuid(900)
    try {
      await internals.markBackfillComplete({
        createdAt: '2026-05-04T05:59:00.000Z',
        id: indexedUuid(1)
      })

      await internals.backfillOnce(
        completedOrdersReplicaPool([
          completedOrderDbRow({
            id: orderId,
            createdAt: new Date('2026-05-04T10:00:00.000Z'),
            completedAt: new Date('2026-05-04T10:01:00.000Z'),
            amountUsdMicro: '42000000'
          })
        ])
      )

      const buckets = await runtime.fetchVolumeBuckets({
        bucketSize: 'hour',
        orderType: 'all',
        from: new Date('2026-05-04T00:00:00.000Z'),
        to: new Date('2026-05-05T00:00:00.000Z')
      })
      expect(buckets).toEqual([
        {
          bucketStart: '2026-05-04T10:00:00.000Z',
          bucketSize: 'hour',
          orderType: 'all',
          volumeUsdMicro: '42000000',
          orderCount: 1
        }
      ])
    } finally {
      await runtime.close()
    }
  })

  test('snapshot backfill keeps unchanged source contributions while pruning stale rows', async () => {
    const testDatabaseUrl = await createTestDatabase(databaseUrl!)
    const runtime = new VolumeAnalyticsRuntime(testDatabaseUrl)
    const internals = runtime as unknown as BackfillStateInternals
    const currentOrder = completedOrder({
      rawAmount: '1000000',
      amountUsdMicro: '1000000',
      completedAt: '2026-05-04T06:00:12.345Z'
    })
    const staleOrder = completedOrder({
      rawAmount: '2000000',
      amountUsdMicro: '2000000',
      completedAt: '2026-05-04T06:10:00.000Z'
    })
    const currentOrderId = currentOrder.id
    const staleOrderId = indexedUuid(777)

    try {
      await runtime.ingestOrders([
        currentOrder,
        {
          ...staleOrder,
          id: staleOrderId
        }
      ])
      await expectHourBucket(runtime, {
        volumeUsdMicro: '3000000',
        orderCount: 2
      })

      await internals.backfillOnce(
        completedOrdersReplicaPool([
          completedOrderDbRow({
            id: currentOrderId,
            createdAt: new Date(currentOrder.createdAt),
            completedAt: new Date('2026-05-04T06:00:12.345Z'),
            amountUsdMicro: '1000000'
          })
        ])
      )

      await expectHourBucket(runtime, {
        volumeUsdMicro: '1000000',
        orderCount: 1
      })
    } finally {
      await runtime.close()
    }
  })

  test('snapshot backfill prunes stale volume and status rows in bounded batches', async () => {
    const testDatabaseUrl = await createTestDatabase(databaseUrl!)
    const runtime = new VolumeAnalyticsRuntime(testDatabaseUrl)
    const staleOrders = Array.from({ length: 1001 }, (_, index) => {
      const order = completedOrder({
        rawAmount: '1000000',
        amountUsdMicro: '1000000',
        completedAt: '2026-05-04T06:01:00.000Z'
      })
      order.id = indexedUuid(8000 + index)
      order.createdAt = '2026-05-04T05:00:00.000Z'
      order.updatedAt = '2026-05-04T06:01:00.000Z'
      return order
    })
    const currentDbOrder = completedOrderDbRow({
      id: indexedUuid(9500),
      createdAt: new Date('2026-05-04T06:30:00.000Z'),
      completedAt: new Date('2026-05-04T06:31:00.000Z'),
      amountUsdMicro: '2000000'
    })

    try {
      await runtime.ingestOrders(staleOrders)
      await expectHourBucket(runtime, {
        volumeUsdMicro: '1001000000',
        orderCount: 1001
      })
      await expectOrderMetrics(runtime, {
        total: 1001,
        active: 0,
        needsAttention: 0
      })

      runtime.startBackfill(completedOrdersReplicaPool([currentDbOrder]))

      await eventually(async () => {
        await expectHourBucket(runtime, {
          volumeUsdMicro: '2000000',
          orderCount: 1
        })
        await expectOrderMetrics(runtime, {
          total: 1,
          active: 0,
          needsAttention: 0
        })
      })
    } finally {
      await runtime.close()
    }
  })

  test('snapshot prune tombstones block later stale contribution and status reinsertion', async () => {
    const testDatabaseUrl = await createTestDatabase(databaseUrl!)
    const runtime = new VolumeAnalyticsRuntime(testDatabaseUrl)
    const staleOrder = completedOrder({
      rawAmount: '1000000',
      amountUsdMicro: '1000000',
      completedAt: '2026-05-04T06:01:00.000Z'
    })
    const staleOrderId = indexedUuid(9901)
    staleOrder.id = staleOrderId

    try {
      await runtime.ingestOrder(staleOrder)
      await expectHourBucket(runtime, {
        volumeUsdMicro: '1000000',
        orderCount: 1
      })
      await expectOrderMetrics(runtime, {
        total: 1,
        active: 0,
        needsAttention: 0
      })

      runtime.startBackfill(completedOrdersReplicaPool([]))

      await eventually(async () => {
        await expect(runtime.fetchVolumeBuckets({
          bucketSize: 'hour',
          orderType: 'all',
          from: new Date('2026-05-04T00:00:00.000Z'),
          to: new Date('2026-05-05T00:00:00.000Z')
        })).resolves.toEqual([])
        await expectOrderMetrics(runtime, {
          total: 0,
          active: 0,
          needsAttention: 0
        })
      })

      await runtime.ingestOrder({
        ...staleOrder,
        updatedAt: '2026-05-04T06:30:00.000Z',
        executionLegs: staleOrder.executionLegs.map((leg) => ({
          ...leg,
          updatedAt: '2026-05-04T06:30:00.000Z',
          completedAt: '2026-05-04T06:30:00.000Z'
        }))
      })

      await expect(runtime.fetchVolumeBuckets({
        bucketSize: 'hour',
        orderType: 'all',
        from: new Date('2026-05-04T00:00:00.000Z'),
        to: new Date('2026-05-05T00:00:00.000Z')
      })).resolves.toEqual([])
      await expectOrderMetrics(runtime, {
        total: 0,
        active: 0,
        needsAttention: 0
      })
    } finally {
      await runtime.close()
    }
  })

  test('snapshot backfill keeps newer dominated CDC state while pruning stale rows', async () => {
    const testDatabaseUrl = await createTestDatabase(databaseUrl!)
    const runtime = new VolumeAnalyticsRuntime(testDatabaseUrl)
    const internals = runtime as unknown as BackfillStateInternals
    const order = completedOrder({
      rawAmount: '1000000',
      amountUsdMicro: '1000000',
      completedAt: '2026-05-04T06:02:00.000Z'
    })

    try {
      await runtime.ingestOrder(order)
      await expectHourBucket(runtime, {
        volumeUsdMicro: '1000000',
        orderCount: 1
      })
      await expectOrderMetrics(runtime, {
        total: 1,
        active: 0,
        needsAttention: 0
      })

      await internals.backfillOnce(
        completedOrdersReplicaPool([
          completedOrderDbRow({
            id: order.id,
            createdAt: new Date(order.createdAt),
            completedAt: new Date('2026-05-04T06:01:00.000Z'),
            amountUsdMicro: '1000000'
          })
        ])
      )

      await expectHourBucket(runtime, {
        volumeUsdMicro: '1000000',
        orderCount: 1
      })
      await expectOrderMetrics(runtime, {
        total: 1,
        active: 0,
        needsAttention: 0
      })
    } finally {
      await runtime.close()
    }
  })

  test('incremental backfill skips replica scans after a completed snapshot', async () => {
    const testDatabaseUrl = await createTestDatabase(databaseUrl!)
    const runtime = new VolumeAnalyticsRuntime(testDatabaseUrl)
    const internals = runtime as unknown as BackfillStateInternals
    let replicaQueries = 0

    try {
      await internals.markBackfillComplete({
        createdAt: '2026-05-04T05:59:00.000Z',
        id: indexedUuid(1)
      })

      await internals.backfillOnce(
        {
          query: async () => {
            replicaQueries += 1
            return { rows: [] }
          }
        } as unknown as Pool,
        { fullSnapshot: false }
      )

      expect(replicaQueries).toBe(0)
    } finally {
      await runtime.close()
    }
  })

  test('incremental backfill resumes from an incomplete snapshot cursor', async () => {
    const testDatabaseUrl = await createTestDatabase(databaseUrl!)
    const runtime = new VolumeAnalyticsRuntime(testDatabaseUrl)
    const internals = runtime as unknown as BackfillStateInternals
    const cursor = {
      createdAt: '2026-05-04T05:59:00.000Z',
      id: indexedUuid(321)
    }
    let queryParams: unknown[] | undefined

    try {
      await internals.saveBackfillCursor(
        cursor,
        new Date('2026-05-04T05:00:00.000Z'),
        {
          createdAt: '2026-05-04T06:30:00.000Z',
          id: indexedUuid(999)
        }
      )

      await internals.backfillOnce(
        {
          query: async (_sql: string, params: unknown[]) => {
            queryParams = params
            return { rows: [] }
          }
        } as unknown as Pool,
        { fullSnapshot: false }
      )

      expect(queryParams?.[1]).toBe(cursor.createdAt)
      expect(queryParams?.[2]).toBe(cursor.id)
      expect(queryParams?.[3]).toBe('2026-05-04T06:30:00.000Z')
      expect(queryParams?.[4]).toBe(indexedUuid(999))
      await expect(internals.readBackfillState()).resolves.toMatchObject({
        completed: true,
        cursor_order_id: cursor.id
      })
    } finally {
      await runtime.close()
    }
  })

  test('public backfill runner queues a full snapshot while an incremental run is active', async () => {
    const testDatabaseUrl = await createTestDatabase(databaseUrl!)
    const runtime = new VolumeAnalyticsRuntime(testDatabaseUrl)
    const currentOrder = completedOrderDbRow({
      id: indexedUuid(401),
      createdAt: new Date('2026-05-04T06:00:00.000Z'),
      completedAt: new Date('2026-05-04T06:01:00.000Z'),
      amountUsdMicro: '1000000'
    })
    const secondOrder = completedOrderDbRow({
      id: indexedUuid(402),
      createdAt: new Date('2026-05-04T05:59:00.000Z'),
      completedAt: new Date('2026-05-04T06:02:00.000Z'),
      amountUsdMicro: '2000000'
    })
    const releaseFirstReplicaQuery = deferred<void>()
    const firstReplica = {
      query: async (_sql: string, _params?: unknown[]) => {
        await releaseFirstReplicaQuery.promise
        return { rows: [currentOrder] }
      }
    } as unknown as Pool
    const secondReplica = completedOrdersReplicaPool([currentOrder, secondOrder])

    try {
      runtime.startBackfill(firstReplica, { fullSnapshot: false })
      runtime.startBackfill(secondReplica, { fullSnapshot: true })
      releaseFirstReplicaQuery.resolve()

      await eventually(async () => {
        const buckets = await runtime.fetchVolumeBuckets({
          bucketSize: 'hour',
          orderType: 'all',
          from: new Date('2026-05-04T00:00:00.000Z'),
          to: new Date('2026-05-05T00:00:00.000Z')
        })
        expect(buckets).toEqual([
          {
            bucketStart: '2026-05-04T06:00:00.000Z',
            bucketSize: 'hour',
            orderType: 'all',
            volumeUsdMicro: '3000000',
            orderCount: 2
          }
        ])
      })
    } finally {
      await runtime.close()
    }
  })

  test('queued default backfill request keeps full snapshot semantics when merged', async () => {
    const testDatabaseUrl = await createTestDatabase(databaseUrl!)
    const runtime = new VolumeAnalyticsRuntime(testDatabaseUrl)
    const staleOrder = completedOrder({
      rawAmount: '1000000',
      amountUsdMicro: '1000000',
      completedAt: '2026-05-04T06:01:00.000Z'
    })
    staleOrder.id = indexedUuid(451)
    const staleDbOrder = completedOrderDbRow({
      id: indexedUuid(451),
      createdAt: new Date('2026-05-04T05:59:00.000Z'),
      completedAt: new Date('2026-05-04T06:01:00.000Z'),
      amountUsdMicro: '1000000'
    })
    const currentDbOrder = completedOrderDbRow({
      id: indexedUuid(452),
      createdAt: new Date('2026-05-04T06:03:00.000Z'),
      completedAt: new Date('2026-05-04T06:04:00.000Z'),
      amountUsdMicro: '2000000'
    })
    const releaseFirstReplicaQuery = deferred<void>()
    const firstReplica = {
      query: async (_sql: string, _params?: unknown[]) => {
        await releaseFirstReplicaQuery.promise
        return { rows: [staleDbOrder] }
      }
    } as unknown as Pool
    const secondReplica = completedOrdersReplicaPool([staleDbOrder])
    const thirdReplica = completedOrdersReplicaPool([currentDbOrder])

    try {
      await runtime.ingestOrder(staleOrder)
      runtime.startBackfill(firstReplica)
      runtime.startBackfill(secondReplica, { fullSnapshot: false })
      runtime.startBackfill(thirdReplica)
      releaseFirstReplicaQuery.resolve()

      await eventually(async () => {
        const buckets = await runtime.fetchVolumeBuckets({
          bucketSize: 'hour',
          orderType: 'all',
          from: new Date('2026-05-04T00:00:00.000Z'),
          to: new Date('2026-05-05T00:00:00.000Z')
        })
        expect(buckets).toEqual([
          {
            bucketStart: '2026-05-04T06:00:00.000Z',
            bucketSize: 'hour',
            orderType: 'all',
            volumeUsdMicro: '2000000',
            orderCount: 1
          }
        ])
      })
    } finally {
      await runtime.close()
    }
  })

  test('full snapshot backfill scans only up to its source upper bound', async () => {
    const testDatabaseUrl = await createTestDatabase(databaseUrl!)
    const runtime = new VolumeAnalyticsRuntime(testDatabaseUrl)
    const internals = runtime as unknown as BackfillStateInternals
    const upperBoundOrder = completedOrderDbRow({
      id: indexedUuid(501),
      createdAt: new Date('2026-05-04T06:00:00.000Z'),
      completedAt: new Date('2026-05-04T06:05:00.000Z'),
      amountUsdMicro: '1000000'
    })
    let upperBoundCaptured = false
    let pageQueryParams: unknown[] | undefined
    const replica = {
      query: async (_sql: string, params?: unknown[]) => {
        if (!params) {
          upperBoundCaptured = true
          return { rows: [upperBoundOrder] }
        }
        pageQueryParams = params
        return { rows: [] }
      }
    } as unknown as Pool

    try {
      await internals.backfillOnce(replica)

      expect(upperBoundCaptured).toBe(true)
      expect(pageQueryParams?.[3]).toBe(upperBoundOrder.created_at.toISOString())
      expect(pageQueryParams?.[4]).toBe(upperBoundOrder.id)
    } finally {
      await runtime.close()
    }
  })

  test('full snapshot backfill reconciles order status metrics', async () => {
    const testDatabaseUrl = await createTestDatabase(databaseUrl!)
    const runtime = new VolumeAnalyticsRuntime(testDatabaseUrl)
    const staleOrder = completedOrder({
      rawAmount: '1000000',
      amountUsdMicro: '1000000',
      completedAt: '2026-05-04T06:01:00.000Z'
    })
    staleOrder.id = indexedUuid(601)
    const currentDbOrder = completedOrderDbRow({
      id: indexedUuid(602),
      createdAt: new Date('2026-05-04T06:02:00.000Z'),
      completedAt: new Date('2026-05-04T06:03:00.000Z'),
      amountUsdMicro: '2000000'
    })

    try {
      await runtime.ingestOrder({
        ...staleOrder,
        status: 'failed',
        updatedAt: '2026-05-04T06:01:00.000Z'
      })
      await expectOrderMetrics(runtime, {
        total: 1,
        active: 0,
        needsAttention: 1
      })

      runtime.startBackfill(completedOrdersReplicaPool([currentDbOrder]))

      await eventually(async () => {
        await expectOrderMetrics(runtime, {
          total: 1,
          active: 0,
          needsAttention: 0
        })
      })
    } finally {
      await runtime.close()
    }
  })

  async function createTestDatabase(baseUrl: string) {
    const database = `admin_dashboard_analytics_test_${process.pid}_${Date.now()}`
    databases.push(database)
    const admin = new Client({ connectionString: adminDatabaseUrl(baseUrl) })
    await admin.connect()
    try {
      await admin.query(`CREATE DATABASE ${quoteIdentifier(database)}`)
    } finally {
      await admin.end()
    }

    const url = new URL(baseUrl)
    url.pathname = `/${database}`
    return url.toString()
  }
})

type BackfillStateInternals = {
  backfillOnce(
    replicaPool: Pool,
    options?: { fullSnapshot?: boolean }
  ): Promise<void>
  readBackfillState(): Promise<
    | {
        completed: boolean
        cursor_created_at: Date | null
        cursor_order_id: string | null
        snapshot_upper_created_at: Date | null
        snapshot_upper_order_id: string | null
      }
    | undefined
  >
  saveBackfillCursor(
    cursor: { createdAt: string; id: string },
    snapshotStartedAt?: Date,
    snapshotUpperBound?: { createdAt: string; id: string }
  ): Promise<void>
  markBackfillComplete(cursor?: { createdAt: string; id: string }): Promise<void>
}

function completedOrdersReplicaPool(rows: CompletedOrderDbRow[]): Pool {
  const orderedRows = [...rows].sort(compareCompletedOrderRowsDesc)
  return {
    query: async (_sql: string, params?: unknown[]) => {
      if (!params) {
        return { rows: orderedRows.slice(0, 1) }
      }
      const limit = Number(params[0])
      const cursorCreatedAt =
        typeof params[1] === 'string' ? params[1] : undefined
      const cursorOrderId = typeof params[2] === 'string' ? params[2] : undefined
      const upperCreatedAt = typeof params[3] === 'string' ? params[3] : undefined
      const upperOrderId = typeof params[4] === 'string' ? params[4] : undefined
      const eligible = orderedRows.filter((row) => {
        const beforeCursor =
          !cursorCreatedAt ||
          !cursorOrderId ||
          rowBeforeCursor(row, cursorCreatedAt, cursorOrderId)
        const withinUpperBound =
          !upperCreatedAt ||
          !upperOrderId ||
          rowAtOrBeforeCursor(row, upperCreatedAt, upperOrderId)
        return beforeCursor && withinUpperBound
      })
      return { rows: eligible.slice(0, limit) }
    }
  } as unknown as Pool
}

function compareCompletedOrderRowsDesc(
  left: CompletedOrderDbRow,
  right: CompletedOrderDbRow
) {
  const createdCompare =
    right.created_at.getTime() - left.created_at.getTime()
  if (createdCompare !== 0) return createdCompare
  return right.id.localeCompare(left.id)
}

function rowBeforeCursor(
  row: CompletedOrderDbRow,
  cursorCreatedAt: string,
  cursorOrderId: string
) {
  const cursorTime = new Date(cursorCreatedAt).getTime()
  const rowTime = row.created_at.getTime()
  return rowTime < cursorTime || (rowTime === cursorTime && row.id < cursorOrderId)
}

function rowAtOrBeforeCursor(
  row: CompletedOrderDbRow,
  cursorCreatedAt: string,
  cursorOrderId: string
) {
  const cursorTime = new Date(cursorCreatedAt).getTime()
  const rowTime = row.created_at.getTime()
  return rowTime < cursorTime || (rowTime === cursorTime && row.id <= cursorOrderId)
}

type CompletedOrderDbRow = ReturnType<typeof completedOrderDbRow>

function completedOrderDbRow({
  id,
  createdAt,
  completedAt,
  amountUsdMicro
}: {
  id: string
  createdAt: Date
  completedAt: Date
  amountUsdMicro: string
}) {
  const rawAmount = '1000000'
  return {
    id,
    order_type: 'market_order',
    status: 'completed',
    created_at: createdAt,
    updated_at: completedAt,
    source_chain_id: 'evm:1',
    source_asset_id: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
    destination_chain_id: 'evm:8453',
    destination_asset_id: '0x833589fcd6edb6e08f4c7c32d4f71b54bda02913',
    recipient_address: '0x1111111111111111111111111111111111111111',
    refund_address: '0x1111111111111111111111111111111111111111',
    action_timeout_at: completedAt,
    workflow_trace_id: null,
    workflow_parent_span_id: null,
    quote_id: null,
    quote_provider_id: null,
    order_kind: 'exact_in',
    quoted_amount_in: rawAmount,
    quoted_amount_out: rawAmount,
    min_amount_out: rawAmount,
    max_amount_in: null,
    slippage_bps: '100',
    quote_expires_at: completedAt,
    provider_quote: {},
    quote_usd_valuation: {},
    funding_tx_hash: null,
    execution_legs: [
      {
        id: indexedUuid(Number.parseInt(id.slice(-12), 10) + 10_000),
        legIndex: 0,
        legType: 'across_bridge',
        provider: 'across',
        status: 'completed',
        input: {
          chainId: 'evm:1',
          assetId: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
        },
        output: {
          chainId: 'evm:8453',
          assetId: '0x833589fcd6edb6e08f4c7c32d4f71b54bda02913'
        },
        amountIn: rawAmount,
        expectedAmountOut: rawAmount,
        actualAmountIn: rawAmount,
        actualAmountOut: rawAmount,
        completedAt: completedAt.toISOString(),
        createdAt: createdAt.toISOString(),
        updatedAt: completedAt.toISOString(),
        details: {},
        usdValuation: {
          amounts: {
            actualInput: {
              asset: {
                chainId: 'evm:1',
                assetId: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
              },
              raw: rawAmount,
              decimals: 6,
              unitUsdMicro: '1000000',
              amountUsdMicro
            }
          }
        }
      }
    ],
    execution_steps: [],
    provider_operations: []
  }
}

function indexedUuid(index: number) {
  return `00000000-0000-4000-8000-${String(index).padStart(12, '0')}`
}

async function expectHourBucket(
  runtime: VolumeAnalyticsRuntime,
  expected: { volumeUsdMicro: string; orderCount: number }
) {
  const buckets = await runtime.fetchVolumeBuckets({
    bucketSize: 'hour',
    orderType: 'all',
    from: new Date('2026-05-04T00:00:00.000Z'),
    to: new Date('2026-05-05T00:00:00.000Z')
  })
  expect(buckets).toHaveLength(1)
  expect(buckets[0]).toMatchObject({
    bucketStart: '2026-05-04T06:00:00.000Z',
    volumeUsdMicro: expected.volumeUsdMicro,
    orderCount: expected.orderCount
  })
}

async function expectOrderMetrics(
  runtime: VolumeAnalyticsRuntime,
  expected: { total: number; active: number; needsAttention: number }
) {
  await expect(runtime.fetchOrderMetrics()).resolves.toEqual(expected)
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

async function eventually(assertion: () => Promise<void>, timeoutMs = 2_000) {
  const deadline = Date.now() + timeoutMs
  let lastError: unknown
  while (Date.now() < deadline) {
    try {
      await assertion()
      return
    } catch (error) {
      lastError = error
      await new Promise((resolve) => setTimeout(resolve, 25))
    }
  }
  if (lastError) throw lastError
  await assertion()
}

function completedOrder({
  rawAmount,
  actualAmountIn,
  amountUsdMicro,
  completedAt
}: {
  rawAmount: string
  actualAmountIn?: string
  amountUsdMicro: string
  completedAt: string
}): OrderFirehoseRow {
  return {
    id: '019df1c4-8d87-7c20-89a4-e76883e94a0f',
    detailLevel: 'summary',
    orderType: 'market_order',
    status: 'completed',
    createdAt: '2026-05-04T05:59:00.000Z',
    updatedAt: completedAt,
    source: {
      chainId: 'evm:1',
      assetId: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
    },
    destination: {
      chainId: 'evm:8453',
      assetId: '0x833589fcd6edb6e08f4c7c32d4f71b54bda02913'
    },
    recipientAddress: '0x1111111111111111111111111111111111111111',
    refundAddress: '0x1111111111111111111111111111111111111111',
    actionTimeoutAt: '2026-05-04T07:00:00.000Z',
    executionLegs: [
      {
        id: '019df1c4-a8ac-7731-b596-a24603580d4f',
        legIndex: 0,
        legType: 'across_bridge',
        provider: 'across',
        status: 'completed',
        input: {
          chainId: 'evm:1',
          assetId: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
        },
        output: {
          chainId: 'evm:8453',
          assetId: '0x833589fcd6edb6e08f4c7c32d4f71b54bda02913'
        },
        amountIn: rawAmount,
        expectedAmountOut: rawAmount,
        actualAmountIn: actualAmountIn ?? rawAmount,
        actualAmountOut: rawAmount,
        completedAt,
        createdAt: '2026-05-04T06:00:00.000Z',
        updatedAt: completedAt,
        details: {},
        usdValuation: {
          amounts: {
            actualInput: {
              asset: {
                chainId: 'evm:1',
                assetId: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
              },
              raw: rawAmount,
              decimals: 6,
              unitUsdMicro: '1000000',
              amountUsdMicro
            }
          }
        }
      }
    ],
    executionSteps: [],
    providerOperations: [],
    progress: {
      totalStages: 1,
      completedStages: 1,
      failedStages: 0
    }
  }
}

function adminDatabaseUrl(value: string) {
  const url = new URL(value)
  url.pathname = '/postgres'
  return url.toString()
}

function quoteIdentifier(value: string) {
  return `"${value.replaceAll('"', '""')}"`
}
