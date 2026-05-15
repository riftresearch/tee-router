import { expect, test } from 'bun:test'

import type {
  AssetRef,
  OrderExecutionStep,
  OrderExecutionLeg,
  OrderFirehoseRow,
  UsdAmountValuation,
  VolumeAnalyticsResponse
} from './types'

test('volume chart normalization preserves USD micro precision beyond Number range', async () => {
  const { normalizedVolumeBuckets } = await appHelpers()
  const unsafeUsdMicro = '9007199254740993000000'
  const buckets = normalizedVolumeBuckets({
    bucketSize: 'hour',
    orderType: 'all',
    from: '2026-05-04T00:00:00.000Z',
    to: '2026-05-04T01:00:00.000Z',
    buckets: [
      {
        bucketStart: '2026-05-04T00:00:00.000Z',
        bucketSize: 'hour',
        orderType: 'all',
        volumeUsdMicro: unsafeUsdMicro,
        orderCount: 1
      }
    ]
  } satisfies VolumeAnalyticsResponse)

  expect(Number.isSafeInteger(Number(BigInt(unsafeUsdMicro)))).toBe(false)
  expect(buckets).toHaveLength(1)
  expect(buckets[0].volumeUsdMicro).toBe(BigInt(unsafeUsdMicro))
})

test('volume chart bar height converts only bounded ratios to Number', async () => {
  const { volumeBarHeight } = await appHelpers()
  const max = 9007199254740993000000n

  expect(volumeBarHeight(max, max, 176)).toBe(176)
  expect(volumeBarHeight(max / 2n, max, 176)).toBe(88)
  expect(volumeBarHeight(0n, max, 176)).toBe(0)
})

test('volume chart axis ticks include readable bounds', async () => {
  const { volumeXAxisTicks, volumeYAxisTicks } = await appHelpers()
  const buckets = Array.from({ length: 10 }, (_, index) => ({
    bucketStart: `2026-05-05T${String(index).padStart(2, '0')}:00:00.000Z`
  }))

  expect(volumeYAxisTicks(900n, 3).map((tick) => tick.value)).toEqual([
    0n,
    300n,
    600n,
    900n
  ])
  expect(volumeXAxisTicks(buckets, 4).map((tick) => tick.index)).toEqual([
    0,
    3,
    6,
    9
  ])
})

test('volume chart normalizes every fixed window and bucket size', async () => {
  const { normalizedVolumeBuckets, volumeDateRange } = await appHelpers()
  const now = new Date('2026-05-05T00:00:00.000Z')
  const windows = ['1h', '6h', '24h', '7d', '30d', '90d'] as const
  const bucketSizes = ['five_minute', 'hour', 'day'] as const

  for (const window of windows) {
    const range = volumeDateRange(window, now)
    for (const bucketSize of bucketSizes) {
      const buckets = normalizedVolumeBuckets({
        bucketSize,
        orderType: 'all',
        from: range.from.toISOString(),
        to: range.to.toISOString(),
        buckets: []
      })

      expect(buckets.length).toBeGreaterThan(0)
      expect(buckets.length).toBeLessThanOrEqual(1500)
    }
  }
})

test('volume analytics refreshes are coalesced under live order bursts', async () => {
  const { createCoalescedAsyncRefresh } = await appHelpers()
  let now = 1000
  let calls = 0
  let nextHandle = 1
  const timers = new Map<number, { delayMs: number; callback: () => void }>()
  const scheduler = createCoalescedAsyncRefresh({
    minIntervalMs: 2000,
    now: () => now,
    run: async () => {
      calls += 1
    },
    onError: (error) => {
      throw error
    },
    setTimer: (callback, delayMs) => {
      const handle = nextHandle++
      timers.set(handle, { delayMs, callback })
      return handle
    },
    clearTimer: (handle) => {
      timers.delete(handle)
    }
  })

  scheduler.request()
  scheduler.request()
  scheduler.request()
  await flushMicrotasks()

  expect(calls).toBe(1)
  expect([...timers.values()].map((timer) => timer.delayMs)).toEqual([2000])

  scheduler.request()
  expect(timers.size).toBe(1)

  now = 3000
  const timer = timers.get(1)
  timers.delete(1)
  timer?.callback()
  await flushMicrotasks()

  expect(calls).toBe(2)
  expect(timers.size).toBe(0)
})

test('order status display names every recovery and intervention state', async () => {
  const { statusDisplay } = await appHelpers()

  expect(statusDisplay('refund_required')).toMatchObject({
    label: 'Refund Required',
    tone: 'danger'
  })
  expect(statusDisplay('refunding')).toMatchObject({
    label: 'Refunding',
    tone: 'active'
  })
  expect(statusDisplay('manual_intervention_required')).toMatchObject({
    label: 'Manual Intervention',
    tone: 'danger'
  })
  expect(statusDisplay('refund_manual_intervention_required')).toMatchObject({
    label: 'Manual Refund',
    tone: 'danger'
  })
})

test('execution timeline does not synthesize actual input from planned input', async () => {
  const { executedAmounts, timelineLegs } = await appHelpers()
  const order = completedOrder({
    actualAmountIn: undefined,
    actualAmountOut: '89'
  })

  const [leg] = timelineLegs(order)

  expect(leg.executedInput).toBeUndefined()
  expect(executedAmounts(order)).toEqual({ placeholder: 'Not observed' })
})

test('executed USD values require actual valuation entries', async () => {
  const { executedAmounts, timelineLegs } = await appHelpers()
  const order = completedOrder({
    actualAmountIn: '99',
    actualAmountOut: '89',
    usdValuation: {
      schemaVersion: 1,
      amounts: {
        plannedInput: valuation('100', USDC),
        actualOutput: valuation('89', USDC)
      }
    }
  })

  const [leg] = timelineLegs(order)
  const executed = executedAmounts(order)

  expect(leg.executedInputUsd).toBeUndefined()
  expect(leg.executedOutputUsd?.raw).toBe('89')
  if ('placeholder' in executed) throw new Error('expected executed amounts')
  expect(executed.input.usdValuation).toBeUndefined()
  expect(executed.output.usdValuation?.raw).toBe('89')
})

test('execution timeline hides superseded legs while preserving original quoted amounts', async () => {
  const { timelineLegs, timelineSupersededStats } = await appHelpers()
  const now = '2026-05-05T00:00:00.000Z'
  const supersededLeg: OrderExecutionLeg = {
    id: 'leg-original',
    executionAttemptId: 'attempt-original',
    transitionDeclId: 'transition-1',
    legIndex: 0,
    legType: 'hyperliquid_trade',
    provider: 'hyperliquid',
    status: 'superseded',
    input: USDC,
    output: USDC,
    amountIn: '100',
    expectedAmountOut: '90',
    minAmountOut: '88',
    createdAt: now,
    updatedAt: now,
    details: {},
    usdValuation: {
      schemaVersion: 1,
      amounts: {
        plannedInput: valuation('100', USDC),
        plannedOutput: valuation('90', USDC)
      }
    }
  }
  const refreshedLeg: OrderExecutionLeg = {
    ...supersededLeg,
    id: 'leg-refreshed',
    executionAttemptId: 'attempt-refreshed',
    status: 'completed',
    amountIn: '105',
    expectedAmountOut: '91',
    minAmountOut: '89',
    actualAmountIn: '105',
    actualAmountOut: '91',
    createdAt: '2026-05-05T00:01:00.000Z',
    updatedAt: '2026-05-05T00:01:00.000Z'
  }
  const order: OrderFirehoseRow = {
    ...completedOrder({}),
    executionLegs: [supersededLeg, refreshedLeg]
  }

  const legs = timelineLegs(order)

  expect(legs).toHaveLength(1)
  expect(legs[0].key).toBe('leg-refreshed')
  expect(legs[0].quotedInput).toBe('100')
  expect(legs[0].quotedOutput).toBe('90')
  expect(legs[0].minAmountOut).toBe('88')
  expect(legs[0].executedInput).toBe('105')
  expect(legs[0].executedOutput).toBe('91')
  expect(legs[0].quotedInputUsd?.raw).toBe('100')
  expect(timelineSupersededStats(order)).toEqual({
    hiddenLegs: 1,
    attempts: 1
  })
})

test('venue progress label stays empty for active funding deposit steps', async () => {
  const { progressVenueLabel } = await appHelpers()
  const now = '2026-05-05T00:00:00.000Z'
  const fundingStep: OrderExecutionStep = {
    id: 'step-1',
    stepIndex: 0,
    stepType: 'wait_for_deposit',
    provider: 'internal',
    status: 'running',
    createdAt: now,
    updatedAt: now,
    details: {},
    request: {},
    response: {},
    error: null
  }
  const order: OrderFirehoseRow = {
    ...completedOrder({}),
    status: 'pending_funding',
    executionLegs: [],
    executionSteps: [fundingStep],
    progress: {
      totalStages: 1,
      completedStages: 0,
      failedStages: 0
    }
  }

  expect(progressVenueLabel(order)).toBeUndefined()
})

test('venue progress count stays numeric when backend reports zero stages', async () => {
  const { progressStageCountLabel } = await appHelpers()

  expect(
    progressStageCountLabel({
      totalStages: 0,
      completedStages: 0,
      failedStages: 0
    })
  ).toBe('0/0')
})

test('venue progress label stays empty before first planned venue', async () => {
  const { progressVenueLabel, shouldShowVenueProgress } = await appHelpers()
  const order: OrderFirehoseRow = {
    ...completedOrder({}),
    status: 'pending_funding',
    executionLegs: [],
    progress: {
      totalStages: 2,
      completedStages: 0,
      failedStages: 0,
      activeStage: 'Cctp / Planned'
    }
  }

  expect(progressVenueLabel(order)).toBeUndefined()
  expect(shouldShowVenueProgress(order)).toBe(false)
})

test('venue progress label stays empty for planned refund venue', async () => {
  const { progressVenueLabel, shouldShowVenueProgress } = await appHelpers()
  const order: OrderFirehoseRow = {
    ...completedOrder({}),
    status: 'refunding',
    executionLegs: [],
    progress: {
      totalStages: 2,
      completedStages: 0,
      failedStages: 0,
      activeStage: 'Unit / Planned'
    }
  }

  expect(progressVenueLabel(order)).toBeUndefined()
  expect(shouldShowVenueProgress(order)).toBe(false)
})

test('venue progress label shows venue for active venue execution', async () => {
  const { progressVenueLabel, shouldShowVenueProgress } = await appHelpers()
  const order: OrderFirehoseRow = {
    ...completedOrder({
      provider: 'cctp',
      status: 'running'
    }),
    status: 'executing',
    progress: {
      totalStages: 2,
      completedStages: 0,
      failedStages: 0,
      activeStage: 'Cctp / Running'
    }
  }

  expect(progressVenueLabel(order)).toBe('CCTP')
  expect(shouldShowVenueProgress(order)).toBe(true)
})

test('venue progress label strips provider action text from active stage', async () => {
  const { progressVenueLabel, shouldShowVenueProgress } = await appHelpers()
  const order: OrderFirehoseRow = {
    ...completedOrder({}),
    status: 'executing',
    executionLegs: [],
    progress: {
      totalStages: 2,
      completedStages: 0,
      failedStages: 0,
      activeStage: 'Cctp Cctp Burn / Waiting'
    }
  }

  expect(progressVenueLabel(order)).toBe('CCTP')
  expect(shouldShowVenueProgress(order)).toBe(true)
})

test('SSE snapshots preserve expanded full-row execution details', async () => {
  const { mergeSnapshotOrders } = await appHelpers()
  const now = '2026-05-05T00:00:00.000Z'
  const fundingStep: OrderExecutionStep = {
    id: 'step-1',
    stepIndex: 0,
    stepType: 'wait_for_deposit',
    provider: 'internal',
    status: 'completed',
    txHash: '0xabc',
    createdAt: now,
    updatedAt: now,
    details: {},
    request: {},
    response: {},
    error: null
  }
  const fullOrder: OrderFirehoseRow = {
    ...completedOrder({}),
    detailLevel: 'full',
    executionSteps: [fundingStep]
  }
  const summaryOrder: OrderFirehoseRow = {
    ...fullOrder,
    detailLevel: 'summary',
    updatedAt: '2026-05-05T00:01:00.000Z',
    executionSteps: []
  }

  const [merged] = mergeSnapshotOrders([fullOrder], [summaryOrder])

  expect(merged.detailLevel).toBe('full')
  expect(merged.updatedAt).toBe(summaryOrder.updatedAt)
  expect(merged.executionSteps).toEqual([fundingStep])
})

test('live order retention caps the rendered firehose window', async () => {
  const { LIVE_ORDER_RENDER_LIMIT, retainLiveOrderWindow } = await appHelpers()
  const orders = firehoseOrders(LIVE_ORDER_RENDER_LIMIT + 25)

  const retained = retainLiveOrderWindow(orders)

  expect(retained).toHaveLength(LIVE_ORDER_RENDER_LIMIT)
  expect(retained.map((order) => order.id)).toEqual(
    orders.slice(0, LIVE_ORDER_RENDER_LIMIT).map((order) => order.id)
  )
})

test('live order retention preserves an expanded older order', async () => {
  const { LIVE_ORDER_RENDER_LIMIT, retainLiveOrderWindow } = await appHelpers()
  const orders = firehoseOrders(LIVE_ORDER_RENDER_LIMIT + 25)
  const retainedOrder = orders[LIVE_ORDER_RENDER_LIMIT + 20]

  const retained = retainLiveOrderWindow(orders, retainedOrder.id)

  expect(retained).toHaveLength(LIVE_ORDER_RENDER_LIMIT + 1)
  expect(retained.at(-1)?.id).toBe(retainedOrder.id)
  expect(retained.slice(0, LIVE_ORDER_RENDER_LIMIT).map((order) => order.id)).toEqual(
    orders.slice(0, LIVE_ORDER_RENDER_LIMIT).map((order) => order.id)
  )
})

test('timeline execution placeholder distinguishes submitted external txs', async () => {
  const { timelineExecutionPlaceholder, timelineLegs } = await appHelpers()
  const now = '2026-05-05T00:00:00.000Z'
  const step: OrderExecutionStep = {
    id: 'step-1',
    executionLegId: 'leg-1',
    stepIndex: 3,
    stepType: 'unit_withdrawal',
    provider: 'unit',
    status: 'waiting',
    txHash: '0xabc',
    createdAt: now,
    updatedAt: now,
    details: {},
    request: {},
    response: {},
    error: null
  }
  const [leg] = timelineLegs({
    ...completedOrder({
      id: 'leg-1',
      legIndex: 3,
      legType: 'unit_withdrawal',
      provider: 'unit',
      status: 'waiting',
      actualAmountIn: undefined,
      actualAmountOut: undefined
    }),
    status: 'executing',
    executionSteps: [step],
    progress: {
      totalStages: 4,
      completedStages: 3,
      failedStages: 0
    }
  })

  expect(timelineExecutionPlaceholder(leg)).toBe(
    'Transaction submitted, awaiting settlement'
  )
})

let appHelpersPromise: Promise<typeof import('./App')> | undefined

function appHelpers() {
  Object.defineProperty(globalThis, 'window', {
    value: { location: { origin: 'http://localhost:3000' } },
    configurable: true
  })
  appHelpersPromise ??= import('./App')
  return appHelpersPromise
}

async function flushMicrotasks() {
  await Promise.resolve()
  await Promise.resolve()
}

const USDC: AssetRef = {
  chainId: 'evm:8453',
  assetId: '0x833589fcd6edb6e08f4c7c32d4f71b54bda02913'
}

function valuation(raw: string, asset: AssetRef): UsdAmountValuation {
  return {
    raw,
    asset,
    canonical: 'usdc',
    decimals: 0,
    unitUsdMicro: '1000000',
    amountUsdMicro: `${raw}000000`
  }
}

function completedOrder(legOverrides: Partial<OrderExecutionLeg>): OrderFirehoseRow {
  const now = '2026-05-05T00:00:00.000Z'
  const leg: OrderExecutionLeg = {
    id: 'leg-1',
    transitionDeclId: 'transition-1',
    legIndex: 0,
    legType: 'universal_router_swap',
    provider: 'velora',
    status: 'completed',
    input: USDC,
    output: USDC,
    amountIn: '100',
    expectedAmountOut: '90',
    actualAmountIn: '99',
    actualAmountOut: '89',
    createdAt: now,
    updatedAt: now,
    details: {},
    ...legOverrides
  }
  return {
    id: 'order-1',
    detailLevel: 'full',
    orderType: 'market_order',
    status: 'completed',
    createdAt: now,
    updatedAt: now,
    source: USDC,
    destination: USDC,
    recipientAddress: '0x1111111111111111111111111111111111111111',
    refundAddress: '0x1111111111111111111111111111111111111111',
    actionTimeoutAt: now,
    executionLegs: [leg],
    executionSteps: [],
    providerOperations: [],
    progress: {
      totalStages: 1,
      completedStages: 1,
      failedStages: 0
    }
  }
}

function firehoseOrders(count: number): OrderFirehoseRow[] {
  const base = Date.parse('2026-05-05T00:00:00.000Z')
  return Array.from({ length: count }, (_, index) => {
    const timestamp = new Date(base - index * 1000).toISOString()
    return {
      ...completedOrder({}),
      id: `00000000-0000-0000-0000-${String(index).padStart(12, '0')}`,
      createdAt: timestamp,
      updatedAt: timestamp
    }
  })
}
