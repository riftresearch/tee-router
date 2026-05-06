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
  const windows = ['6h', '24h', '7d', '30d', '90d'] as const
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

test('venue progress label shows awaiting funding for active funding deposit steps', async () => {
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

  expect(progressVenueLabel(order)).toBe('Awaiting Funding')
})

test('venue progress label shows awaiting funding before first planned venue', async () => {
  const { progressVenueLabel } = await appHelpers()
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

  expect(progressVenueLabel(order)).toBe('Awaiting Funding')
})

test('venue progress label shows venue after funding is observed', async () => {
  const { progressVenueLabel } = await appHelpers()
  const order: OrderFirehoseRow = {
    ...completedOrder({}),
    status: 'executing',
    fundingTxHash: '0xabc',
    executionLegs: [],
    progress: {
      totalStages: 2,
      completedStages: 0,
      failedStages: 0,
      activeStage: 'Cctp / Planned'
    }
  }

  expect(progressVenueLabel(order)).toBe('CCTP')
})

test('venue progress label strips provider action text from active stage', async () => {
  const { progressVenueLabel } = await appHelpers()
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
