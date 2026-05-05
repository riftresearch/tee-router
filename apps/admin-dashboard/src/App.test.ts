import { expect, test } from 'bun:test'

import type {
  AssetRef,
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
