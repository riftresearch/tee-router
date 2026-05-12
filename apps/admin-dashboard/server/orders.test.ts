import { expect, test } from 'bun:test'
import type { Pool } from 'pg'

import { fetchOrderById, fetchOrderFirehose, fetchOrderMetrics } from './orders'

test('fetchOrderFirehose emits explicit lifecycle predicates for indexed tabs', async () => {
  let capturedSql = ''
  let capturedValues: unknown[] = []
  const pool = {
    query: async (sql: string, values: unknown[]) => {
      capturedSql = sql
      capturedValues = values
      return { rows: [orderRow()] }
    }
  } as unknown as Pool

  await fetchOrderFirehose(pool, 50, undefined, 'market_order', 'needs_attention')

  expect(capturedValues).toEqual([50, null, null, 'market_order'])
  expect(capturedSql).toContain('ro.order_type = $4::text')
  expect(capturedSql).toContain("ro.status IN (\n        'refund_required'")
  expect(capturedSql).not.toContain("'expired'")
  expect(capturedSql).toContain("'manual_intervention_required'")
  expect(capturedSql).not.toContain("ro.status IN (\n        'failed'")
  expect(capturedSql).not.toContain('$5::text')
  expect(capturedSql).not.toContain("OR $5::text = 'needs_attention'")
})

test('fetchOrderFirehose emits dedicated expired lifecycle predicate', async () => {
  let capturedSql = ''
  let capturedValues: unknown[] = []
  const pool = {
    query: async (sql: string, values: unknown[]) => {
      capturedSql = sql
      capturedValues = values
      return { rows: [orderRow()] }
    }
  } as unknown as Pool

  await fetchOrderFirehose(pool, 50, undefined, 'market_order', 'expired')

  expect(capturedValues).toEqual([50, null, null, 'market_order'])
  expect(capturedSql).toContain("ro.status = 'expired'")
  expect(capturedSql).not.toContain("'refund_required'")
})

test('fetchOrderFirehose keeps firehose scans free of lifecycle OR predicates', async () => {
  let capturedSql = ''
  let capturedValues: unknown[] = []
  const pool = {
    query: async (sql: string, values: unknown[]) => {
      capturedSql = sql
      capturedValues = values
      return { rows: [orderRow()] }
    }
  } as unknown as Pool

  await fetchOrderFirehose(pool, 100, undefined, undefined, 'firehose')

  expect(capturedValues).toEqual([100, null, null])
  expect(capturedSql).not.toContain('$4::text IS NULL')
  expect(capturedSql).not.toContain('$5::text')
  expect(capturedSql).not.toContain('refund_manual_intervention_required')
})

test('fetchOrderFirehose derives summary progress from provider quote without exposing full quote', async () => {
  let capturedSql = ''
  const pool = {
    query: async (sql: string) => {
      capturedSql = sql
      return {
        rows: [
          orderRow({
            status: 'pending_funding',
            provider_quote: {
              legs: [
                {
                  provider: 'cctp',
                  transition_kind: 'cctp_bridge',
                  execution_step_type: 'cctp_bridge',
                  transition_decl_id: 'bridge-1'
                },
                {
                  provider: 'unit',
                  transition_kind: 'unit_withdrawal',
                  execution_step_type: 'unit_withdrawal',
                  transition_decl_id: 'withdrawal-1'
                }
              ]
            },
            execution_legs: [],
            provider_operations: []
          })
        ]
      }
    }
  } as unknown as Pool

  const [order] = await fetchOrderFirehose(pool, 10)

  expect(capturedSql).toContain(
    'COALESCE(moq.provider_quote, loq.provider_quote) AS provider_quote'
  )
  expect(capturedSql).not.toContain('NULL::jsonb AS provider_quote')
  expect(order.detailLevel).toBe('summary')
  expect(order.providerQuote).toBeUndefined()
  expect(order.executionLegs).toEqual([])
  expect(order.progress).toMatchObject({
    totalStages: 2,
    completedStages: 0,
    failedStages: 0,
    activeStage: 'Cctp / Planned'
  })
})

test('fetchOrderFirehose keeps displayed quote terms anchored to the original order', async () => {
  let capturedSql = ''
  const pool = {
    query: async (sql: string) => {
      capturedSql = sql
      return { rows: [orderRow()] }
    }
  } as unknown as Pool

  await fetchOrderFirehose(pool, 10)

  expect(capturedSql).toContain('initial_quotes AS')
  expect(capturedSql).toContain('ORDER BY moq.order_id, moq.created_at ASC, moq.id ASC')
  expect(capturedSql).toContain(
    'COALESCE(moa.amount_in, moq.amount_in, loa.input_amount, loq.input_amount) AS quoted_amount_in'
  )
  expect(capturedSql).toContain(
    'COALESCE(moa.amount_out, moq.amount_out, loa.output_amount, loq.output_amount) AS quoted_amount_out'
  )
})

test('fetchOrderFirehose preserves execution attempt ids on execution legs', async () => {
  const now = new Date('2026-05-04T00:00:00.000Z')
  const attemptId = '019e0000-0000-7000-8000-000000000001'
  const pool = {
    query: async () => {
      return {
        rows: [
          orderRow({
            execution_legs: [
              {
                id: 'leg-1',
                executionAttemptId: attemptId,
                transitionDeclId: 'transition-1',
                legIndex: 0,
                legType: 'cctp_bridge',
                provider: 'cctp',
                status: 'completed',
                input: {
                  chainId: 'evm:8453',
                  assetId: 'usdc'
                },
                output: {
                  chainId: 'evm:42161',
                  assetId: 'usdc'
                },
                amountIn: '10000000',
                expectedAmountOut: '10000000',
                startedAt: now.toISOString(),
                completedAt: now.toISOString(),
                createdAt: now.toISOString(),
                updatedAt: now.toISOString(),
                details: {}
              }
            ]
          })
        ]
      }
    }
  } as unknown as Pool

  const [order] = await fetchOrderFirehose(pool, 10)

  expect(order.executionLegs[0]?.executionAttemptId).toBe(attemptId)
})

test('fetchOrderFirehose keeps summary rows lightweight', async () => {
  const now = new Date('2026-05-04T00:00:00.000Z')
  const actualInput = usdAmount('10000000', 'evm:8453', 'usdc')
  const actualOutput = usdAmount('10000000', 'evm:42161', 'usdc')
  const plannedInput = usdAmount('10000000', 'evm:8453', 'usdc')
  const plannedOutput = usdAmount('10000000', 'evm:42161', 'usdc')
  const plannedMinOutput = usdAmount('9900000', 'evm:42161', 'usdc')
  const pool = {
    query: async () => {
      return {
        rows: [
          orderRow({
            quote_usd_valuation: {
              schemaVersion: 1,
              pricing: {
                source: 'test',
                capturedAt: now.toISOString()
              },
              amounts: {
                input: actualInput,
                output: actualOutput,
                plannedInput
              },
              legs: [{ index: 0, amounts: { input: actualInput } }]
            },
            execution_legs: [
              executionLeg({
                id: 'leg-1',
                status: 'completed',
                actualAmountIn: '10000000',
                actualAmountOut: '10000000',
                usdValuation: {
                  schemaVersion: 1,
                  amounts: {
                    actualInput,
                    actualOutput,
                    plannedInput,
                    plannedOutput,
                    plannedMinOutput
                  },
                  legs: [{ index: 0, amounts: { actualInput } }]
                }
              }),
              executionLeg({
                id: 'leg-2',
                status: 'superseded',
                createdAt: '2026-05-04T00:01:00.000Z',
                updatedAt: '2026-05-04T00:01:00.000Z'
              })
            ]
          })
        ]
      }
    }
  } as unknown as Pool

  const [order] = await fetchOrderFirehose(pool, 10)
  const quoteValuation = order.quoteUsdValuation as
    | { amounts?: Record<string, unknown>; legs?: unknown[] }
    | undefined
  const legValuation = order.executionLegs[0]?.usdValuation as
    | { amounts?: Record<string, unknown>; legs?: unknown[] }
    | undefined

  expect(order.detailLevel).toBe('summary')
  expect(quoteValuation?.legs).toBeUndefined()
  expect(Object.keys(quoteValuation?.amounts ?? {}).sort()).toEqual([
    'input',
    'output'
  ])
  expect(order.executionLegs.map((leg) => leg.id)).toEqual(['leg-1'])
  expect(legValuation?.legs).toBeUndefined()
  expect(Object.keys(legValuation?.amounts ?? {}).sort()).toEqual([
    'actualInput',
    'actualOutput',
    'plannedInput',
    'plannedMinOutput',
    'plannedOutput'
  ])
})

test('fetchOrderById preserves full order detail payloads', async () => {
  const actualInput = usdAmount('10000000', 'evm:8453', 'usdc')
  const providerQuote = { legs: [{ provider: 'cctp' }] }
  const quoteUsdValuation = {
    schemaVersion: 1,
    amounts: {
      input: actualInput
    },
    legs: [{ index: 0, amounts: { input: actualInput } }]
  }
  const pool = {
    query: async () => {
      return {
        rows: [
          orderRow({
            provider_quote: providerQuote,
            quote_usd_valuation: quoteUsdValuation,
            execution_legs: [
              executionLeg({
                id: 'leg-1',
                status: 'superseded',
                usdValuation: quoteUsdValuation
              })
            ]
          })
        ]
      }
    }
  } as unknown as Pool

  const order = await fetchOrderById(pool, '019df446-096f-7290-bf84-dc9dac9dd8af')

  expect(order?.detailLevel).toBe('full')
  expect(order?.providerQuote).toEqual(providerQuote)
  expect(order?.quoteUsdValuation).toEqual(quoteUsdValuation)
  expect(order?.executionLegs).toHaveLength(1)
  expect(order?.executionLegs[0]?.usdValuation).toEqual(quoteUsdValuation)
})

test('fetchOrderFirehose rejects malformed JSON aggregate strings', async () => {
  const pool = {
    query: async () => {
      return {
        rows: [
          orderRow({
            execution_legs: '{',
            execution_steps: '{',
            provider_operations: '{'
          })
        ]
      }
    }
  } as unknown as Pool

  await expect(fetchOrderFirehose(pool, 10)).rejects.toThrow(
    /Expected property name|JSON/
  )
})

test('fetchOrderFirehose rejects empty JSON aggregate strings', async () => {
  const pool = {
    query: async () => {
      return {
        rows: [
          orderRow({
            execution_legs: '',
            execution_steps: [],
            provider_operations: []
          })
        ]
      }
    }
  } as unknown as Pool

  await expect(fetchOrderFirehose(pool, 10)).rejects.toThrow(
    /Unexpected (end of JSON input|EOF)/
  )
})

test('fetchOrderMetrics rejects unsafe count values', async () => {
  const pool = {
    query: async () => {
      return {
        rows: [
          {
            total: '9007199254740992',
            active: '0',
            needs_attention: '0'
          }
        ]
      }
    }
  } as unknown as Pool

  await expect(fetchOrderMetrics(pool)).rejects.toThrow(
    'order metric total exceeds JavaScript safe integer range'
  )
})

test('fetchOrderMetrics excludes expired orders from needs-attention counts', async () => {
  let capturedSql = ''
  const pool = {
    query: async (sql: string) => {
      capturedSql = sql
      return {
        rows: [
          {
            total: '0',
            active: '0',
            needs_attention: '0'
          }
        ]
      }
    }
  } as unknown as Pool

  await expect(fetchOrderMetrics(pool)).resolves.toEqual({
    total: 0,
    active: 0,
    needsAttention: 0
  })
  expect(capturedSql).toContain("WHERE status IN (\n          'refund_required'")
  expect(capturedSql).not.toContain("'expired'")
})

test('fetchOrderMetrics rejects oversized count strings before numeric coercion', async () => {
  const pool = {
    query: async () => {
      return {
        rows: [
          {
            total: '9'.repeat(10_000),
            active: '0',
            needs_attention: '0'
          }
        ]
      }
    }
  } as unknown as Pool

  await expect(fetchOrderMetrics(pool)).rejects.toThrow(
    'order metric total exceeds JavaScript safe integer range'
  )
})

function orderRow(overrides: Record<string, unknown> = {}) {
  const now = new Date('2026-05-04T00:00:00.000Z')
  return {
    id: '019df446-096f-7290-bf84-dc9dac9dd8af',
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
    funding_tx_hash: null,
    ...overrides
  }
}

function executionLeg(overrides: Record<string, unknown> = {}) {
  const now = '2026-05-04T00:00:00.000Z'
  return {
    id: 'leg-1',
    executionAttemptId: '019e0000-0000-7000-8000-000000000001',
    transitionDeclId: 'transition-1',
    legIndex: 0,
    legType: 'cctp_bridge',
    provider: 'cctp',
    status: 'completed',
    input: {
      chainId: 'evm:8453',
      assetId: 'usdc'
    },
    output: {
      chainId: 'evm:42161',
      assetId: 'usdc'
    },
    amountIn: '10000000',
    expectedAmountOut: '10000000',
    minAmountOut: null,
    actualAmountIn: null,
    actualAmountOut: null,
    startedAt: null,
    completedAt: null,
    createdAt: now,
    updatedAt: now,
    details: {},
    usdValuation: null,
    ...overrides
  }
}

function usdAmount(raw: string, chainId: string, assetId: string) {
  return {
    raw,
    asset: {
      chainId,
      assetId
    },
    decimals: 6,
    canonical: 'usdc',
    unitUsdMicro: '1000000',
    amountUsdMicro: raw
  }
}
