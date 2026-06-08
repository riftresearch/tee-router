import type { Pool } from 'pg'

export type RouteCostSampleEventRow = {
  id: string
  sampledAt: string
  provider: string
  transitionId: string
  amountBucket: string
  edgeKind: string
  source: { chainId: string; assetId: string }
  destination: { chainId: string; assetId: string }
  sampleAmountUsdMicros: number
  outcome: 'succeeded' | 'failed'
  estimatedFeeBps: number | null
  estimatedLatencyMs: number | null
  reason: string | null
}

type RouteCostEventDbRow = {
  id: string | number
  sampled_at: Date | string
  provider: string
  transition_id: string
  amount_bucket: string
  edge_kind: string
  source_chain: string
  source_asset: string
  destination_chain: string
  destination_asset: string
  sample_amount_usd_micros: string | number
  outcome: string
  estimated_fee_bps: string | number | null
  estimated_latency_ms: string | number | null
  reason: string | null
}

const ROUTE_COST_EVENTS_SQL = `
  SELECT
    id,
    sampled_at,
    provider,
    transition_id,
    amount_bucket,
    edge_kind,
    source_chain,
    source_asset,
    destination_chain,
    destination_asset,
    sample_amount_usd_micros,
    outcome,
    estimated_fee_bps,
    estimated_latency_ms,
    reason
  FROM public.router_route_cost_sample_events
  WHERE sampled_at >= $1
  ORDER BY sampled_at DESC, id DESC
  LIMIT $2
`

export type FetchRouteCostSampleEventsOptions = {
  sinceMs?: number
  limit?: number
}

const DEFAULT_WINDOW_MS = 40 * 60 * 1000
const DEFAULT_LIMIT = 2000

export async function fetchRouteCostSampleEvents(
  pool: Pool,
  options: FetchRouteCostSampleEventsOptions = {}
): Promise<RouteCostSampleEventRow[]> {
  const sinceMs = options.sinceMs ?? Date.now() - DEFAULT_WINDOW_MS
  const limit = options.limit ?? DEFAULT_LIMIT
  const result = await pool.query<RouteCostEventDbRow>(ROUTE_COST_EVENTS_SQL, [
    new Date(sinceMs).toISOString(),
    limit
  ])
  return result.rows.map(mapRouteCostEventRow)
}

function mapRouteCostEventRow(row: RouteCostEventDbRow): RouteCostSampleEventRow {
  return {
    id: String(row.id),
    sampledAt: toIso(row.sampled_at),
    provider: row.provider,
    transitionId: row.transition_id,
    amountBucket: row.amount_bucket,
    edgeKind: row.edge_kind,
    source: { chainId: row.source_chain, assetId: row.source_asset },
    destination: {
      chainId: row.destination_chain,
      assetId: row.destination_asset
    },
    sampleAmountUsdMicros: toNumber(
      row.sample_amount_usd_micros,
      'sample_amount_usd_micros'
    ),
    outcome: row.outcome === 'failed' ? 'failed' : 'succeeded',
    estimatedFeeBps: toNullableNumber(row.estimated_fee_bps),
    estimatedLatencyMs: toNullableNumber(row.estimated_latency_ms),
    reason: row.reason
  }
}

function toNumber(value: string | number, field: string): number {
  const parsed = typeof value === 'number' ? value : Number(value)
  if (!Number.isFinite(parsed)) {
    throw new Error(`route cost event ${field} is not a finite number: ${value}`)
  }
  return parsed
}

function toNullableNumber(value: string | number | null): number | null {
  if (value === null || value === undefined) return null
  const parsed = typeof value === 'number' ? value : Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function toIso(value: Date | string): string {
  if (value instanceof Date) return value.toISOString()
  return new Date(value).toISOString()
}
