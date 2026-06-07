import type { Pool } from 'pg'

export type RouteCostSnapshotRow = {
  transitionId: string
  amountBucket: string
  provider: string
  edgeKind: string
  source: { chainId: string; assetId: string }
  destination: { chainId: string; assetId: string }
  estimatedFeeBps: number
  estimatedFeeUsdMicros: number
  estimatedGasUsdMicros: number
  estimatedLatencyMs: number
  sampleAmountUsdMicros: number
  quoteSource: string
  refreshedAt: string
  expiresAt: string
  expired: boolean
}

type RouteCostDbRow = {
  transition_id: string
  amount_bucket: string
  provider: string
  edge_kind: string
  source_chain: string
  source_asset: string
  destination_chain: string
  destination_asset: string
  estimated_fee_bps: string | number
  estimated_fee_usd_micros: string | number
  estimated_gas_usd_micros: string | number
  estimated_latency_ms: string | number
  sample_amount_usd_micros: string | number
  quote_source: string
  refreshed_at: Date | string
  expires_at: Date | string
}

const ROUTE_COSTS_SQL = `
  SELECT
    transition_id,
    amount_bucket,
    provider,
    edge_kind,
    source_chain,
    source_asset,
    destination_chain,
    destination_asset,
    estimated_fee_bps,
    estimated_fee_usd_micros,
    estimated_gas_usd_micros,
    estimated_latency_ms,
    sample_amount_usd_micros,
    quote_source,
    refreshed_at,
    expires_at
  FROM public.router_route_cost_snapshots
  ORDER BY provider ASC, transition_id ASC, sample_amount_usd_micros ASC
`

export async function fetchRouteCostSnapshots(
  pool: Pool
): Promise<RouteCostSnapshotRow[]> {
  const now = Date.now()
  const result = await pool.query<RouteCostDbRow>(ROUTE_COSTS_SQL)
  return result.rows.map((row) => mapRouteCostRow(row, now))
}

function mapRouteCostRow(row: RouteCostDbRow, nowMs: number): RouteCostSnapshotRow {
  const expiresAt = toIso(row.expires_at)
  return {
    transitionId: row.transition_id,
    amountBucket: row.amount_bucket,
    provider: row.provider,
    edgeKind: row.edge_kind,
    source: { chainId: row.source_chain, assetId: row.source_asset },
    destination: {
      chainId: row.destination_chain,
      assetId: row.destination_asset
    },
    estimatedFeeBps: toNumber(row.estimated_fee_bps, 'estimated_fee_bps'),
    estimatedFeeUsdMicros: toNumber(
      row.estimated_fee_usd_micros,
      'estimated_fee_usd_micros'
    ),
    estimatedGasUsdMicros: toNumber(
      row.estimated_gas_usd_micros,
      'estimated_gas_usd_micros'
    ),
    estimatedLatencyMs: toNumber(row.estimated_latency_ms, 'estimated_latency_ms'),
    sampleAmountUsdMicros: toNumber(
      row.sample_amount_usd_micros,
      'sample_amount_usd_micros'
    ),
    quoteSource: row.quote_source,
    refreshedAt: toIso(row.refreshed_at),
    expiresAt,
    expired: Date.parse(expiresAt) <= nowMs
  }
}

function toNumber(value: string | number, field: string): number {
  const parsed = typeof value === 'number' ? value : Number(value)
  if (!Number.isFinite(parsed)) {
    throw new Error(`route cost ${field} is not a finite number: ${value}`)
  }
  return parsed
}

function toIso(value: Date | string): string {
  if (value instanceof Date) return value.toISOString()
  return new Date(value).toISOString()
}
