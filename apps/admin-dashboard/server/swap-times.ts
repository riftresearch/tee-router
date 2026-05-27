import type { Pool } from 'pg'

import { parseNonNegativeSafeInteger } from './numeric'

export type SwapTimeAverageRow = {
  provider: string
  legType: string
  transitionDeclId: string
  input: {
    chainId: string
    assetId: string
  }
  output: {
    chainId: string
    assetId: string
  }
  sampleCount: number
  avgDurationMs: number
  minDurationMs: number
  maxDurationMs: number
  lastSampleAt: string
}

type SwapTimeAverageDbRow = {
  provider: string
  leg_type: string
  transition_decl_id: string
  input_chain_id: string
  input_asset_id: string
  output_chain_id: string
  output_asset_id: string
  sample_count: string
  avg_duration_ms: string
  min_duration_ms: string
  max_duration_ms: string
  last_sample_at: Date | string
}

export async function fetchSwapTimeAverages(
  pool: Pool,
  limit: number
): Promise<SwapTimeAverageRow[]> {
  const result = await pool.query<SwapTimeAverageDbRow>(
    `
    SELECT
      provider,
      leg_type,
      transition_decl_id,
      input_chain_id,
      input_asset_id,
      output_chain_id,
      output_asset_id,
      sample_count::text AS sample_count,
      avg_duration_ms::text AS avg_duration_ms,
      min_duration_ms::text AS min_duration_ms,
      max_duration_ms::text AS max_duration_ms,
      last_sample_at
    FROM public.router_swap_time_averages
    ORDER BY last_sample_at DESC, updated_at DESC
    LIMIT $1
    `,
    [limit]
  )

  return result.rows.map(mapSwapTimeAverageRow)
}

function mapSwapTimeAverageRow(row: SwapTimeAverageDbRow): SwapTimeAverageRow {
  return {
    provider: row.provider,
    legType: row.leg_type,
    transitionDeclId: row.transition_decl_id,
    input: {
      chainId: row.input_chain_id,
      assetId: row.input_asset_id
    },
    output: {
      chainId: row.output_chain_id,
      assetId: row.output_asset_id
    },
    sampleCount: parseNonNegativeSafeInteger(row.sample_count, 'swap time sample count'),
    avgDurationMs: parseNonNegativeSafeInteger(
      row.avg_duration_ms,
      'swap time average duration'
    ),
    minDurationMs: parseNonNegativeSafeInteger(
      row.min_duration_ms,
      'swap time minimum duration'
    ),
    maxDurationMs: parseNonNegativeSafeInteger(
      row.max_duration_ms,
      'swap time maximum duration'
    ),
    lastSampleAt: toIso(row.last_sample_at)
  }
}

function toIso(value: Date | string): string {
  if (value instanceof Date) return value.toISOString()
  return new Date(value).toISOString()
}
