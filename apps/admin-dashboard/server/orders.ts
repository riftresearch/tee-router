import type { Pool } from 'pg'

export type AssetRef = {
  chainId: string
  assetId: string
}

export type OrderExecutionStep = {
  id: string
  stepIndex: number
  stepType: string
  provider: string
  status: string
  input?: AssetRef
  output?: AssetRef
  amountIn?: string
  minAmountOut?: string
  txHash?: string
  providerRef?: string
  startedAt?: string
  completedAt?: string
  createdAt: string
  updatedAt: string
  details: unknown
  request: unknown
  response: unknown
  error: unknown
}

export type ProviderOperation = {
  id: string
  executionStepId?: string
  provider: string
  operationType: string
  providerRef?: string
  status: string
  createdAt: string
  updatedAt: string
  request: unknown
  response: unknown
  observedState: unknown
}

export type OrderProgress = {
  totalStages: number
  completedStages: number
  failedStages: number
  activeStage?: string
}

export type OrderFirehoseRow = {
  id: string
  orderType: string
  status: string
  createdAt: string
  updatedAt: string
  source: AssetRef
  destination: AssetRef
  recipientAddress: string
  refundAddress: string
  actionTimeoutAt: string
  orderKind?: string
  quotedInputAmount?: string
  quotedOutputAmount?: string
  minAmountOut?: string
  maxAmountIn?: string
  slippageBps?: string
  quoteId?: string
  quoteProviderId?: string
  quoteExpiresAt?: string
  providerQuote?: unknown
  workflowTraceId?: string
  workflowParentSpanId?: string
  executionSteps: OrderExecutionStep[]
  providerOperations: ProviderOperation[]
  progress: OrderProgress
}

type OrderFirehoseDbRow = {
  id: string
  order_type: string
  status: string
  created_at: Date | string
  updated_at: Date | string
  source_chain_id: string
  source_asset_id: string
  destination_chain_id: string
  destination_asset_id: string
  recipient_address: string
  refund_address: string
  action_timeout_at: Date | string
  workflow_trace_id: string | null
  workflow_parent_span_id: string | null
  quote_id: string | null
  quote_provider_id: string | null
  order_kind: string | null
  quoted_amount_in: string | null
  quoted_amount_out: string | null
  min_amount_out: string | null
  max_amount_in: string | null
  slippage_bps: string | number | null
  quote_expires_at: Date | string | null
  provider_quote: unknown
  execution_steps: unknown
  provider_operations: unknown
}

const ORDER_FIREHOSE_SQL = `
WITH selected_orders AS (
  SELECT
    ro.*
  FROM public.router_orders ro
  ORDER BY ro.created_at DESC, ro.id DESC
  LIMIT $1
),
latest_quotes AS (
  SELECT DISTINCT ON (moq.order_id)
    moq.*
  FROM public.market_order_quotes moq
  WHERE moq.order_id IN (SELECT id FROM selected_orders)
  ORDER BY moq.order_id, moq.created_at DESC, moq.id DESC
)
SELECT
  ro.id::text,
  ro.order_type,
  ro.status,
  ro.created_at,
  ro.updated_at,
  ro.source_chain_id,
  ro.source_asset_id,
  ro.destination_chain_id,
  ro.destination_asset_id,
  ro.recipient_address,
  ro.refund_address,
  ro.action_timeout_at,
  ro.workflow_trace_id,
  ro.workflow_parent_span_id,
  moq.id::text AS quote_id,
  moq.provider_id AS quote_provider_id,
  COALESCE(moq.order_kind, moa.order_kind) AS order_kind,
  COALESCE(moq.amount_in, moa.amount_in) AS quoted_amount_in,
  COALESCE(moq.amount_out, moa.amount_out) AS quoted_amount_out,
  COALESCE(moq.min_amount_out, moa.min_amount_out) AS min_amount_out,
  COALESCE(moq.max_amount_in, moa.max_amount_in) AS max_amount_in,
  COALESCE(moq.slippage_bps, moa.slippage_bps) AS slippage_bps,
  moq.expires_at AS quote_expires_at,
  moq.provider_quote,
  COALESCE(steps.execution_steps, '[]'::jsonb) AS execution_steps,
  COALESCE(ops.provider_operations, '[]'::jsonb) AS provider_operations
FROM selected_orders ro
LEFT JOIN latest_quotes moq ON moq.order_id = ro.id
LEFT JOIN public.market_order_actions moa ON moa.order_id = ro.id
LEFT JOIN LATERAL (
  SELECT jsonb_agg(
    jsonb_build_object(
      'id', s.id::text,
      'stepIndex', s.step_index,
      'stepType', s.step_type,
      'provider', s.provider,
      'status', s.status,
      'input', CASE
        WHEN s.input_chain_id IS NULL THEN NULL
        ELSE jsonb_build_object('chainId', s.input_chain_id, 'assetId', s.input_asset_id)
      END,
      'output', CASE
        WHEN s.output_chain_id IS NULL THEN NULL
        ELSE jsonb_build_object('chainId', s.output_chain_id, 'assetId', s.output_asset_id)
      END,
      'amountIn', s.amount_in,
      'minAmountOut', s.min_amount_out,
      'txHash', s.tx_hash,
      'providerRef', s.provider_ref,
      'startedAt', s.started_at,
      'completedAt', s.completed_at,
      'createdAt', s.created_at,
      'updatedAt', s.updated_at,
      'details', s.details_json,
      'request', s.request_json,
      'response', s.response_json,
      'error', s.error_json
    )
    ORDER BY s.step_index ASC, s.created_at ASC, s.id ASC
  ) AS execution_steps
  FROM public.order_execution_steps s
  WHERE s.order_id = ro.id
) steps ON true
LEFT JOIN LATERAL (
  SELECT jsonb_agg(
    jsonb_build_object(
      'id', opo.id::text,
      'executionStepId', opo.execution_step_id::text,
      'provider', opo.provider,
      'operationType', opo.operation_type,
      'providerRef', opo.provider_ref,
      'status', opo.status,
      'createdAt', opo.created_at,
      'updatedAt', opo.updated_at,
      'request', opo.request_json,
      'response', opo.response_json,
      'observedState', opo.observed_state_json
    )
    ORDER BY opo.created_at ASC, opo.id ASC
  ) AS provider_operations
  FROM public.order_provider_operations opo
  WHERE opo.order_id = ro.id
) ops ON true
ORDER BY ro.created_at DESC, ro.id DESC
`

export async function fetchOrderFirehose(
  pool: Pool,
  limit: number
): Promise<OrderFirehoseRow[]> {
  const result = await pool.query<OrderFirehoseDbRow>(ORDER_FIREHOSE_SQL, [limit])
  return result.rows.map(mapOrderRow)
}

export function orderFingerprint(order: OrderFirehoseRow): string {
  return JSON.stringify(order)
}

function mapOrderRow(row: OrderFirehoseDbRow): OrderFirehoseRow {
  const executionSteps = normalizeJsonArray<OrderExecutionStep>(
    row.execution_steps
  )
  const providerOperations = normalizeJsonArray<ProviderOperation>(
    row.provider_operations
  )

  return {
    id: row.id,
    orderType: row.order_type,
    status: row.status,
    createdAt: toIso(row.created_at),
    updatedAt: toIso(row.updated_at),
    source: {
      chainId: row.source_chain_id,
      assetId: row.source_asset_id
    },
    destination: {
      chainId: row.destination_chain_id,
      assetId: row.destination_asset_id
    },
    recipientAddress: row.recipient_address,
    refundAddress: row.refund_address,
    actionTimeoutAt: toIso(row.action_timeout_at),
    orderKind: row.order_kind ?? undefined,
    quotedInputAmount: row.quoted_amount_in ?? undefined,
    quotedOutputAmount: row.quoted_amount_out ?? undefined,
    minAmountOut: row.min_amount_out ?? undefined,
    maxAmountIn: row.max_amount_in ?? undefined,
    slippageBps:
      row.slippage_bps === null || row.slippage_bps === undefined
        ? undefined
        : String(row.slippage_bps),
    quoteId: row.quote_id ?? undefined,
    quoteProviderId: row.quote_provider_id ?? undefined,
    quoteExpiresAt: row.quote_expires_at
      ? toIso(row.quote_expires_at)
      : undefined,
    providerQuote: row.provider_quote ?? undefined,
    workflowTraceId: row.workflow_trace_id ?? undefined,
    workflowParentSpanId: row.workflow_parent_span_id ?? undefined,
    executionSteps,
    providerOperations,
    progress: summarizeProgress(executionSteps, providerOperations)
  }
}

function normalizeJsonArray<T>(value: unknown): T[] {
  if (!value) return []
  if (Array.isArray(value)) return value as T[]
  if (typeof value === 'string') {
    const parsed = JSON.parse(value) as unknown
    return Array.isArray(parsed) ? (parsed as T[]) : []
  }
  return []
}

function toIso(value: Date | string): string {
  if (value instanceof Date) return value.toISOString()
  return new Date(value).toISOString()
}

function summarizeProgress(
  steps: OrderExecutionStep[],
  operations: ProviderOperation[]
): OrderProgress {
  const stages =
    steps.length > 0
      ? steps.map((step) => ({
          label: `${humanize(step.provider)} ${humanize(step.stepType)}`,
          status: step.status
        }))
      : operations.map((operation) => ({
          label: `${humanize(operation.provider)} ${humanize(operation.operationType)}`,
          status: operation.status
        }))

  const completedStages = stages.filter((stage) =>
    isCompletedStatus(stage.status)
  ).length
  const failedStages = stages.filter((stage) => isFailedStatus(stage.status)).length
  const activeStage = stages.find((stage) => !isTerminalStatus(stage.status))

  return {
    totalStages: stages.length,
    completedStages,
    failedStages,
    activeStage: activeStage
      ? `${activeStage.label} / ${humanize(activeStage.status)}`
      : undefined
  }
}

function isCompletedStatus(status: string): boolean {
  return status === 'completed' || status === 'skipped'
}

function isFailedStatus(status: string): boolean {
  return (
    status === 'failed' ||
    status === 'expired' ||
    status === 'cancelled' ||
    status === 'refund_manual_intervention_required'
  )
}

function isTerminalStatus(status: string): boolean {
  return isCompletedStatus(status) || isFailedStatus(status)
}

function humanize(value: string): string {
  return value
    .split('_')
    .filter(Boolean)
    .map((part) => part[0]?.toUpperCase() + part.slice(1))
    .join(' ')
}
