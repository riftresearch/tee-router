import type { Pool } from 'pg'

export type AssetRef = {
  chainId: string
  assetId: string
}

export type OrderExecutionStep = {
  id: string
  executionLegId?: string
  transitionDeclId?: string
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
  usdValuation?: unknown
}

export type OrderExecutionLeg = {
  id: string
  transitionDeclId?: string
  legIndex: number
  legType: string
  provider: string
  status: string
  input: AssetRef
  output: AssetRef
  amountIn: string
  expectedAmountOut: string
  minAmountOut?: string
  actualAmountIn?: string
  actualAmountOut?: string
  startedAt?: string
  completedAt?: string
  createdAt: string
  updatedAt: string
  details: unknown
  usdValuation?: unknown
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

export type OrderMetrics = {
  total: number
  active: number
  needsAttention: number
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
  quoteUsdValuation?: unknown
  workflowTraceId?: string
  workflowParentSpanId?: string
  executionLegs: OrderExecutionLeg[]
  executionSteps: OrderExecutionStep[]
  providerOperations: ProviderOperation[]
  progress: OrderProgress
}

export type OrderPageCursor = {
  createdAt: string
  id: string
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
  quote_usd_valuation: unknown
  execution_legs: unknown
  execution_steps: unknown
  provider_operations: unknown
}

const ORDER_FIREHOSE_SQL = orderRowsSql(`
  SELECT
    ro.*
  FROM public.router_orders ro
  WHERE (
    $2::timestamptz IS NULL
    OR (ro.created_at, ro.id) < ($2::timestamptz, $3::uuid)
  )
  ORDER BY ro.created_at DESC, ro.id DESC
  LIMIT $1
`)

const ORDER_BY_ID_SQL = orderRowsSql(`
  SELECT
    ro.*
  FROM public.router_orders ro
  WHERE ro.id = $1::uuid
`)

function orderRowsSql(selectedOrdersSql: string): string {
  return `
WITH selected_orders AS (
${selectedOrdersSql.trimEnd()}
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
  moq.usd_valuation_json AS quote_usd_valuation,
  COALESCE(legs.execution_legs, '[]'::jsonb) AS execution_legs,
  COALESCE(steps.execution_steps, '[]'::jsonb) AS execution_steps,
  COALESCE(ops.provider_operations, '[]'::jsonb) AS provider_operations
FROM selected_orders ro
LEFT JOIN latest_quotes moq ON moq.order_id = ro.id
LEFT JOIN public.market_order_actions moa ON moa.order_id = ro.id
LEFT JOIN LATERAL (
  SELECT jsonb_agg(
    jsonb_build_object(
      'id', l.id::text,
      'transitionDeclId', l.transition_decl_id,
      'legIndex', l.leg_index,
      'legType', l.leg_type,
      'provider', l.provider,
      'status', l.status,
      'input', jsonb_build_object('chainId', l.input_chain_id, 'assetId', l.input_asset_id),
      'output', jsonb_build_object('chainId', l.output_chain_id, 'assetId', l.output_asset_id),
      'amountIn', l.amount_in,
      'expectedAmountOut', l.expected_amount_out,
      'minAmountOut', l.min_amount_out,
      'actualAmountIn', l.actual_amount_in,
      'actualAmountOut', l.actual_amount_out,
      'startedAt', l.started_at,
      'completedAt', l.completed_at,
      'createdAt', l.created_at,
      'updatedAt', l.updated_at,
      'details', l.details_json,
      'usdValuation', l.usd_valuation_json
    )
    ORDER BY l.leg_index ASC, l.created_at ASC, l.id ASC
  ) AS execution_legs
  FROM public.order_execution_legs l
  WHERE l.order_id = ro.id
) legs ON true
LEFT JOIN LATERAL (
  SELECT jsonb_agg(
    jsonb_build_object(
      'id', s.id::text,
      'executionLegId', s.execution_leg_id::text,
      'transitionDeclId', s.transition_decl_id,
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
      'error', s.error_json,
      'usdValuation', s.usd_valuation_json
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
}

export async function fetchOrderFirehose(
  pool: Pool,
  limit: number,
  cursor?: OrderPageCursor
): Promise<OrderFirehoseRow[]> {
  const result = await pool.query<OrderFirehoseDbRow>(ORDER_FIREHOSE_SQL, [
    limit,
    cursor?.createdAt ?? null,
    cursor?.id ?? null
  ])
  return result.rows.map(mapOrderRow)
}

export async function fetchOrderCount(pool: Pool): Promise<number> {
  const result = await pool.query<{ total: string }>(
    'SELECT COUNT(*)::text AS total FROM public.router_orders'
  )
  return Number(result.rows[0]?.total ?? 0)
}

export async function fetchOrderMetrics(pool: Pool): Promise<OrderMetrics> {
  const result = await pool.query<{
    total: string
    active: string
    needs_attention: string
  }>(`
    SELECT
      COUNT(*)::text AS total,
      COUNT(*) FILTER (
        WHERE status IN (
          'pending_funding',
          'funded',
          'executing',
          'refund_required',
          'refunding'
        )
      )::text AS active,
      COUNT(*) FILTER (
        WHERE status IN (
          'failed',
          'expired',
          'refund_manual_intervention_required'
        )
      )::text AS needs_attention
    FROM public.router_orders
  `)

  const row = result.rows[0]
  return {
    total: Number(row?.total ?? 0),
    active: Number(row?.active ?? 0),
    needsAttention: Number(row?.needs_attention ?? 0)
  }
}

export async function fetchOrderById(
  pool: Pool,
  id: string
): Promise<OrderFirehoseRow | undefined> {
  const result = await pool.query<OrderFirehoseDbRow>(ORDER_BY_ID_SQL, [id])
  return result.rows[0] ? mapOrderRow(result.rows[0]) : undefined
}

function mapOrderRow(row: OrderFirehoseDbRow): OrderFirehoseRow {
  const executionLegs = normalizeJsonArray<OrderExecutionLeg>(row.execution_legs)
  const executionSteps = normalizeJsonArray<OrderExecutionStep>(row.execution_steps)
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
    quoteUsdValuation: row.quote_usd_valuation ?? undefined,
    workflowTraceId: row.workflow_trace_id ?? undefined,
    workflowParentSpanId: row.workflow_parent_span_id ?? undefined,
    executionLegs,
    executionSteps,
    providerOperations,
    progress: summarizeProgress(
      row.provider_quote,
      executionLegs,
      providerOperations
    )
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
  providerQuote: unknown,
  legs: OrderExecutionLeg[],
  operations: ProviderOperation[]
): OrderProgress {
  const plannedStages = extractPlannedStages(providerQuote)
  const operationStages = operations.map((operation) => ({
    transitionDeclId: undefined,
    transitionMatchIds: [],
    label: `${humanize(operation.provider)} ${humanize(operation.operationType)}`,
    status: operation.status
  }))
  const actualStages = legs.map((leg) => ({
    transitionDeclId: leg.transitionDeclId,
    transitionMatchIds: leg.transitionDeclId ? [leg.transitionDeclId] : [],
    label: `${humanize(leg.provider)} ${humanize(leg.legType)}`,
    status: leg.status
  }))
  const stages =
    actualStages.length > 0
      ? actualStages
      : plannedStages.length > 0
      ? plannedStages.map((planned) => {
          return {
            ...planned,
            status: 'planned'
          }
        })
      : operationStages

  const completedStages = stages.filter((stage) =>
    isCompletedStatus(stage.status)
  ).length
  const failedStages = stages.filter((stage) =>
    isFailedStatus(stage.status)
  ).length
  const activeStage = stages.find((stage) => !isTerminalStatus(stage.status))

  return {
    totalStages: stages.length,
    completedStages: Math.min(completedStages, stages.length),
    failedStages: Math.min(failedStages, stages.length),
    activeStage: activeStage
      ? `${stageLabel(activeStage)} / ${humanize(activeStage.status)}`
      : undefined
  }
}

function latestStepsByStage(steps: OrderExecutionStep[]): OrderExecutionStep[] {
  const byStage = new Map<string, OrderExecutionStep>()
  for (const step of steps) {
    byStage.set(step.transitionDeclId ?? step.id, step)
  }
  return [...byStage.values()].sort(
    (left, right) =>
      left.stepIndex - right.stepIndex ||
      left.createdAt.localeCompare(right.createdAt) ||
      left.id.localeCompare(right.id)
  )
}

type ProgressStage = {
  transitionDeclId?: string
  transitionMatchIds: string[]
  label: string
  status: string
}

function latestMatchingStep(
  steps: OrderExecutionStep[],
  matchIds: string[]
): OrderExecutionStep | undefined {
  if (matchIds.length === 0) return undefined

  let latestExact: OrderExecutionStep | undefined
  let latestPrefix: OrderExecutionStep | undefined
  for (const step of steps) {
    const stepId = step.transitionDeclId
    if (!stepId) continue
    if (matchIds.includes(stepId)) {
      latestExact = step
    } else if (matchIds.some((matchId) => stepId.startsWith(`${matchId}:`))) {
      latestPrefix = step
    }
  }

  return latestExact ?? latestPrefix
}

function extractPlannedStages(providerQuote: unknown): ProgressStage[] {
  const quote = asRecord(providerQuote)
  const legs = Array.isArray(quote?.legs) ? quote.legs : []
  const stages: ProgressStage[] = []
  for (const leg of legs) {
    const record = asRecord(leg)
    if (!record) continue
    const provider = stringField(record.provider)
    const transitionKind = stringField(record.transition_kind)
    const executionStepType = stringField(record.execution_step_type)
    const transitionDeclId = stringField(record.transition_decl_id)
    const transitionParentDeclId = stringField(record.transition_parent_decl_id)
    if (!provider || !transitionKind) continue
    const transitionMatchIds = uniqueStrings([
      transitionDeclId,
      transitionParentDeclId
    ])
    stages.push({
      ...(transitionDeclId ? { transitionDeclId } : {}),
      transitionMatchIds,
      label: `${humanize(provider)} ${humanize(executionStepType ?? transitionKind)}`,
      status: 'planned'
    })
  }
  return stages
}

function uniqueStrings(values: (string | undefined)[]): string[] {
  return [...new Set(values.filter((value): value is string => Boolean(value)))]
}

function stageLabel(stage: ProgressStage | OrderExecutionStep): string {
  if ('label' in stage) return stage.label
  return `${humanize(stage.provider)} ${humanize(stage.stepType)}`
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === 'object'
    ? (value as Record<string, unknown>)
    : undefined
}

function stringField(value: unknown): string | undefined {
  return typeof value === 'string' && value.length > 0 ? value : undefined
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
