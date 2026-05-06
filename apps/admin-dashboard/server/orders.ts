import type { Pool } from 'pg'

import { parseNonNegativeSafeInteger } from './numeric'

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
  actionAddresses?: OrderExecutionActionAddresses
}

export type OrderExecutionAddress = {
  address: string
  chainId?: string
}

export type OrderExecutionActionAddresses = {
  sender?: OrderExecutionAddress
  senders?: OrderExecutionAddress[]
  recipient?: OrderExecutionAddress
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

export type OrderTypeFilter = 'market_order' | 'limit_order'
export type OrderLifecycleFilter =
  | 'firehose'
  | 'in_progress'
  | 'failed'
  | 'refunded'
  | 'manual_refund'

export type OrderFirehoseRow = {
  id: string
  detailLevel: 'summary' | 'full'
  orderType: string
  status: string
  createdAt: string
  updatedAt: string
  fundingTxHash?: string
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
  limitStatus?: LimitOrderStatus
}

export type OrderAnalyticsRow = Pick<
  OrderFirehoseRow,
  'id' | 'orderType' | 'status' | 'createdAt' | 'updatedAt' | 'executionLegs'
>

export type LimitOrderStatus = {
  phase:
    | 'awaiting_funding'
    | 'preparing'
    | 'on_book'
    | 'filled'
    | 'completed'
    | 'refunded'
    | 'manual_refund'
    | 'failed'
    | 'expired'
  label: string
  detail?: string
  tone: 'neutral' | 'success' | 'warning' | 'danger'
}

export type OrderPageCursor = {
  createdAt: string
  id: string
}

export type CompletedOrderAnalyticsCursor = {
  createdAt: string
  id: string
}

export type CompletedOrderAnalyticsBound = CompletedOrderAnalyticsCursor

export type OrderStatusAnalyticsRow = {
  id: string
  status: string
  createdAt: string
  updatedAt: string
}

export type OrderStatusAnalyticsCursor = CompletedOrderAnalyticsCursor

export type OrderStatusAnalyticsBound = OrderStatusAnalyticsCursor

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
  funding_tx_hash: string | null
}

type OrderAnalyticsDbRow = {
  id: string
  order_type: string
  status: string
  created_at: Date | string
  updated_at: Date | string
  execution_legs: unknown
}

const ORDER_BY_ID_SQL = orderRowsSql(`
  SELECT
    ro.*
  FROM public.router_orders ro
  WHERE ro.id = $1::uuid
`)

const ORDER_SUMMARY_BY_ID_SQL = orderSummaryRowsSql(`
  SELECT
    ro.*
  FROM public.router_orders ro
  WHERE ro.id = $1::uuid
`)

const ORDER_SUMMARIES_BY_IDS_SQL = orderSummaryRowsSql(`
  SELECT
    ro.*
  FROM public.router_orders ro
  WHERE ro.id = ANY($1::uuid[])
`)

const COMPLETED_ORDERS_FOR_ANALYTICS_SQL = `
  SELECT
    ro.id::text,
    ro.order_type,
    ro.status,
    to_char(ro.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS created_at,
    to_char(
      GREATEST(ro.updated_at, COALESCE(legs.updated_at, ro.updated_at)) AT TIME ZONE 'UTC',
      'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
    ) AS updated_at,
    COALESCE(legs.execution_legs, '[]'::jsonb) AS execution_legs
  FROM public.router_orders ro
  LEFT JOIN LATERAL (
    SELECT
      jsonb_agg(
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
          'details', '{}'::jsonb,
          'usdValuation', l.usd_valuation_json
        )
        ORDER BY l.leg_index ASC, l.created_at ASC, l.id ASC
      ) AS execution_legs,
      MAX(l.updated_at) AS updated_at
    FROM public.order_execution_legs l
    WHERE l.order_id = ro.id
  ) legs ON true
  WHERE ro.status = 'completed'
    AND (
      $2::timestamptz IS NULL
      OR (ro.created_at, ro.id) < ($2::timestamptz, $3::uuid)
    )
    AND (
      $4::timestamptz IS NULL
      OR (ro.created_at, ro.id) <= ($4::timestamptz, $5::uuid)
    )
  ORDER BY ro.created_at DESC, ro.id DESC
  LIMIT $1
`

const COMPLETED_ORDER_ANALYTICS_UPPER_BOUND_SQL = `
  SELECT
    to_char(ro.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS created_at,
    ro.id::text
  FROM public.router_orders ro
  WHERE ro.status = 'completed'
  ORDER BY ro.created_at DESC, ro.id DESC
  LIMIT 1
`

const ORDER_STATUS_FOR_ANALYTICS_SQL = `
  SELECT
    ro.id::text,
    ro.status,
    to_char(ro.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS created_at,
    to_char(ro.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS updated_at
  FROM public.router_orders ro
  WHERE (
      $2::timestamptz IS NULL
      OR (ro.created_at, ro.id) < ($2::timestamptz, $3::uuid)
    )
    AND (
      $4::timestamptz IS NULL
      OR (ro.created_at, ro.id) <= ($4::timestamptz, $5::uuid)
    )
  ORDER BY ro.created_at DESC, ro.id DESC
  LIMIT $1
`

const ORDER_STATUS_ANALYTICS_UPPER_BOUND_SQL = `
  SELECT
    to_char(ro.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS created_at,
    ro.id::text
  FROM public.router_orders ro
  ORDER BY ro.created_at DESC, ro.id DESC
  LIMIT 1
`

function orderSummaryRowsSql(selectedOrdersSql: string): string {
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
),
latest_limit_quotes AS (
  SELECT DISTINCT ON (loq.order_id)
    loq.*
  FROM public.limit_order_quotes loq
  WHERE loq.order_id IN (SELECT id FROM selected_orders)
  ORDER BY loq.order_id, loq.created_at DESC, loq.id DESC
)
SELECT
  ro.id::text,
	  ro.order_type,
	  ro.status,
	  ro.created_at,
	  GREATEST(
	    ro.updated_at,
	    COALESCE(funding_vault.updated_at, ro.updated_at),
	    COALESCE(funding_step.updated_at, ro.updated_at),
	    COALESCE(moq.created_at, ro.updated_at),
	    COALESCE(loq.created_at, ro.updated_at),
	    COALESCE(moa.updated_at, ro.updated_at),
	    COALESCE(loa.updated_at, ro.updated_at),
	    COALESCE(legs.updated_at, ro.updated_at),
	    COALESCE(ops.updated_at, ro.updated_at)
	  ) AS updated_at,
  COALESCE(funding_step.tx_hash, funding_vault.funding_tx_hash) AS funding_tx_hash,
  ro.source_chain_id,
  ro.source_asset_id,
  ro.destination_chain_id,
  ro.destination_asset_id,
  ro.recipient_address,
  ro.refund_address,
  ro.action_timeout_at,
  ro.workflow_trace_id,
  ro.workflow_parent_span_id,
  COALESCE(moq.id, loq.id)::text AS quote_id,
  COALESCE(moq.provider_id, loq.provider_id) AS quote_provider_id,
  CASE
    WHEN ro.order_type = 'limit_order' THEN 'limit'
    ELSE COALESCE(moq.order_kind, moa.order_kind)
  END AS order_kind,
  COALESCE(moq.amount_in, moa.amount_in, loq.input_amount, loa.input_amount) AS quoted_amount_in,
  COALESCE(moq.amount_out, moa.amount_out, loq.output_amount, loa.output_amount) AS quoted_amount_out,
  COALESCE(moq.min_amount_out, moa.min_amount_out) AS min_amount_out,
  COALESCE(moq.max_amount_in, moa.max_amount_in) AS max_amount_in,
  COALESCE(moq.slippage_bps, moa.slippage_bps) AS slippage_bps,
  COALESCE(moq.expires_at, loq.expires_at) AS quote_expires_at,
  COALESCE(moq.provider_quote, loq.provider_quote) AS provider_quote,
  COALESCE(moq.usd_valuation_json, loq.usd_valuation_json, '{}'::jsonb) AS quote_usd_valuation,
  COALESCE(legs.execution_legs, '[]'::jsonb) AS execution_legs,
  '[]'::jsonb AS execution_steps,
  COALESCE(ops.provider_operations, '[]'::jsonb) AS provider_operations
FROM selected_orders ro
LEFT JOIN public.deposit_vaults funding_vault ON funding_vault.id = ro.funding_vault_id
LEFT JOIN LATERAL (
  SELECT s.tx_hash, s.updated_at
  FROM public.order_execution_steps s
  WHERE s.order_id = ro.id
    AND s.step_type = 'wait_for_deposit'
    AND s.tx_hash IS NOT NULL
  ORDER BY s.updated_at DESC, s.id DESC
  LIMIT 1
) funding_step ON true
LEFT JOIN latest_quotes moq ON moq.order_id = ro.id
LEFT JOIN latest_limit_quotes loq ON loq.order_id = ro.id
LEFT JOIN public.market_order_actions moa ON moa.order_id = ro.id
LEFT JOIN public.limit_order_actions loa ON loa.order_id = ro.id
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
      'details', '{}'::jsonb,
      'usdValuation', l.usd_valuation_json
    )
    ORDER BY l.leg_index ASC, l.created_at ASC, l.id ASC
	  ) AS execution_legs
	    , MAX(l.updated_at) AS updated_at
	  FROM public.order_execution_legs l
	  WHERE l.order_id = ro.id
	) legs ON true
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
      'request', '{}'::jsonb,
      'response', '{}'::jsonb,
      'observedState', '{}'::jsonb
    )
    ORDER BY opo.created_at ASC, opo.id ASC
	  ) AS provider_operations
	    , MAX(opo.updated_at) AS updated_at
	  FROM public.order_provider_operations opo
	  WHERE opo.order_id = ro.id
	) ops ON true
ORDER BY ro.created_at DESC, ro.id DESC
`
}

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
),
latest_limit_quotes AS (
  SELECT DISTINCT ON (loq.order_id)
    loq.*
  FROM public.limit_order_quotes loq
  WHERE loq.order_id IN (SELECT id FROM selected_orders)
  ORDER BY loq.order_id, loq.created_at DESC, loq.id DESC
)
SELECT
  ro.id::text,
	  ro.order_type,
	  ro.status,
	  ro.created_at,
	  GREATEST(
	    ro.updated_at,
	    COALESCE(funding_vault.updated_at, ro.updated_at),
	    COALESCE(funding_step.updated_at, ro.updated_at),
	    COALESCE(moq.created_at, ro.updated_at),
	    COALESCE(loq.created_at, ro.updated_at),
	    COALESCE(moa.updated_at, ro.updated_at),
	    COALESCE(loa.updated_at, ro.updated_at),
	    COALESCE(legs.updated_at, ro.updated_at),
	    COALESCE(steps.updated_at, ro.updated_at),
	    COALESCE(ops.updated_at, ro.updated_at)
	  ) AS updated_at,
  COALESCE(funding_step.tx_hash, funding_vault.funding_tx_hash) AS funding_tx_hash,
  ro.source_chain_id,
  ro.source_asset_id,
  ro.destination_chain_id,
  ro.destination_asset_id,
  ro.recipient_address,
  ro.refund_address,
  ro.action_timeout_at,
  ro.workflow_trace_id,
  ro.workflow_parent_span_id,
  COALESCE(moq.id, loq.id)::text AS quote_id,
  COALESCE(moq.provider_id, loq.provider_id) AS quote_provider_id,
  CASE
    WHEN ro.order_type = 'limit_order' THEN 'limit'
    ELSE COALESCE(moq.order_kind, moa.order_kind)
  END AS order_kind,
  COALESCE(moq.amount_in, moa.amount_in, loq.input_amount, loa.input_amount) AS quoted_amount_in,
  COALESCE(moq.amount_out, moa.amount_out, loq.output_amount, loa.output_amount) AS quoted_amount_out,
  COALESCE(moq.min_amount_out, moa.min_amount_out) AS min_amount_out,
  COALESCE(moq.max_amount_in, moa.max_amount_in) AS max_amount_in,
  COALESCE(moq.slippage_bps, moa.slippage_bps) AS slippage_bps,
  COALESCE(moq.expires_at, loq.expires_at) AS quote_expires_at,
  COALESCE(moq.provider_quote, loq.provider_quote) AS provider_quote,
  COALESCE(moq.usd_valuation_json, loq.usd_valuation_json, '{}'::jsonb) AS quote_usd_valuation,
  COALESCE(legs.execution_legs, '[]'::jsonb) AS execution_legs,
  COALESCE(steps.execution_steps, '[]'::jsonb) AS execution_steps,
  COALESCE(ops.provider_operations, '[]'::jsonb) AS provider_operations
FROM selected_orders ro
LEFT JOIN public.deposit_vaults funding_vault ON funding_vault.id = ro.funding_vault_id
LEFT JOIN LATERAL (
  SELECT s.tx_hash, s.updated_at
  FROM public.order_execution_steps s
  WHERE s.order_id = ro.id
    AND s.step_type = 'wait_for_deposit'
    AND s.tx_hash IS NOT NULL
  ORDER BY s.updated_at DESC, s.id DESC
  LIMIT 1
) funding_step ON true
LEFT JOIN latest_quotes moq ON moq.order_id = ro.id
LEFT JOIN latest_limit_quotes loq ON loq.order_id = ro.id
LEFT JOIN public.market_order_actions moa ON moa.order_id = ro.id
LEFT JOIN public.limit_order_actions loa ON loa.order_id = ro.id
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
	    , MAX(l.updated_at) AS updated_at
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
      'txHash', COALESCE(s.tx_hash, funding_vault.funding_tx_hash),
      'providerRef', s.provider_ref,
      'startedAt', s.started_at,
      'completedAt', s.completed_at,
      'createdAt', s.created_at,
      'updatedAt', s.updated_at,
      'details', s.details_json,
      'request', s.request_json,
      'response', s.response_json,
      'error', s.error_json,
      'usdValuation', s.usd_valuation_json,
      'actionAddresses', jsonb_strip_nulls(jsonb_build_object(
        'sender', CASE
          WHEN sender_addr.address IS NULL THEN NULL
          ELSE jsonb_strip_nulls(jsonb_build_object(
            'address', sender_addr.address,
            'chainId', sender_addr.chain_id
          ))
        END,
        'senders', sender_addresses.addresses,
        'recipient', CASE
          WHEN recipient_addr.address IS NULL THEN NULL
          ELSE jsonb_strip_nulls(jsonb_build_object(
            'address', recipient_addr.address,
            'chainId', recipient_addr.chain_id
          ))
        END
      ))
    )
    ORDER BY s.step_index ASC, s.created_at ASC, s.id ASC
	  ) AS execution_steps
	    , MAX(s.updated_at) AS updated_at
	  FROM public.order_execution_steps s
  LEFT JOIN public.deposit_vaults funding_vault
    ON funding_vault.id = ro.funding_vault_id
   AND s.step_type = 'wait_for_deposit'
  LEFT JOIN public.custody_vaults funding_custody_vault
    ON funding_custody_vault.id = funding_vault.id
  LEFT JOIN LATERAL (
    SELECT cv.address, cv.chain_id
    FROM (
      SELECT COALESCE(
        NULLIF(s.request_json->>'source_custody_vault_id', ''),
        NULLIF(s.details_json->>'source_custody_vault_id', ''),
        NULLIF(s.request_json->>'depositor_custody_vault_id', ''),
        NULLIF(s.details_json->>'depositor_custody_vault_id', ''),
        NULLIF(s.request_json->>'hyperliquid_custody_vault_id', ''),
        NULLIF(s.details_json->>'hyperliquid_custody_vault_id', '')
      ) AS vault_id
    ) vault_ref
    JOIN public.custody_vaults cv
      ON cv.id = CASE
        WHEN vault_ref.vault_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
          THEN vault_ref.vault_id::uuid
        ELSE NULL::uuid
      END
    LIMIT 1
  ) sender_vault ON true
  LEFT JOIN LATERAL (
    SELECT cv.address, cv.chain_id
    FROM (
      SELECT COALESCE(
        NULLIF(s.request_json->>'recipient_custody_vault_id', ''),
        NULLIF(s.details_json->>'recipient_custody_vault_id', ''),
        NULLIF(s.request_json->>'destination_custody_vault_id', ''),
        NULLIF(s.details_json->>'destination_custody_vault_id', ''),
        NULLIF(s.details_json->>'custody_vault_id', ''),
        NULLIF(s.details_json->>'vault_id', '')
      ) AS vault_id
    ) vault_ref
    JOIN public.custody_vaults cv
      ON cv.id = CASE
        WHEN vault_ref.vault_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
          THEN vault_ref.vault_id::uuid
        ELSE NULL::uuid
      END
    LIMIT 1
  ) recipient_vault ON true
  LEFT JOIN LATERAL (
    SELECT pa.address, pa.chain_id
    FROM public.order_provider_addresses pa
    WHERE pa.execution_step_id = s.id
      AND pa.role IN (
        'across_recipient',
        'unit_deposit',
        'hyperliquid_destination'
      )
    ORDER BY pa.updated_at DESC, pa.created_at DESC, pa.id DESC
    LIMIT 1
  ) provider_recipient_addr ON true
  LEFT JOIN LATERAL (
    SELECT
      COALESCE(
        CASE
          WHEN s.step_type = 'wait_for_deposit'
            THEN NULLIF(funding_vault.funding_sender_address, '')
          ELSE NULL
        END,
        sender_vault.address,
        NULLIF(s.request_json->>'source_custody_vault_address', ''),
        NULLIF(s.details_json->>'source_custody_vault_address', ''),
        NULLIF(s.request_json->>'depositor_custody_vault_address', ''),
        NULLIF(s.details_json->>'depositor_custody_vault_address', ''),
        NULLIF(s.request_json->>'hyperliquid_custody_vault_address', ''),
        NULLIF(s.details_json->>'hyperliquid_custody_vault_address', ''),
        NULLIF(s.response_json->>'sender_address', ''),
        NULLIF(s.request_json->>'sender_address', ''),
        NULLIF(s.details_json->>'sender_address', ''),
        NULLIF(s.response_json #>> '{provider_context,user}', ''),
        NULLIF(s.response_json #>> '{observed_state,provider_observed_state,sourceAddress}', '')
      ) AS address,
      CASE
        WHEN s.step_type IN ('hyperliquid_trade', 'unit_withdrawal') THEN 'hyperliquid'
        ELSE COALESCE(
          funding_custody_vault.chain_id,
          sender_vault.chain_id,
          NULLIF(s.request_json->>'source_chain_id', ''),
          NULLIF(s.request_json->>'src_chain_id', ''),
          NULLIF(s.request_json->>'input_chain_id', ''),
          s.input_chain_id
        )
      END AS chain_id
  ) sender_addr ON true
  LEFT JOIN LATERAL (
    SELECT COALESCE(
      jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
        'address', sender_address,
        'chainId', sender_addr.chain_id
      ))) FILTER (WHERE sender_address IS NOT NULL AND sender_address <> ''),
      '[]'::jsonb
    ) AS addresses
    FROM (
      SELECT DISTINCT sender_address
      FROM (
        SELECT jsonb_array_elements_text(
          CASE
            WHEN jsonb_typeof(funding_vault.funding_sender_addresses) = 'array'
              THEN funding_vault.funding_sender_addresses
            ELSE '[]'::jsonb
          END
        ) AS sender_address
        UNION ALL
        SELECT jsonb_array_elements_text(
          CASE
            WHEN jsonb_typeof(s.response_json->'sender_addresses') = 'array'
              THEN s.response_json->'sender_addresses'
            ELSE '[]'::jsonb
          END
        ) AS sender_address
        UNION ALL
        SELECT NULLIF(funding_vault.funding_sender_address, '') AS sender_address
        UNION ALL
        SELECT NULLIF(s.response_json->>'sender_address', '') AS sender_address
      ) sender_candidates
    ) sender_address_rows
  ) sender_addresses ON true
  LEFT JOIN LATERAL (
    SELECT
      COALESCE(
        recipient_vault.address,
        provider_recipient_addr.address,
        funding_custody_vault.address,
        NULLIF(funding_vault.funding_recipient_address, ''),
        NULLIF(s.request_json->>'recipient_custody_vault_address', ''),
        NULLIF(s.details_json->>'recipient_custody_vault_address', ''),
        NULLIF(s.request_json->>'destination_custody_vault_address', ''),
        NULLIF(s.details_json->>'destination_custody_vault_address', ''),
        NULLIF(s.response_json->>'recipient_address', ''),
        NULLIF(s.request_json->>'recipient_address', ''),
        NULLIF(s.details_json->>'recipient_address', ''),
        NULLIF(s.response_json #>> '{observed_state,provider_observed_state,destinationAddress}', ''),
        NULLIF(s.response_json #>> '{observed_state,provider_observed_state,protocolAddress}', ''),
        NULLIF(s.response_json->>'provider_ref', ''),
        NULLIF(s.provider_ref, ''),
        CASE
          WHEN s.step_type = 'hyperliquid_bridge_deposit' THEN sender_addr.address
          ELSE NULL
        END,
        CASE
          WHEN s.step_type = 'hyperliquid_trade' THEN sender_addr.address
          ELSE NULL
        END
      ) AS address,
      CASE
        WHEN s.step_type = 'unit_deposit' THEN COALESCE(s.input_chain_id, NULLIF(s.request_json->>'src_chain_id', ''))
        WHEN s.step_type = 'hyperliquid_bridge_deposit' THEN 'hyperliquid'
        WHEN s.step_type = 'hyperliquid_trade' THEN COALESCE(s.output_chain_id, 'hyperliquid')
        ELSE COALESCE(
          funding_custody_vault.chain_id,
          recipient_vault.chain_id,
          provider_recipient_addr.chain_id,
          NULLIF(s.request_json->>'dst_chain_id', ''),
          NULLIF(s.request_json->>'destination_chain_id', ''),
          NULLIF(s.request_json->>'output_chain_id', ''),
          s.output_chain_id
        )
      END AS chain_id
  ) recipient_addr ON true
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
	    , MAX(opo.updated_at) AS updated_at
	  FROM public.order_provider_operations opo
	  WHERE opo.order_id = ro.id
	) ops ON true
ORDER BY ro.created_at DESC, ro.id DESC
`
}

export async function fetchOrderFirehose(
  pool: Pool,
  limit: number,
  cursor?: OrderPageCursor,
  orderType?: OrderTypeFilter,
  lifecycleFilter?: OrderLifecycleFilter
): Promise<OrderFirehoseRow[]> {
  const query = buildOrderFirehoseQuery(limit, cursor, orderType, lifecycleFilter)
  const result = await pool.query<OrderFirehoseDbRow>(query.sql, query.values)
  return result.rows.map((row) => mapOrderRow(row, 'summary'))
}

function buildOrderFirehoseQuery(
  limit: number,
  cursor?: OrderPageCursor,
  orderType?: OrderTypeFilter,
  lifecycleFilter?: OrderLifecycleFilter
): { sql: string; values: unknown[] } {
  const values: unknown[] = [limit, cursor?.createdAt ?? null, cursor?.id ?? null]
  const predicates = [
    `(
      $2::timestamptz IS NULL
      OR (ro.created_at, ro.id) < ($2::timestamptz, $3::uuid)
    )`
  ]

  if (orderType) {
    values.push(orderType)
    predicates.push(`ro.order_type = $${values.length}::text`)
  }

  const lifecyclePredicate = orderLifecycleSqlPredicate(lifecycleFilter)
  if (lifecyclePredicate) predicates.push(lifecyclePredicate)

  return {
    sql: orderSummaryRowsSql(`
      SELECT
        ro.*
      FROM public.router_orders ro
      WHERE ${predicates.join('\n        AND ')}
      ORDER BY ro.created_at DESC, ro.id DESC
      LIMIT $1
    `),
    values
  }
}

function orderLifecycleSqlPredicate(
  lifecycleFilter?: OrderLifecycleFilter
): string | undefined {
  if (!lifecycleFilter || lifecycleFilter === 'firehose') return undefined
  if (lifecycleFilter === 'in_progress') {
    return `
      ro.status NOT IN (
        'completed',
        'refunded',
        'failed',
        'expired',
        'manual_intervention_required',
        'refund_manual_intervention_required'
      )
      AND EXISTS (
        SELECT 1
        FROM public.order_execution_steps funding_step
        LEFT JOIN public.deposit_vaults funding_vault
          ON funding_vault.id = ro.funding_vault_id
        WHERE funding_step.order_id = ro.id
          AND funding_step.step_type = 'wait_for_deposit'
          AND COALESCE(funding_step.tx_hash, funding_vault.funding_tx_hash) IS NOT NULL
      )
    `
  }
  if (lifecycleFilter === 'failed') {
    return `
      ro.status IN (
        'failed',
        'expired',
        'refund_required',
        'refunding',
        'manual_intervention_required',
        'refund_manual_intervention_required'
      )
    `
  }
  if (lifecycleFilter === 'refunded') return `ro.status = 'refunded'`
  if (lifecycleFilter === 'manual_refund') {
    return `ro.status = 'refund_manual_intervention_required'`
  }
  return assertNeverOrderLifecycleFilter(lifecycleFilter)
}

function assertNeverOrderLifecycleFilter(value: never): never {
  throw new Error(`unhandled order lifecycle filter: ${value}`)
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
          'manual_intervention_required',
          'refund_manual_intervention_required'
        )
      )::text AS needs_attention
    FROM public.router_orders
  `)
  const row = result.rows[0] ?? {
    total: '0',
    active: '0',
    needs_attention: '0'
  }

  return {
    total: parseNonNegativeSafeInteger(row.total, 'order metric total'),
    active: parseNonNegativeSafeInteger(row.active, 'order metric active'),
    needsAttention: parseNonNegativeSafeInteger(
      row.needs_attention,
      'order metric needs_attention'
    )
  }
}

export async function fetchOrderById(
  pool: Pool,
  id: string
): Promise<OrderFirehoseRow | undefined> {
  const result = await pool.query<OrderFirehoseDbRow>(ORDER_BY_ID_SQL, [id])
  return result.rows[0] ? mapOrderRow(result.rows[0], 'full') : undefined
}

export async function fetchOrderSummaryById(
  pool: Pool,
  id: string
): Promise<OrderFirehoseRow | undefined> {
  const result = await pool.query<OrderFirehoseDbRow>(ORDER_SUMMARY_BY_ID_SQL, [
    id
  ])
  return result.rows[0] ? mapOrderRow(result.rows[0], 'summary') : undefined
}

export async function fetchOrderSummariesByIds(
  pool: Pool,
  ids: string[]
): Promise<Map<string, OrderFirehoseRow>> {
  const uniqueIds = [...new Set(ids)]
  if (uniqueIds.length === 0) return new Map()

  const result = await pool.query<OrderFirehoseDbRow>(ORDER_SUMMARIES_BY_IDS_SQL, [
    uniqueIds
  ])
  return new Map(
    result.rows.map((row) => {
      const order = mapOrderRow(row, 'summary')
      return [order.id, order]
    })
  )
}

export async function fetchCompletedOrdersForAnalytics(
  pool: Pool,
  limit: number,
  cursor?: CompletedOrderAnalyticsCursor,
  upperBound?: CompletedOrderAnalyticsBound
): Promise<OrderAnalyticsRow[]> {
  const result = await pool.query<OrderAnalyticsDbRow>(
    COMPLETED_ORDERS_FOR_ANALYTICS_SQL,
    [
      limit,
      cursor?.createdAt ?? null,
      cursor?.id ?? null,
      upperBound?.createdAt ?? null,
      upperBound?.id ?? null
    ]
  )
  return result.rows.map(mapOrderAnalyticsRow)
}

export async function fetchCompletedOrderAnalyticsUpperBound(
  pool: Pool
): Promise<CompletedOrderAnalyticsBound | undefined> {
  const result = await pool.query<{ created_at: Date | string; id: string }>(
    COMPLETED_ORDER_ANALYTICS_UPPER_BOUND_SQL
  )
  const row = result.rows[0]
  if (!row) return undefined
  return {
    createdAt: toAnalyticsCursorIso(row.created_at),
    id: row.id
  }
}

export async function fetchOrderStatusesForAnalytics(
  pool: Pool,
  limit: number,
  cursor?: OrderStatusAnalyticsCursor,
  upperBound?: OrderStatusAnalyticsBound
): Promise<OrderStatusAnalyticsRow[]> {
  const result = await pool.query<{
    id: string
    status: string
    created_at: Date | string
    updated_at: Date | string
  }>(ORDER_STATUS_FOR_ANALYTICS_SQL, [
    limit,
    cursor?.createdAt ?? null,
    cursor?.id ?? null,
    upperBound?.createdAt ?? null,
    upperBound?.id ?? null
  ])
  return result.rows.map((row) => ({
    id: row.id,
    status: row.status,
    createdAt: toAnalyticsCursorIso(row.created_at),
    updatedAt: toAnalyticsCursorIso(row.updated_at)
  }))
}

export async function fetchOrderStatusAnalyticsUpperBound(
  pool: Pool
): Promise<OrderStatusAnalyticsBound | undefined> {
  const result = await pool.query<{ created_at: Date | string; id: string }>(
    ORDER_STATUS_ANALYTICS_UPPER_BOUND_SQL
  )
  const row = result.rows[0]
  if (!row) return undefined
  return {
    createdAt: toAnalyticsCursorIso(row.created_at),
    id: row.id
  }
}

function mapOrderRow(
  row: OrderFirehoseDbRow,
  detailLevel: 'summary' | 'full'
): OrderFirehoseRow {
  const executionLegs = normalizeJsonArray<OrderExecutionLeg>(
    row.execution_legs,
    'execution_legs'
  )
  const executionSteps = normalizeJsonArray<OrderExecutionStep>(
    row.execution_steps,
    'execution_steps'
  )
  const providerOperations = normalizeJsonArray<ProviderOperation>(
    row.provider_operations,
    'provider_operations'
  )

  return {
    id: row.id,
    detailLevel,
    orderType: row.order_type,
    status: row.status,
    createdAt: toIso(row.created_at),
    updatedAt: toIso(row.updated_at),
    fundingTxHash: row.funding_tx_hash ?? undefined,
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
    providerQuote: detailLevel === 'full' ? row.provider_quote ?? undefined : undefined,
    quoteUsdValuation: row.quote_usd_valuation ?? undefined,
    workflowTraceId: row.workflow_trace_id ?? undefined,
    workflowParentSpanId: row.workflow_parent_span_id ?? undefined,
    executionLegs,
    executionSteps: detailLevel === 'full' ? executionSteps : [],
    providerOperations: detailLevel === 'full' ? providerOperations : [],
    progress: summarizeProgress(
      row.provider_quote,
      executionLegs,
      providerOperations
    ),
    limitStatus:
      row.order_type === 'limit_order'
        ? summarizeLimitOrderStatus(row.status, executionSteps, providerOperations)
        : undefined
  }
}

function mapOrderAnalyticsRow(row: OrderAnalyticsDbRow): OrderAnalyticsRow {
  return {
    id: row.id,
    orderType: row.order_type,
    status: row.status,
    createdAt: toAnalyticsCursorIso(row.created_at),
    updatedAt: toAnalyticsCursorIso(row.updated_at),
    executionLegs: normalizeJsonArray<OrderExecutionLeg>(
      row.execution_legs,
      'execution_legs'
    )
  }
}

export function orderMatchesLifecycleFilter(
  order: OrderFirehoseRow,
  filter: OrderLifecycleFilter | undefined
): boolean {
  if (!filter || filter === 'firehose') return true
  if (filter === 'in_progress') {
    return (
      ![
        'completed',
        'refunded',
        'failed',
        'expired',
        'manual_intervention_required',
        'refund_manual_intervention_required'
      ].includes(order.status) &&
      (Boolean(order.fundingTxHash) ||
        order.executionSteps.some(
          (step) => step.stepType === 'wait_for_deposit' && Boolean(step.txHash)
        ))
    )
  }
  if (filter === 'refunded') return order.status === 'refunded'
  if (filter === 'manual_refund') {
    return order.status === 'refund_manual_intervention_required'
  }
  return [
    'failed',
    'refund_required',
    'refunding',
    'manual_intervention_required',
    'refund_manual_intervention_required'
  ].includes(order.status)
}

function summarizeLimitOrderStatus(
  orderStatus: string,
  steps: OrderExecutionStep[],
  operations: ProviderOperation[]
): LimitOrderStatus {
  if (orderStatus === 'completed') {
    return {
      phase: 'completed',
      label: 'Completed',
      detail: 'Limit fill settled',
      tone: 'success'
    }
  }
  if (orderStatus === 'refunded') {
    return {
      phase: 'refunded',
      label: 'Refunded',
      detail: 'Funds returned',
      tone: 'neutral'
    }
  }
  if (
    orderStatus === 'manual_intervention_required' ||
    orderStatus === 'refund_manual_intervention_required'
  ) {
    return {
      phase:
        orderStatus === 'refund_manual_intervention_required'
          ? 'manual_refund'
          : 'failed',
      label:
        orderStatus === 'refund_manual_intervention_required'
          ? 'Manual Refund'
          : 'Manual Intervention',
      detail: 'Operator action required',
      tone: 'danger'
    }
  }
  if (orderStatus === 'failed' || orderStatus === 'refund_required') {
    return {
      phase: 'failed',
      label: orderStatus === 'refund_required' ? 'Refund Required' : 'Failed',
      detail: 'Execution did not finish cleanly',
      tone: 'danger'
    }
  }
  if (orderStatus === 'expired') {
    return {
      phase: 'expired',
      label: 'Expired',
      detail: 'Order deadline passed',
      tone: 'danger'
    }
  }

  const limitOperation = operations.find(
    (operation) => operation.operationType === 'hyperliquid_limit_order'
  )
  const limitStep = steps.find(
    (step) => step.stepType === 'hyperliquid_limit_order'
  )
  const fundingStep = steps.find((step) => step.stepType === 'wait_for_deposit')
  if (limitOperation?.status === 'waiting_external') {
    return {
      phase: 'on_book',
      label: 'On Book',
      detail: limitOperation.providerRef
        ? `Hyperliquid ${limitOperation.providerRef}`
        : 'Resting on Hyperliquid',
      tone: 'warning'
    }
  }
  if (limitOperation?.status === 'completed') {
    return {
      phase: 'filled',
      label: 'Filled',
      detail: 'Fill observed, settling route',
      tone: 'warning'
    }
  }
  if (limitOperation?.status === 'failed' || limitOperation?.status === 'expired') {
    return {
      phase: limitOperation.status === 'expired' ? 'expired' : 'failed',
      label: limitOperation.status === 'expired' ? 'Limit Expired' : 'Limit Failed',
      detail: 'Hyperliquid order left the book unsuccessfully',
      tone: 'danger'
    }
  }
  if (
    limitOperation?.status === 'submitted' ||
    limitStep?.status === 'running' ||
    limitStep?.status === 'waiting'
  ) {
    return {
      phase: 'preparing',
      label: 'Placing Limit',
      detail: 'Submitting to Hyperliquid',
      tone: 'warning'
    }
  }
  if (fundingStep?.txHash || ['funded', 'executing'].includes(orderStatus)) {
    return {
      phase: 'preparing',
      label: 'Preparing',
      detail: 'Funding received, moving funds to Hyperliquid',
      tone: 'warning'
    }
  }
  return {
    phase: 'awaiting_funding',
    label: 'Awaiting Funding',
    detail: 'No funding transaction yet',
    tone: 'neutral'
  }
}

function normalizeJsonArray<T>(value: unknown, field: string): T[] {
  if (value === null || value === undefined) return []
  if (Array.isArray(value)) return value as T[]
  if (typeof value === 'string') {
    const parsed = JSON.parse(value) as unknown
    if (!Array.isArray(parsed)) {
      throw new Error(`${field} JSON aggregate must be an array`)
    }
    return parsed as T[]
  }
  throw new Error(`${field} JSON aggregate must be an array`)
}

function toIso(value: Date | string): string {
  if (value instanceof Date) return value.toISOString()
  return new Date(value).toISOString()
}

function toAnalyticsCursorIso(value: Date | string): string {
  if (value instanceof Date) return value.toISOString()
  return value
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
    label: humanize(operation.provider),
    status: operation.status
  }))
  const actualStages = legs.map((leg) => ({
    transitionDeclId: leg.transitionDeclId,
    transitionMatchIds: leg.transitionDeclId ? [leg.transitionDeclId] : [],
    label: humanize(leg.provider),
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
      label: humanize(provider),
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
  return humanize(stage.provider)
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
    status === 'manual_intervention_required' ||
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
