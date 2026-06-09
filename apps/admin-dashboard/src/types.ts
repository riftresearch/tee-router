export type AssetRef = {
  chainId: string
  assetId: string
}

export type UsdAmountValuation = {
  raw: string
  asset: AssetRef
  canonical: string
  decimals: number
  unitUsdMicro: string
  amountUsdMicro: string
}

export type UsdValuation = {
  schemaVersion: number
  pricing?: {
    source: string
    capturedAt: string
    expiresAt?: string | null
  }
  amounts?: Record<string, UsdAmountValuation | undefined>
  legs?: Array<{
    index: number
    transitionDeclId?: string
    amounts?: Record<string, UsdAmountValuation | undefined>
  }>
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
  usdValuation?: UsdValuation
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
  executionAttemptId?: string
  transitionDeclId?: string
  legIndex: number
  legType: string
  provider: string
  status: string
  input: AssetRef
  output: AssetRef
  amountIn: string
  estimatedAmountOut: string
  actualAmountIn?: string
  actualAmountOut?: string
  startedAt?: string
  completedAt?: string
  createdAt: string
  updatedAt: string
  details: unknown
  usdValuation?: UsdValuation
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
  stages?: OrderProgressStage[]
}

export type OrderProgressStage = {
  label: string
  status: string
  input?: AssetRef
  output?: AssetRef
  txHash?: string
  txChainId?: string
}

export type LimitOrderStatus = {
  phase:
    | 'awaiting_funding'
    | 'preparing'
    | 'on_book'
    | 'filled'
    | 'completed'
    | 'refunded'
    | 'refund_required'
    | 'failed'
    | 'expired'
  label: string
  detail?: string
  tone: 'neutral' | 'success' | 'warning' | 'danger'
}

export type OrderMetrics = {
  total: number
  active: number
  needsAttention: number
}

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
  quotedInputAmount?: string
  estimatedOutputAmount?: string
  quoteId?: string
  quoteProviderId?: string
  quoteExpiresAt?: string
  providerQuote?: unknown
  quoteUsdValuation?: UsdValuation
  workflowTraceId?: string
  workflowParentSpanId?: string
  executionLegs: OrderExecutionLeg[]
  executionSteps: OrderExecutionStep[]
  providerOperations: ProviderOperation[]
  progress: OrderProgress
  limitStatus?: LimitOrderStatus
}

export type OrderTypeFilter = 'market_order' | 'limit_order'
export type OrderLifecycleFilter =
  | 'firehose'
  | 'in_progress'
  | 'needs_attention'
  | 'refunded'
export type VolumeBucketSize = 'five_minute' | 'hour' | 'day'
export type VolumeOrderTypeFilter = 'all' | OrderTypeFilter

export type VolumeBucket = {
  bucketStart: string
  bucketSize: VolumeBucketSize
  orderType: VolumeOrderTypeFilter
  volumeUsdMicro: string
  orderCount: number
}

export type VolumeAnalyticsResponse = {
  bucketSize: VolumeBucketSize
  orderType: VolumeOrderTypeFilter
  from: string
  to: string
  buckets: VolumeBucket[]
}

export type SwapTimeAverage = {
  provider: string
  legType: string
  transitionDeclId: string
  input: AssetRef
  output: AssetRef
  sampleCount: number
  avgDurationMs: number
  minDurationMs: number
  maxDurationMs: number
  lastSampleAt: string
}

export type SwapTimeAveragesResponse = {
  averages: SwapTimeAverage[]
  sort: 'last_sample_at_desc'
}

export type CurrentUser = {
  id: string
  name: string
  email: string | null
  emailVerified: boolean
  image?: string | null
}

export type MeResponse = {
  authenticated: boolean
  authorized: boolean
  user: CurrentUser | null
  allowedEmails: string[]
  routerAdminKeyConfigured?: boolean
  routerInternalApiConfigured?: boolean
  analyticsConfigured?: boolean
  chatAdminConfigured?: boolean
  missingAuthConfig?: string[]
  authMode?: 'google' | 'development_bypass'
}

export type ChatMessageRole = 'user' | 'admin'

export type ChatMessage = {
  role: ChatMessageRole
  message: string
  ts: string
}

export type ChatThread = {
  id: string
  user_eth_address: string
  meta?: unknown
  last_message_at: string
  created_at?: string
  updated_at?: string
  last_admin_msg_at?: string
  user_unread_count?: number
  has_unread?: boolean
  messages: ChatMessage[]
}

export type ChatListResponse = {
  chats: ChatThread[]
}

export type ChatThreadResponse = {
  thread: ChatThread
}

export type ChatReplyResponse = {
  ok: boolean
  result?: unknown
}

export type ProviderQuoteState = 'enabled' | 'disabled'
export type ProviderExecutionState = 'enabled' | 'drain' | 'disabled'

export type ProviderPolicy = {
  provider: string
  quote_state: ProviderQuoteState
  execution_state: ProviderExecutionState
  reason: string
  updated_by: string
  updated_at: string
}

export type RouterSwitch = {
  name: 'refund_only_mode'
  enabled: boolean
  reason: string
  updated_by: string
  updated_at: string
}

export type SwitchesResponse = {
  refund_only_mode: RouterSwitch
  provider_policies: ProviderPolicy[]
}

export type RouterSwitchEnvelope = {
  switch: RouterSwitch
}

export type ProviderPolicyEnvelope = {
  policy: ProviderPolicy
}

export type OrdersResponse = {
  orders: OrderFirehoseRow[]
  nextCursor?: string
  total?: number
  metrics?: OrderMetrics
  sort: 'created_at_desc'
}

export type OrderLookupResponse = {
  order: OrderFirehoseRow
}

export type SnapshotEvent = {
  orders: OrderFirehoseRow[]
  nextCursor?: string
  total?: number
  metrics?: OrderMetrics
  sort: 'created_at_desc'
}

export type UpsertEvent = {
  order: OrderFirehoseRow
  total?: number
  metrics?: OrderMetrics
  analyticsChanged?: boolean
  sort: 'created_at_desc'
}

export type RemoveEvent = {
  id: string
  total?: number
  metrics?: OrderMetrics
  analyticsChanged?: boolean
  sort?: 'created_at_desc'
}

export type MetricsEvent = {
  total?: number
  metrics?: OrderMetrics
  analyticsChanged?: boolean
  sort?: 'created_at_desc'
}

export type RouteCostSnapshot = {
  transitionId: string
  amountBucket: string
  provider: string
  edgeKind: string
  source: AssetRef
  destination: AssetRef
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

export type RouteCostsResponse = {
  snapshots: RouteCostSnapshot[]
  fetchedAt: string
}

export type RouteCostSampleOutcome = 'succeeded' | 'failed' | 'skipped'

export type RouteCostFailureCategory =
  | 'rate_limited'
  | 'timeout'
  | 'no_route'
  | 'upstream_server'
  | 'upstream_client'
  | 'other'

export type RouteCostSampleEvent = {
  id: string
  sampledAt: string
  provider: string
  transitionId: string
  amountBucket: string
  edgeKind: string
  source: AssetRef
  destination: AssetRef
  sampleAmountUsdMicros: number
  outcome: RouteCostSampleOutcome
  estimatedFeeBps: number | null
  estimatedLatencyMs: number | null
  reason: string | null
  failureCategory: RouteCostFailureCategory | null
}

export type RouteCostEventsResponse = {
  events: RouteCostSampleEvent[]
  windowSeconds: number
  fetchedAt: string
}

// Route graph + dry-run explain types. These mirror the snake_case JSON
// emitted directly by router-api (the dashboard server proxies the payload
// through unchanged), unlike the camelCase replica-backed types above.

export type RouteGraphNode = {
  key: string
  kind: 'external' | 'venue' | string
  chain: string
  asset: string
  canonical: string
  decimals: number
}

export type RouteGraphEdge = {
  id: string
  provider: string
  kind: string
  edge_kind: string
  from: string
  to: string
  from_chain: string
  from_asset: string
  to_chain: string
  to_asset: string
  curated: boolean
}

export type RouteGraphResponse = {
  nodes: RouteGraphNode[]
  edges: RouteGraphEdge[]
}

export type RouteExplainAsset = {
  chain: string
  asset: string
}

export type RouteExplainRequest = {
  from_asset: RouteExplainAsset
  to_asset: RouteExplainAsset
  amount_in: string
}

export type RouteTransitionView = {
  id: string
  provider: string
  kind: string
  edge_kind: string
  from_chain: string
  from_asset: string
  to_chain: string
  to_asset: string
  /** This leg's effective cost (fee + gas) in bps of the request notional,
   * exactly as the ranker scored it. `null`/absent when the leg had no fresh
   * cost and was not live-sampled. */
  cost_bps?: number | null
  /** Same effective leg cost in absolute USD micros — makes fixed-cost legs
   * legible regardless of the bps denominator. */
  cost_usd_micros?: number | null
  /** The cached snapshot's fee as bps of the *tier sample* amount — the exact
   * number the route-cost cache table shows for this leg. */
  tier_fee_bps?: number | null
}

export type RankedPathView = {
  rank: number
  path_id: string
  top_path: boolean
  winner: boolean
  hop_count: number
  missing_edges: number
  total_effective_cost_usd_micros: number
  /** Total path cost in bps; equals the sum of every leg's `cost_bps` and is
   * the value the ranker minimizes. */
  total_bps: number
  total_latency_ms: number
  transitions: RouteTransitionView[]
  estimated_amount_out: string | null
  /** Realized total cost in bps from the live end-to-end quote (output valued
   * in USD vs the request notional). The live counterpart to `total_bps`.
   * `null`/absent when the path was not live-quoted or could not be priced. */
  live_total_bps?: number | null
}

export type RouteExplainCounts = {
  paths_enumerated: number
  paths_after_executable: number
  paths_after_provider: number
  paths_after_hyperevm: number
  paths_after_same_chain: number
  ranked_count: number
  top_paths: number
}

export type RouteExplainTimings = {
  bfs_ms: number
  rank_ms: number
  live_quote_ms: number
  total_ms: number
}

export type RouteExplainGraphNode = {
  key: string
  kind: string
  chain: string
  asset: string
  canonical: string
  depth: number
  role: string
}

export type RouteExplainGraphEdge = {
  id: string
  from: string
  to: string
  provider: string
  kind: string
  edge_kind: string
}

export type RouteExplainGraph = {
  nodes: RouteExplainGraphNode[]
  edges: RouteExplainGraphEdge[]
  max_depth: number
}

export type SingleHopVenueStatus =
  | 'success'
  | 'no_route'
  | 'disabled'
  | 'timeout'
  | 'error'
  | 'invalid'

/** One single-hop venue's result in a route-explain: the cross-family
 * alternative the router compared against the multi-hop `ranked` routes. */
export type SingleHopVenueView = {
  provider: string
  status: SingleHopVenueStatus | string
  latency_ms: number
  estimated_amount_out?: string | null
  best: boolean
  error?: string | null
}

/** Cross-family winner: which family (and route/venue) produced the highest
 * output, i.e. what a real /quote would return. */
export type RouteExplainWinner = {
  family: 'multi_hop' | 'single_hop' | string
  /** Winning transition-path id (multi_hop) or provider id (single_hop). */
  label: string
  estimated_amount_out: string
}

export type RouteExplain = {
  from_asset: RouteExplainAsset
  to_asset: RouteExplainAsset
  amount_in: string
  request_usd_micros: number
  tier_label: string
  /** Sample size (USD micros) of the route-cost tier this request ranked
   * against; the tier ladder rounds the request notional up. */
  tier_sample_usd_micros?: number
  counts: RouteExplainCounts
  timings: RouteExplainTimings
  ranked: RankedPathView[]
  winner_path_id: string | null
  /** Per-transition value-loss bps for legs live-sampled during this explain
   * (notably uncached runtime Velora wrap legs). Keyed by transition id.
   * Overlaid onto the cached bps map. */
  live_bps_by_transition: Record<string, number>
  /** Single-hop venue checks queried in parallel alongside the multi-hop
   * routes (the full cross-family comparison a real /quote makes). Empty when
   * no single-hop venues are configured. */
  single_hop: SingleHopVenueView[]
  /** The cross-family winner (higher output of best multi-hop vs best
   * single-hop), i.e. what a real /quote would return. Null when neither
   * family produced a quote. */
  overall_winner?: RouteExplainWinner | null
  graph: RouteExplainGraph
}

export type RouteExplainResponse = {
  explain: RouteExplain
}
