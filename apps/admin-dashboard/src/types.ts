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
}

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
  | 'failed'
  | 'refunded'
  | 'manual_refund'
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
  analyticsConfigured?: boolean
  missingAuthConfig?: string[]
  authMode?: 'google' | 'development_bypass'
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
