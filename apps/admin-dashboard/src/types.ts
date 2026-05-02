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
  quoteUsdValuation?: UsdValuation
  workflowTraceId?: string
  workflowParentSpanId?: string
  executionLegs: OrderExecutionLeg[]
  executionSteps: OrderExecutionStep[]
  providerOperations: ProviderOperation[]
  progress: OrderProgress
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
  missingAuthConfig?: string[]
  authMode?: 'google' | 'development_bypass'
}

export type OrdersResponse = {
  orders: OrderFirehoseRow[]
  nextCursor?: string
  total: number
  metrics: OrderMetrics
  sort: 'created_at_desc'
}

export type SnapshotEvent = {
  orders: OrderFirehoseRow[]
  total?: number
  metrics?: OrderMetrics
  sort: 'created_at_desc'
}

export type UpsertEvent = {
  order: OrderFirehoseRow
  total?: number
  metrics?: OrderMetrics
  sort: 'created_at_desc'
}
