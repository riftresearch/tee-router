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
}

export type OrdersResponse = {
  orders: OrderFirehoseRow[]
  sort: 'created_at_desc'
}

export type SnapshotEvent = {
  orders: OrderFirehoseRow[]
  sort: 'created_at_desc'
}

export type UpsertEvent = {
  order: OrderFirehoseRow
  sort: 'created_at_desc'
}
