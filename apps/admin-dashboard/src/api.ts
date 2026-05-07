import type {
  MeResponse,
  MetricsEvent,
  OrderLifecycleFilter,
  OrderLookupResponse,
  OrdersResponse,
  OrderTypeFilter,
  RemoveEvent,
  SnapshotEvent,
  UpsertEvent,
  VolumeAnalyticsResponse,
  VolumeBucketSize,
  VolumeOrderTypeFilter
} from './types'

const ADMIN_API_MAX_RESPONSE_BYTES = 8 * 1024 * 1024

export async function fetchMe(): Promise<MeResponse> {
  const response = await fetch('/api/me', {
    credentials: 'include'
  })

  if (!response.ok && response.status !== 401 && response.status !== 503) {
    throw new Error(`Failed to load session: HTTP ${response.status}`)
  }

  const payload = await readJsonResponse(response, 'session')
  if (!isMeResponse(payload)) throw new Error('Invalid session response payload')
  return payload
}

export async function fetchOrders(
  limit = 100,
  cursor?: string,
  orderType?: OrderTypeFilter,
  filter?: OrderLifecycleFilter,
  includeMetrics = true
): Promise<OrdersResponse> {
  const params = new URLSearchParams({ limit: String(limit) })
  if (cursor) params.set('cursor', cursor)
  if (orderType) params.set('orderType', orderType)
  if (filter && filter !== 'firehose') params.set('filter', filter)
  if (!includeMetrics) params.set('metrics', 'false')

  const response = await fetch(`/api/orders?${params.toString()}`, {
    credentials: 'include'
  })

  if (!response.ok) {
    throw new Error(`Failed to load orders: HTTP ${response.status}`)
  }

  const payload = await readJsonResponse(response, 'orders')
  if (!isOrdersResponse(payload)) throw new Error('Invalid orders response payload')
  return payload
}

export async function fetchOrderById(
  orderId: string
): Promise<OrderLookupResponse> {
  const response = await fetch(`/api/orders/${encodeURIComponent(orderId)}`, {
    credentials: 'include'
  })

  if (!response.ok) {
    if (response.status === 404) throw new Error('Order not found')
    throw new Error(`Failed to load order: HTTP ${response.status}`)
  }

  const payload = await readJsonResponse(response, 'order lookup')
  if (!isOrderLookupResponse(payload)) {
    throw new Error('Invalid order lookup response payload')
  }
  return payload
}

export async function fetchVolumeAnalytics({
  bucketSize,
  orderType,
  from,
  to
}: {
  bucketSize: VolumeBucketSize
  orderType: VolumeOrderTypeFilter
  from: string
  to: string
}): Promise<VolumeAnalyticsResponse> {
  const params = new URLSearchParams({
    bucketSize,
    orderType,
    from,
    to
  })
  const response = await fetch(`/api/analytics/volume?${params.toString()}`, {
    credentials: 'include'
  })

  if (!response.ok) {
    throw new Error(`Failed to load volume analytics: HTTP ${response.status}`)
  }

  const payload = await readJsonResponse(response, 'volume analytics')
  if (!isVolumeAnalyticsResponse(payload)) {
    throw new Error('Invalid volume analytics response payload')
  }
  return payload
}

export function parseSnapshotEvent(event: MessageEvent<string>): SnapshotEvent {
  const payload = parseStreamEventPayload(event, 'snapshot')
  if (
    !isRecord(payload) ||
    !isOrderArray(payload.orders) ||
    (payload.nextCursor !== undefined && typeof payload.nextCursor !== 'string') ||
    (payload.total !== undefined && !isNonNegativeSafeInteger(payload.total)) ||
    (payload.sort !== undefined && payload.sort !== 'created_at_desc') ||
    (payload.metrics !== undefined && !isMetrics(payload.metrics))
  ) {
    throw new Error('Invalid order snapshot stream payload')
  }
  return payload as SnapshotEvent
}

export function parseUpsertEvent(event: MessageEvent<string>): UpsertEvent {
  const payload = parseStreamEventPayload(event, 'upsert')
  if (
    !isRecord(payload) ||
    !isOrderFirehoseRow(payload.order) ||
    (payload.total !== undefined && !isNonNegativeSafeInteger(payload.total)) ||
    (payload.metrics !== undefined && !isMetrics(payload.metrics)) ||
    (payload.analyticsChanged !== undefined &&
      typeof payload.analyticsChanged !== 'boolean') ||
    (payload.sort !== undefined && payload.sort !== 'created_at_desc')
  ) {
    throw new Error('Invalid order upsert stream payload')
  }
  return payload as UpsertEvent
}

export function parseRemoveEvent(event: MessageEvent<string>): RemoveEvent {
  const payload = parseStreamEventPayload(event, 'remove')
  if (
    !isRecord(payload) ||
    typeof payload.id !== 'string' ||
    (payload.total !== undefined && !isNonNegativeSafeInteger(payload.total)) ||
    (payload.metrics !== undefined && !isMetrics(payload.metrics)) ||
    (payload.analyticsChanged !== undefined &&
      typeof payload.analyticsChanged !== 'boolean') ||
    (payload.sort !== undefined && payload.sort !== 'created_at_desc')
  ) {
    throw new Error('Invalid order remove stream payload')
  }
  return payload as RemoveEvent
}

export function parseMetricsEvent(event: MessageEvent<string>): MetricsEvent {
  const payload = parseStreamEventPayload(event, 'metrics')
  if (
    !isRecord(payload) ||
    (payload.total !== undefined && !isNonNegativeSafeInteger(payload.total)) ||
    (payload.metrics !== undefined && !isMetrics(payload.metrics)) ||
    (payload.analyticsChanged !== undefined &&
      typeof payload.analyticsChanged !== 'boolean') ||
    (payload.sort !== undefined && payload.sort !== 'created_at_desc')
  ) {
    throw new Error('Invalid order metrics stream payload')
  }
  return payload as MetricsEvent
}

function parseStreamEventPayload(
  event: MessageEvent<string>,
  eventName: string
): unknown {
  try {
    return JSON.parse(event.data) as unknown
  } catch (_error) {
    throw new Error(`Invalid ${eventName} stream JSON payload`)
  }
}

async function readJsonResponse(response: Response, label: string): Promise<unknown> {
  const text = await readLimitedResponseText(
    response,
    ADMIN_API_MAX_RESPONSE_BYTES,
    label
  )
  try {
    return JSON.parse(text) as unknown
  } catch (_error) {
    throw new Error(`Invalid ${label} JSON response payload`)
  }
}

async function readLimitedResponseText(
  response: Response,
  maxBytes: number,
  label: string
) {
  if (!response.body) return ''
  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  const chunks: string[] = []
  let bytes = 0

  for (;;) {
    const result = await reader.read()
    if (result.done) break
    bytes += result.value.byteLength
    if (bytes > maxBytes) {
      await reader.cancel().catch(() => undefined)
      throw new Error(`${label} response exceeded ${maxBytes} bytes`)
    }
    chunks.push(decoder.decode(result.value, { stream: true }))
  }
  chunks.push(decoder.decode())
  return chunks.join('')
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value)
}

function isMeResponse(value: unknown): value is MeResponse {
  return (
    isRecord(value) &&
    typeof value.authenticated === 'boolean' &&
    typeof value.authorized === 'boolean' &&
    (value.user === null || isRecord(value.user)) &&
    Array.isArray(value.allowedEmails) &&
    value.allowedEmails.every((email) => typeof email === 'string') &&
    (value.routerAdminKeyConfigured === undefined ||
      typeof value.routerAdminKeyConfigured === 'boolean') &&
    (value.analyticsConfigured === undefined ||
      typeof value.analyticsConfigured === 'boolean') &&
    (value.missingAuthConfig === undefined ||
      (Array.isArray(value.missingAuthConfig) &&
        value.missingAuthConfig.every((entry) => typeof entry === 'string'))) &&
    (value.authMode === undefined ||
      value.authMode === 'google' ||
      value.authMode === 'development_bypass')
  )
}

function isOrdersResponse(value: unknown): value is OrdersResponse {
  return (
    isRecord(value) &&
    isOrderArray(value.orders) &&
    (value.nextCursor === undefined || typeof value.nextCursor === 'string') &&
    (value.total === undefined || isNonNegativeSafeInteger(value.total)) &&
    (value.metrics === undefined || isMetrics(value.metrics)) &&
    value.sort === 'created_at_desc'
  )
}

function isOrderLookupResponse(value: unknown): value is OrderLookupResponse {
  return isRecord(value) && isOrderFirehoseRow(value.order)
}

function isVolumeAnalyticsResponse(
  value: unknown
): value is VolumeAnalyticsResponse {
  return (
    isRecord(value) &&
    isVolumeBucketSize(value.bucketSize) &&
    isVolumeOrderType(value.orderType) &&
    isIsoTimestamp(value.from) &&
    isIsoTimestamp(value.to) &&
    Array.isArray(value.buckets) &&
    value.buckets.every(isVolumeBucket)
  )
}

function isMetrics(value: unknown): boolean {
  return (
    isRecord(value) &&
    isNonNegativeSafeInteger(value.total) &&
    isNonNegativeSafeInteger(value.active) &&
    isNonNegativeSafeInteger(value.needsAttention)
  )
}

function isVolumeBucket(value: unknown): boolean {
  return (
    isRecord(value) &&
    isIsoTimestamp(value.bucketStart) &&
    isVolumeBucketSize(value.bucketSize) &&
    isVolumeOrderType(value.orderType) &&
    typeof value.volumeUsdMicro === 'string' &&
    /^\d+$/.test(value.volumeUsdMicro) &&
    isNonNegativeSafeInteger(value.orderCount)
  )
}

function isVolumeBucketSize(value: unknown): value is VolumeBucketSize {
  return value === 'five_minute' || value === 'hour' || value === 'day'
}

function isVolumeOrderType(value: unknown): value is VolumeOrderTypeFilter {
  return value === 'all' || value === 'market_order' || value === 'limit_order'
}

function isOrderArray(value: unknown): boolean {
  return Array.isArray(value) && value.every(isOrderFirehoseRow)
}

function isOrderFirehoseRow(value: unknown): boolean {
  if (!isRecord(value)) return false
  return (
    typeof value.id === 'string' &&
    typeof value.detailLevel === 'string' &&
    (value.detailLevel === 'summary' || value.detailLevel === 'full') &&
    typeof value.orderType === 'string' &&
    typeof value.status === 'string' &&
    isIsoTimestamp(value.createdAt) &&
    isIsoTimestamp(value.updatedAt) &&
    isAssetRef(value.source) &&
    isAssetRef(value.destination) &&
    typeof value.recipientAddress === 'string' &&
    typeof value.refundAddress === 'string' &&
    isIsoTimestamp(value.actionTimeoutAt) &&
    Array.isArray(value.executionLegs) &&
    Array.isArray(value.executionSteps) &&
    Array.isArray(value.providerOperations) &&
    isOrderProgress(value.progress)
  )
}

function isAssetRef(value: unknown): boolean {
  return (
    isRecord(value) &&
    typeof value.chainId === 'string' &&
    typeof value.assetId === 'string'
  )
}

function isOrderProgress(value: unknown): boolean {
  return (
    isRecord(value) &&
    isNonNegativeSafeInteger(value.totalStages) &&
    isNonNegativeSafeInteger(value.completedStages) &&
    isNonNegativeSafeInteger(value.failedStages) &&
    value.completedStages <= value.totalStages &&
    value.failedStages <= value.totalStages &&
    (value.activeStage === undefined || typeof value.activeStage === 'string') &&
    (value.stages === undefined ||
      (Array.isArray(value.stages) && value.stages.every(isOrderProgressStage)))
  )
}

function isOrderProgressStage(value: unknown): boolean {
  return (
    isRecord(value) &&
    typeof value.label === 'string' &&
    typeof value.status === 'string' &&
    (value.input === undefined || isAssetRef(value.input)) &&
    (value.output === undefined || isAssetRef(value.output)) &&
    (value.txHash === undefined || typeof value.txHash === 'string') &&
    (value.txChainId === undefined || typeof value.txChainId === 'string')
  )
}

function isIsoTimestamp(value: unknown): boolean {
  if (typeof value !== 'string') return false
  const parsed = Date.parse(value)
  return Number.isFinite(parsed) && new Date(parsed).toISOString() === value
}

function isNonNegativeSafeInteger(value: unknown): value is number {
  return (
    typeof value === 'number' &&
    Number.isSafeInteger(value) &&
    value >= 0
  )
}
