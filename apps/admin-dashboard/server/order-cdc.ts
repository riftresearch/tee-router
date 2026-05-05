import {
  LogicalReplicationService,
  PgoutputPlugin
} from 'pg-logical-replication'
import type { Pool } from 'pg'

import type { AdminDashboardConfig } from './config'
import {
  fetchOrderMetrics,
  fetchOrderSummariesByIds,
  type OrderFirehoseRow,
  type OrderMetrics
} from './orders'
import { logError } from './logging'

export type OrderCdcUpsert = {
  kind: 'upsert'
  order: OrderFirehoseRow
  total?: number
  metrics?: OrderMetrics
  analyticsChanged?: boolean
  sort: 'created_at_desc'
}

export type OrderCdcRemove = {
  kind: 'remove'
  id: string
  sourceUpdatedAt?: string
  total?: number
  metrics?: OrderMetrics
  analyticsChanged?: boolean
  sort: 'created_at_desc'
}

export type OrderCdcEvent = OrderCdcUpsert | OrderCdcRemove

export type OrderCdcStartupState = {
  snapshotBackfillRequired: boolean
}

export type OrderCdcHealth = {
  configured: boolean
  startupResolved: boolean
  streaming: boolean
  lastStartedAt?: string
  lastErrorAt?: string
}

type Listener = (event: OrderCdcEvent) => void | Promise<void>

type ListenerOptions = {
  ackRequired?: boolean
  includeMetrics?: boolean
}

type ListenerRegistration = {
  listener: Listener
  ackRequired: boolean
  includeMetrics: boolean
}

type SnapshotBackfillRequiredListener = () => void | Promise<void>

export type OrderCdcFlushChanges = {
  upserts: OrderFirehoseRow[]
  removes: OrderCdcRemove[]
}

export type OrderCdcDerivedStoreResult = {
  analyticsChanged?: boolean
  analyticsChangedOrderIds?: Iterable<string>
  analyticsChangedRemoveIds?: Iterable<string>
}

type OrderCdcBrokerHooks = {
  beforeDispatch?: (
    changes: OrderCdcFlushChanges
  ) => void | OrderCdcDerivedStoreResult | Promise<void | OrderCdcDerivedStoreResult>
  fetchMetrics?: () => Promise<OrderMetrics>
}

type RouterCdcMessage = {
  version: number
  schema: string
  table: string
  op: string
  id: string
  orderId: string
  orderUpdatedAt?: string | null
  eventUpdatedAt?: string | null
  watchId?: string | null
  providerOperationId?: string | null
}

type RouterCdcParseResult =
  | { kind: 'ignore' }
  | { kind: 'malformed'; reason: string }
  | { kind: 'change'; change: RouterCdcMessage }

export class OrderCdcBroker {
  private readonly listeners = new Set<ListenerRegistration>()
  private readonly pendingOrderIds = new Set<string>()
  private readonly pendingRemoveOrders = new Map<string, string | undefined>()
  private readonly pendingRefreshOrRemoveOrders = new Map<string, string | undefined>()
  private readonly snapshotBackfillRequiredListeners =
    new Set<SnapshotBackfillRequiredListener>()
  private readonly ready: Promise<OrderCdcStartupState>
  private resolveReady!: (state: OrderCdcStartupState) => void
  private readyResolved = false
  private service?: LogicalReplicationService
  private stopped = false
  private loopStarted = false
  private flushTimer: ReturnType<typeof setTimeout> | undefined
  private flushing = false
  private flushRetryDelayMs = 100
  private pendingAckLsn: string | undefined
  private pendingSnapshotBackfillRequired = false
  private lastAcknowledgedLsn = '0/00000000'
  private streaming = false
  private lastStartedAt: Date | undefined
  private lastErrorAt: Date | undefined
  private reconnectTimer: ReturnType<typeof setTimeout> | undefined
  private resolveReconnectSleep: (() => void) | undefined

  constructor(
    private readonly pool: Pool,
    private readonly config: AdminDashboardConfig,
    private readonly hooks: OrderCdcBrokerHooks = {}
  ) {
    this.ready = new Promise((resolve) => {
      this.resolveReady = resolve
    })
  }

  start(): Promise<OrderCdcStartupState> {
    if (!this.config.replicaDatabaseUrl) {
      return Promise.resolve({ snapshotBackfillRequired: true })
    }
    if (this.loopStarted) return this.ready
    this.loopStarted = true
    void this.run()
    return this.ready
  }

  subscribe(listener: Listener, options: ListenerOptions = {}) {
    const registration = {
      listener,
      ackRequired: options.ackRequired ?? false,
      includeMetrics: options.includeMetrics ?? true
    }
    this.listeners.add(registration)
    return () => {
      this.listeners.delete(registration)
    }
  }

  subscribeSnapshotBackfillRequired(listener: SnapshotBackfillRequiredListener) {
    this.snapshotBackfillRequiredListeners.add(listener)
    return () => {
      this.snapshotBackfillRequiredListeners.delete(listener)
    }
  }

  getHealth(): OrderCdcHealth {
    return {
      configured: Boolean(this.config.replicaDatabaseUrl),
      startupResolved: this.readyResolved,
      streaming: this.streaming,
      lastStartedAt: this.lastStartedAt?.toISOString(),
      lastErrorAt: this.lastErrorAt?.toISOString()
    }
  }

  async close() {
    this.stopped = true
    this.streaming = false
    if (this.flushTimer) clearTimeout(this.flushTimer)
    this.flushTimer = undefined
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer)
    this.reconnectTimer = undefined
    this.resolveReconnectSleep?.()
    this.resolveReconnectSleep = undefined
    await this.service?.destroy()
  }

  private async run() {
    let reconnectDelayMs = CDC_RECONNECT_INITIAL_DELAY_MS
    while (!this.stopped) {
      try {
        const slotState = await ensureCdcSlot(this.pool, this.config.cdcSlotName)
        const service = new LogicalReplicationService(
          {
            connectionString: this.config.replicaDatabaseUrl,
            application_name: 'rift-admin-dashboard-cdc'
          },
          {
            acknowledge: { auto: false, timeoutSeconds: 0 },
            flowControl: { enabled: true }
          }
        )
        this.service = service

        service.once('start', () => {
          this.handleServiceStarted(slotState)
        })
        service.on('data', (lsn: string, message: unknown) => {
          if (this.service !== service) return
          this.handleReplicationMessage(lsn, message).catch((error) => {
            logError('admin dashboard order CDC message failed', error)
          })
        })
        service.on('heartbeat', (_lsn, _timestamp, shouldRespond) => {
          if (!shouldRespond) return
          void this.acknowledge(this.lastAcknowledgedLsn, true).catch((error) => {
            logError('admin dashboard order CDC heartbeat ack failed', error)
          })
        })
        service.on('error', (error: Error) => {
          this.handleServiceError(service, error)
        })

        const plugin = new PgoutputPlugin({
          protoVersion: 1,
          publicationNames: [this.config.cdcPublicationName],
          messages: true
        })
        await service.subscribe(plugin, this.config.cdcSlotName)
        await service.destroy().catch(() => undefined)
        if (this.service === service) {
          this.streaming = false
          this.service = undefined
        }
        if (!this.stopped) {
          console.error('admin dashboard order CDC stream ended unexpectedly')
          await this.sleepForReconnectDelay(reconnectDelayMs)
          reconnectDelayMs = nextReconnectDelayMs(reconnectDelayMs)
        }
      } catch (error) {
        this.recordStreamError()
        this.resolveStartupWithSnapshotBackfill()
        await this.service?.destroy().catch(() => undefined)
        this.service = undefined
        if (!this.stopped) {
          logError('admin dashboard order CDC stream stopped', error)
          await this.sleepForReconnectDelay(reconnectDelayMs)
          reconnectDelayMs = nextReconnectDelayMs(reconnectDelayMs)
        }
      }
    }
  }

  private async sleepForReconnectDelay(ms: number) {
    if (this.stopped) return
    await new Promise<void>((resolve) => {
      this.resolveReconnectSleep = resolve
      this.reconnectTimer = setTimeout(() => {
        this.reconnectTimer = undefined
        this.resolveReconnectSleep = undefined
        resolve()
      }, ms)
    })
  }

  private async handleReplicationMessage(lsn: string, message: unknown) {
    this.pendingAckLsn = lsn
    const parsed = parseRouterCdcMessage(message, this.config.cdcMessagePrefix)
    if (parsed.kind === 'ignore') {
      this.scheduleFlush()
      return
    }
    if (parsed.kind === 'malformed') {
      console.error(
        `admin dashboard CDC received malformed router message: ${parsed.reason}`
      )
      this.pendingSnapshotBackfillRequired = true
      this.scheduleFlush()
      return
    }

    const change = parsed.change
    if (!change?.orderId) {
      this.scheduleFlush()
      return
    }

    if (change.op === 'DELETE' && change.table === 'router_orders') {
      const sourceUpdatedAt = maxIsoTimestamp(
        maxIsoTimestamp(
          this.pendingRemoveOrders.get(change.orderId),
          this.pendingRefreshOrRemoveOrders.get(change.orderId)
        ),
        change.orderUpdatedAt ?? change.eventUpdatedAt ?? undefined
      )
      this.pendingOrderIds.delete(change.orderId)
      this.pendingRefreshOrRemoveOrders.delete(change.orderId)
      this.pendingRemoveOrders.set(change.orderId, sourceUpdatedAt)
    } else if (change.op === 'DELETE') {
      const sourceUpdatedAt = change.eventUpdatedAt ?? undefined
      if (this.pendingRemoveOrders.has(change.orderId)) {
        this.pendingRemoveOrders.set(
          change.orderId,
          maxIsoTimestamp(this.pendingRemoveOrders.get(change.orderId), sourceUpdatedAt)
        )
      } else {
        this.pendingRefreshOrRemoveOrders.set(
          change.orderId,
          maxIsoTimestamp(
            this.pendingRefreshOrRemoveOrders.get(change.orderId),
            sourceUpdatedAt
          )
        )
      }
    } else if (!this.pendingRemoveOrders.has(change.orderId)) {
      this.pendingRefreshOrRemoveOrders.delete(change.orderId)
      this.pendingOrderIds.add(change.orderId)
    }
    if (this.pendingOrderBacklogSize() > MAX_CDC_PENDING_ORDER_IDS) {
      await this.restartAfterPendingBacklogOverflow()
      return
    }
    this.scheduleFlush()
  }

  private handleServiceError(service: LogicalReplicationService, error: Error) {
    logError('admin dashboard order CDC stream error', error)
    if (this.stopped || this.service !== service) return
    this.recordStreamError()
    this.service = undefined
    void service.destroy().catch((destroyError) => {
      logError(
        'admin dashboard order CDC stream destroy after error failed',
        destroyError
      )
    })
  }

  private handleServiceStarted(slotState: { created: boolean }) {
    this.streaming = true
    this.lastStartedAt = new Date()
    if (!this.readyResolved) {
      this.readyResolved = true
      this.resolveReady({
        snapshotBackfillRequired: slotState.created
      })
      return
    }

    if (slotState.created) {
      void this.dispatchSnapshotBackfillRequired()
    }
  }

  private recordStreamError() {
    this.streaming = false
    this.lastErrorAt = new Date()
  }

  private pendingOrderBacklogSize() {
    return (
      this.pendingOrderIds.size +
      this.pendingRefreshOrRemoveOrders.size +
      this.pendingRemoveOrders.size
    )
  }

  private async restartAfterPendingBacklogOverflow() {
    console.error(
      `admin dashboard order CDC pending backlog exceeded ${MAX_CDC_PENDING_ORDER_IDS}; restarting stream without acknowledgement`
    )
    if (this.flushTimer) {
      clearTimeout(this.flushTimer)
      this.flushTimer = undefined
    }
    this.pendingOrderIds.clear()
    this.pendingRefreshOrRemoveOrders.clear()
    this.pendingRemoveOrders.clear()
    this.pendingAckLsn = undefined
    this.recordStreamError()
    const service = this.service
    this.service = undefined
    await service?.destroy().catch((error) => {
      logError(
        'admin dashboard order CDC stream destroy after backlog overflow failed',
        error
      )
    })
  }

  private resolveStartupWithSnapshotBackfill() {
    if (this.readyResolved) return
    this.readyResolved = true
    this.resolveReady({ snapshotBackfillRequired: true })
  }

  private async dispatchSnapshotBackfillRequired(
    options: { requireSuccess?: boolean } = {}
  ) {
    const deliveries = [...this.snapshotBackfillRequiredListeners].map(
      async (listener) => {
        try {
          await listener()
        } catch (error) {
          logError(
            'admin dashboard CDC snapshot backfill listener failed',
            error
          )
          if (options.requireSuccess) throw error
        }
      }
    )
    await Promise.all(deliveries)
  }

  private scheduleFlush(delayMs = 25) {
    if (this.stopped || this.flushTimer || this.flushing) return
    this.flushTimer = setTimeout(() => {
      this.flushTimer = undefined
      void this.flushPendingOrders()
    }, delayMs)
  }

  private async flushPendingOrders() {
    if (this.flushing || this.stopped) return
    const removeOrders = new Map(this.pendingRemoveOrders)
    const removeOrderIds = [...removeOrders.keys()]
    const removeOrderIdSet = new Set(removeOrderIds)
    const orderIds = [...this.pendingOrderIds].filter(
      (orderId) => !removeOrderIdSet.has(orderId)
    )
    const refreshOrRemoveOrders = new Map(
      [...this.pendingRefreshOrRemoveOrders].filter(
        ([orderId]) => !removeOrderIdSet.has(orderId)
      )
    )
    const refreshOrRemoveOrderIds = [...refreshOrRemoveOrders.keys()]
    this.pendingOrderIds.clear()
    this.pendingRefreshOrRemoveOrders.clear()
    this.pendingRemoveOrders.clear()
    const ackLsn = this.pendingAckLsn
    this.pendingAckLsn = undefined
    const snapshotBackfillRequired = this.pendingSnapshotBackfillRequired
    this.pendingSnapshotBackfillRequired = false

    const hasListeners = this.listeners.size > 0
    const hasDerivedStoreConsumer = Boolean(this.hooks.beforeDispatch)

    this.flushing = true
    let nextFlushDelayMs = 25
    try {
      if (snapshotBackfillRequired) {
        await this.dispatchSnapshotBackfillRequired({ requireSuccess: true })
      }
      if (!hasListeners && !hasDerivedStoreConsumer) {
        if (ackLsn) await this.acknowledge(ackLsn)
        this.flushRetryDelayMs = 100
        return
      }
      if (
        orderIds.length === 0 &&
        refreshOrRemoveOrderIds.length === 0 &&
        removeOrderIds.length === 0
      ) {
        if (ackLsn) await this.acknowledge(ackLsn)
        this.flushRetryDelayMs = 100
        return
      }
      const fetchOrderIds = uniqueStrings([...orderIds, ...refreshOrRemoveOrderIds])
      const includeMetrics =
        hasListeners &&
        [...this.listeners].some((registration) => registration.includeMetrics)
      const orders =
        fetchOrderIds.length > 0
          ? await fetchOrderSummariesByIdsForCdcFlush(this.pool, fetchOrderIds)
          : new Map<string, OrderFirehoseRow>()
      const missingOrderIds = orderIds.filter((orderId) => !orders.has(orderId))
      if (missingOrderIds.length > 0) {
        throw new Error(
          `CDC referenced order ids not visible in replica query: ${missingOrderIds.join(', ')}`
        )
      }
      const dispatchRemoveOrders = new Map(removeOrders)
      for (const orderId of refreshOrRemoveOrderIds) {
        if (!orders.has(orderId) && !dispatchRemoveOrders.has(orderId)) {
          dispatchRemoveOrders.set(orderId, undefined)
        }
      }
      const upsertEvents: OrderCdcUpsert[] = []
      for (const orderId of fetchOrderIds) {
        let order = orders.get(orderId)
        if (!order) continue
        const sourceUpdatedAt = refreshOrRemoveOrders.get(orderId)
        if (sourceUpdatedAt) {
          order = withUpdatedAtFloor(order, sourceUpdatedAt)
        }
        upsertEvents.push({
          kind: 'upsert',
          order,
          sort: 'created_at_desc'
        })
      }
      const removeEvents: OrderCdcRemove[] = [...dispatchRemoveOrders].map(
        ([orderId, sourceUpdatedAt]) => ({
          kind: 'remove',
          id: orderId,
          sourceUpdatedAt,
          sort: 'created_at_desc'
        })
      )
      const derivedStoreResult = await this.dispatchDerivedStoreChanges({
        upserts: upsertEvents.map((event) => event.order),
        removes: removeEvents
      })
      applyDerivedStoreResult(upsertEvents, removeEvents, derivedStoreResult)
      const metrics = includeMetrics ? await this.fetchMetricsForFlush() : undefined
      for (const event of upsertEvents) {
        event.total = metrics?.total
        event.metrics = metrics
        await this.dispatchOrderEvent(event)
      }
      for (const event of removeEvents) {
        event.total = metrics?.total
        event.metrics = metrics
        await this.dispatchOrderEvent(event)
      }
      if (ackLsn) await this.acknowledge(ackLsn)
      this.flushRetryDelayMs = 100
    } catch (error) {
      for (const orderId of orderIds) {
        if (!this.pendingRemoveOrders.has(orderId)) {
          this.pendingOrderIds.add(orderId)
        }
      }
      for (const orderId of refreshOrRemoveOrderIds) {
        if (!this.pendingRemoveOrders.has(orderId)) {
          this.pendingRefreshOrRemoveOrders.set(orderId, refreshOrRemoveOrders.get(orderId))
        }
      }
      for (const [orderId, sourceUpdatedAt] of removeOrders) {
        this.pendingRemoveOrders.set(orderId, sourceUpdatedAt)
      }
      this.pendingSnapshotBackfillRequired =
        this.pendingSnapshotBackfillRequired || snapshotBackfillRequired
      this.pendingAckLsn = this.pendingAckLsn ?? ackLsn
      nextFlushDelayMs = this.flushRetryDelayMs
      this.flushRetryDelayMs = Math.min(this.flushRetryDelayMs * 2, 1_000)
      logError('admin dashboard order CDC flush failed', error)
    } finally {
      this.flushing = false
      if (
        this.pendingAckLsn ||
        this.pendingSnapshotBackfillRequired ||
        this.pendingOrderIds.size > 0 ||
        this.pendingRefreshOrRemoveOrders.size > 0 ||
        this.pendingRemoveOrders.size > 0
      ) {
        this.scheduleFlush(nextFlushDelayMs)
      }
    }
  }

  private async acknowledge(lsn: string, ping = false) {
    const acknowledged = await this.service?.acknowledge(lsn, ping)
    if (acknowledged) this.lastAcknowledgedLsn = lsn
  }

  private async fetchMetricsForFlush() {
    if (!this.hooks.fetchMetrics) return fetchOrderMetrics(this.pool)
    try {
      return await this.hooks.fetchMetrics()
    } catch (error) {
      logError(
        'admin dashboard CDC metrics hook failed; falling back to replica metrics',
        error
      )
      void this.dispatchSnapshotBackfillRequired()
      return fetchOrderMetrics(this.pool)
    }
  }

  private async dispatchDerivedStoreChanges(changes: OrderCdcFlushChanges) {
    if (!this.hooks.beforeDispatch) return undefined
    try {
      return await this.hooks.beforeDispatch(changes)
    } catch (error) {
      logError(
        'admin dashboard CDC derived-store hook failed; requesting snapshot backfill',
        error
      )
      await this.dispatchSnapshotBackfillRequired()
      throw error
    }
  }

  private async dispatchOrderEvent(event: OrderCdcEvent) {
    const ackRequiredDeliveries: Promise<void>[] = []

    for (const registration of [...this.listeners]) {
      try {
        const delivery = Promise.resolve(registration.listener(event))
        if (registration.ackRequired) {
          ackRequiredDeliveries.push(delivery)
        } else {
          void delivery.catch((error) => {
            logError('admin dashboard order CDC best-effort listener failed', error)
          })
        }
      } catch (error) {
        if (registration.ackRequired) {
          ackRequiredDeliveries.push(Promise.reject(error))
        } else {
          logError('admin dashboard order CDC best-effort listener failed', error)
        }
      }
    }

    await Promise.all(ackRequiredDeliveries)
  }
}

function applyDerivedStoreResult(
  upserts: OrderCdcUpsert[],
  removes: OrderCdcRemove[],
  result: void | OrderCdcDerivedStoreResult
) {
  if (!result) return
  const allChanged = result.analyticsChanged === true
  const changedOrderIds = new Set(result.analyticsChangedOrderIds ?? [])
  const changedRemoveIds = new Set(result.analyticsChangedRemoveIds ?? [])

  for (const event of upserts) {
    if (allChanged || changedOrderIds.has(event.order.id)) {
      event.analyticsChanged = true
    }
  }
  for (const event of removes) {
    if (allChanged || changedRemoveIds.has(event.id)) {
      event.analyticsChanged = true
    }
  }
}

function uniqueStrings(values: string[]) {
  return [...new Set(values)]
}

async function fetchOrderSummariesByIdsForCdcFlush(pool: Pool, ids: string[]) {
  const orders = new Map<string, OrderFirehoseRow>()
  for (let index = 0; index < ids.length; index += MAX_CDC_ORDER_FETCH_BATCH_SIZE) {
    const batch = ids.slice(index, index + MAX_CDC_ORDER_FETCH_BATCH_SIZE)
    const batchOrders = await fetchOrderSummariesByIds(pool, batch)
    for (const [orderId, order] of batchOrders) {
      orders.set(orderId, order)
    }
  }
  return orders
}

function withUpdatedAtFloor(order: OrderFirehoseRow, sourceUpdatedAt: string) {
  const orderUpdatedAtMs = Date.parse(order.updatedAt)
  const sourceUpdatedAtMs = Date.parse(sourceUpdatedAt)
  if (Number.isNaN(sourceUpdatedAtMs)) return order
  if (!Number.isNaN(orderUpdatedAtMs) && orderUpdatedAtMs >= sourceUpdatedAtMs) {
    return order
  }
  return { ...order, updatedAt: sourceUpdatedAt }
}

function maxIsoTimestamp(left: string | undefined, right: string | undefined) {
  if (!left) return right
  if (!right) return left
  const leftMs = Date.parse(left)
  const rightMs = Date.parse(right)
  if (Number.isNaN(leftMs)) return right
  if (Number.isNaN(rightMs)) return left
  return rightMs > leftMs ? right : left
}

export async function ensureCdcSlot(pool: Pool, slotName: string) {
  const existing = await pool.query<{
    plugin: string | null
    database: string | null
    current_database: string
  }>(
    `
    SELECT plugin, database, current_database() AS current_database
    FROM pg_catalog.pg_replication_slots
    WHERE slot_name = $1
    `,
    [slotName]
  )

  const slot = existing.rows[0]
  const plugin = slot?.plugin
  if (slot) {
    if (plugin !== 'pgoutput') {
      throw new Error(
        `CDC slot ${slotName} uses plugin ${plugin}; expected pgoutput`
      )
    }
    if (slot.database !== slot.current_database) {
      throw new Error(
        `CDC slot ${slotName} belongs to database ${slot.database}; expected ${slot.current_database}`
      )
    }
    const invalidReason = cdcSlotInvalidReason(
      await fetchCdcSlotHealth(pool, slotName)
    )
    if (invalidReason) {
      await recreateCdcSlot(pool, slotName, invalidReason)
      return { created: true }
    }
    return { created: false }
  }

  await createCdcSlot(pool, slotName)
  return { created: true }
}

async function createCdcSlot(pool: Pool, slotName: string) {
  await pool.query(
    `
    SELECT slot_name
    FROM pg_catalog.pg_create_logical_replication_slot($1::name, 'pgoutput')
    `,
    [slotName]
  )
}

async function recreateCdcSlot(pool: Pool, slotName: string, reason: string) {
  const health = await fetchCdcSlotHealth(pool, slotName)
  if (health.active) {
    throw new Error(
      `CDC slot ${slotName} is invalid (${reason}) but active; stop the active consumer before recreating it`
    )
  }
  console.error(`admin dashboard CDC slot ${slotName} is invalid (${reason}); recreating`)
  await pool.query(
    `
    SELECT pg_catalog.pg_drop_replication_slot($1::name)
    `,
    [slotName]
  )
  await createCdcSlot(pool, slotName)
}

async function fetchCdcSlotHealth(pool: Pool, slotName: string) {
  const columnResult = await pool.query<{ attname: string }>(
    `
    SELECT attname
    FROM pg_catalog.pg_attribute
    WHERE attrelid = 'pg_catalog.pg_replication_slots'::regclass
      AND attnum > 0
      AND NOT attisdropped
      AND attname = ANY($1::name[])
    `,
    [['active', 'wal_status', 'invalidation_reason']]
  )
  const columns = new Set(columnResult.rows.map((row) => row.attname))
  const activeExpr = columns.has('active')
    ? 'active'
    : 'NULL::boolean AS active'
  const walStatusExpr = columns.has('wal_status')
    ? 'wal_status'
    : 'NULL::text AS wal_status'
  const invalidationReasonExpr = columns.has('invalidation_reason')
    ? 'invalidation_reason'
    : 'NULL::text AS invalidation_reason'
  const result = await pool.query<{
    active: boolean | null
    wal_status: string | null
    invalidation_reason: string | null
  }>(
    `
    SELECT ${activeExpr}, ${walStatusExpr}, ${invalidationReasonExpr}
    FROM pg_catalog.pg_replication_slots
    WHERE slot_name = $1
    `,
    [slotName]
  )
  const row = result.rows[0]
  return {
    active: row?.active ?? null,
    walStatus: row?.wal_status ?? null,
    invalidationReason: row?.invalidation_reason ?? null
  }
}

function cdcSlotInvalidReason(slot: {
  walStatus: string | null
  invalidationReason: string | null
}) {
  if (slot.invalidationReason) {
    return `invalidation_reason=${slot.invalidationReason}`
  }
  if (slot.walStatus === 'lost') return 'wal_status=lost'
  return undefined
}

function parseRouterCdcMessage(
  message: unknown,
  expectedPrefix: string
): RouterCdcParseResult {
  if (!isRecord(message) || message.tag !== 'message') return { kind: 'ignore' }
  if (message.prefix !== expectedPrefix) return { kind: 'ignore' }
  if (!(message.content instanceof Uint8Array)) {
    return malformedRouterCdcMessage('message content is not bytes')
  }
  if (message.content.byteLength > MAX_ROUTER_CDC_MESSAGE_BYTES) {
    return malformedRouterCdcMessage('message content exceeds maximum size')
  }

  const payload = new TextDecoder().decode(message.content)
  const parsed = parseJson(payload)
  if (!isRecord(parsed)) return malformedRouterCdcMessage('message content is not JSON object')
  const hasOrderUpdatedAt = Object.hasOwn(parsed, 'orderUpdatedAt')
  const hasEventUpdatedAt = Object.hasOwn(parsed, 'eventUpdatedAt')
  const version = parsed.version
  const schema = optionalBoundedString(parsed.schema)
  const table = optionalBoundedString(parsed.table)
  const op = optionalBoundedString(parsed.op)
  const id = optionalUuidString(parsed.id)
  const orderId = optionalUuidString(parsed.orderId)
  const orderUpdatedAt = optionalTimestampString(parsed.orderUpdatedAt)
  const eventUpdatedAt = optionalTimestampString(parsed.eventUpdatedAt)
  const watchId = optionalUuidString(parsed.watchId)
  const providerOperationId = optionalUuidString(parsed.providerOperationId)
  const orderUpdatedAtInvalid = hasOrderUpdatedAt && orderUpdatedAt === undefined
  const eventUpdatedAtInvalid = hasEventUpdatedAt && eventUpdatedAt === undefined
  if (
    version === 1 &&
    schema === 'public' &&
    table &&
    SUPPORTED_ROUTER_CDC_TABLES.has(table) &&
    op &&
    SUPPORTED_ROUTER_CDC_OPS.has(op) &&
    id !== undefined &&
    id !== null &&
    orderId === null &&
    !eventUpdatedAtInvalid &&
    NULL_ORDER_ID_IGNORED_TABLES.has(table)
  ) {
    return { kind: 'ignore' }
  }
  if (
    version !== 1 ||
    schema !== 'public' ||
    !table ||
    !SUPPORTED_ROUTER_CDC_TABLES.has(table) ||
    !op ||
    !SUPPORTED_ROUTER_CDC_OPS.has(op) ||
    id === undefined ||
    id === null ||
    orderId === undefined ||
    orderId === null ||
    orderUpdatedAtInvalid ||
    (table === 'router_orders' && hasOrderUpdatedAt && orderUpdatedAt === null) ||
    eventUpdatedAtInvalid ||
    (hasEventUpdatedAt && eventUpdatedAt === null) ||
    watchId === undefined ||
    providerOperationId === undefined
  ) {
    return malformedRouterCdcMessage('message fields failed validation')
  }

  return {
    kind: 'change',
    change: {
      version,
      schema,
      table,
      op,
      id,
      orderId,
      orderUpdatedAt,
      eventUpdatedAt,
      watchId,
      providerOperationId
    }
  }
}

function malformedRouterCdcMessage(reason: string): RouterCdcParseResult {
  return { kind: 'malformed', reason }
}

function parseJson(value: string): unknown {
  try {
    return JSON.parse(value) as unknown
  } catch (_error) {
    return undefined
  }
}

function optionalBoundedString(value: unknown): string | null | undefined {
  if (value === undefined || value === null || value === '') return null
  if (typeof value !== 'string') return undefined
  return value.length <= MAX_ROUTER_CDC_FIELD_BYTES ? value : undefined
}

function optionalUuidString(value: unknown): string | null | undefined {
  const string = optionalBoundedString(value)
  if (!string) return string
  return UUID_PATTERN.test(string) ? string : undefined
}

function optionalTimestampString(value: unknown): string | null | undefined {
  const string = optionalBoundedString(value)
  if (!string) return string
  if (!CDC_TIMESTAMP_PATTERN.test(string)) return undefined
  return Number.isNaN(Date.parse(string)) ? undefined : string
}

function nextReconnectDelayMs(currentDelayMs: number): number {
  return Math.min(currentDelayMs * 2, CDC_RECONNECT_MAX_DELAY_MS)
}

const SUPPORTED_ROUTER_CDC_OPS = new Set(['INSERT', 'UPDATE', 'DELETE'])
const NULL_ORDER_ID_IGNORED_TABLES = new Set([
  'market_order_quotes',
  'limit_order_quotes',
  'custody_vaults',
  'deposit_vaults'
])
const MAX_ROUTER_CDC_MESSAGE_BYTES = 16 * 1024
const MAX_ROUTER_CDC_FIELD_BYTES = 128
export const MAX_CDC_PENDING_ORDER_IDS = 10_000
export const MAX_CDC_ORDER_FETCH_BATCH_SIZE = 500
const CDC_RECONNECT_INITIAL_DELAY_MS = 1_000
const CDC_RECONNECT_MAX_DELAY_MS = 30_000
const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i
const CDC_TIMESTAMP_PATTERN =
  /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})$/
const SUPPORTED_ROUTER_CDC_TABLES = new Set([
  'router_orders',
  'market_order_quotes',
  'market_order_actions',
  'limit_order_quotes',
  'limit_order_actions',
  'order_execution_legs',
  'order_execution_steps',
  'order_provider_operations',
  'order_provider_addresses',
  'deposit_vaults',
  'custody_vaults'
])

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value)
}
