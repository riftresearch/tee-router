import { Hono, type Context } from 'hono'
import { cors } from 'hono/cors'
import { secureHeaders } from 'hono/secure-headers'
import { serveStatic } from 'hono/bun'

import { createDashboardAuth, type DashboardAuthRuntime } from './auth'
import {
  createVolumeAnalyticsRuntime,
  type VolumeBucketSize,
  type VolumeOrderTypeFilter
} from './analytics'
import {
  ALLOWED_ADMIN_EMAILS,
  type AdminDashboardConfig,
  isAllowedAdminEmail,
  loadConfig,
  normalizeAdminEmail,
  validateRuntimeConfig
} from './config'
import { createReplicaDatabase, type ReplicaDatabaseRuntime } from './db'
import { logError } from './logging'
import {
  OrderCdcBroker,
  type OrderCdcEvent,
  type OrderCdcRemove
} from './order-cdc'
import {
  fetchOrderFirehose,
  fetchOrderById,
  fetchOrderMetrics as fetchReplicaOrderMetrics,
  orderMatchesLifecycleFilter,
  type OrderFirehoseRow,
  type OrderLifecycleFilter,
  type OrderMetrics,
  type OrderPageCursor,
  type OrderTypeFilter
} from './orders'

type AppBindings = {
  Variables: {
    adminEmail: string
  }
}

const LOCAL_DEV_ADMIN_EMAIL = ALLOWED_ADMIN_EMAILS[0]
const MAX_ORDER_LIMIT = 500
const MAX_ORDER_CURSOR_LENGTH = 512
const MAX_ANALYTICS_RANGE_BUCKETS = 20_000
const MAX_ANALYTICS_TIMESTAMP_LENGTH = 64
export const MAX_SSE_PENDING_EVENTS = 1_000
export const MAX_SSE_BACKPRESSURE_CHUNKS = 256
const ORDER_CURSOR_BASE64URL_PATTERN = /^[A-Za-z0-9_-]+$/
const ORDER_CURSOR_CREATED_AT_MAX_LENGTH = 64
const ANALYTICS_BUCKET_SIZE_MS: Record<VolumeBucketSize, number> = {
  minute: 60_000,
  hour: 60 * 60_000,
  day: 24 * 60 * 60_000
}
const SSE_HEARTBEAT_MS = 15_000

type OrderStreamMetricsEvent = {
  kind: 'metrics'
  total?: number
  metrics?: OrderMetrics
  analyticsChanged?: boolean
  sort: 'created_at_desc'
}

type OrderStreamQueuedEvent = OrderCdcEvent | OrderStreamMetricsEvent

export type AdminDashboardRuntime = {
  app: Hono<AppBindings>
  close: () => Promise<void>
}

export function createApp(
  config: AdminDashboardConfig = loadConfig()
): AdminDashboardRuntime {
  validateRuntimeConfig(config)
  const app = new Hono<AppBindings>()
  const authRuntime = config.production ? createDashboardAuth(config) : null
  const replicaRuntime = createReplicaDatabase(config)
  const volumeAnalyticsRuntime = createVolumeAnalyticsRuntime(config)
  const orderCdcBroker = replicaRuntime
    ? new OrderCdcBroker(
        replicaRuntime.pool,
        config,
        volumeAnalyticsRuntime
          ? {
              beforeDispatch: async ({ upserts, removes }) => {
                const analyticsChangedOrderIds = new Set<string>()
                const analyticsChangedRemoveIds = new Set<string>()
                if (upserts.length > 0) {
                  const changedOrderIds = await volumeAnalyticsRuntime.ingestOrders(upserts)
                  for (const orderId of changedOrderIds) {
                    analyticsChangedOrderIds.add(orderId)
                  }
                }
                for (const event of removes) {
                  const volumeChanged = await volumeAnalyticsRuntime.removeOrder(
                    event.id,
                    event.sourceUpdatedAt
                  )
                  if (volumeChanged) analyticsChangedRemoveIds.add(event.id)
                }
                return {
                  analyticsChangedOrderIds,
                  analyticsChangedRemoveIds
                }
              },
              fetchMetrics: () => volumeAnalyticsRuntime.fetchOrderMetrics()
            }
          : undefined
      )
    : null
  const unsubscribeVolumeAnalyticsBackfill =
    volumeAnalyticsRuntime && orderCdcBroker && replicaRuntime
      ? orderCdcBroker.subscribeSnapshotBackfillRequired(() => {
          return volumeAnalyticsRuntime.startBackfill(replicaRuntime.pool, {
            fullSnapshot: true
          })
        })
      : undefined
  const cdcReady =
    orderCdcBroker?.start() ?? Promise.resolve({ snapshotBackfillRequired: true })
  let closing = false

  if (replicaRuntime && volumeAnalyticsRuntime) {
    void cdcReady
      .then((startupState) => {
        if (!closing) {
          void volumeAnalyticsRuntime
            .startBackfill(replicaRuntime.pool, {
              fullSnapshot: startupState.snapshotBackfillRequired
            })
            .catch((error) => {
              logError('admin dashboard analytics startup backfill failed', error)
            })
        }
      })
      .catch((error) => {
        logError('admin dashboard CDC startup failed', error)
      })
  }

  app.use(
    '*',
    secureHeaders({
      contentSecurityPolicy: {
        defaultSrc: ["'self'"],
        scriptSrc: ["'self'"],
        styleSrc: ["'self'"],
        imgSrc: ["'self'", 'https:', 'data:'],
        connectSrc: ["'self'", config.webOrigin, config.authBaseUrl],
        frameAncestors: ["'none'"],
        objectSrc: ["'none'"],
        baseUri: ["'self'"]
      },
      crossOriginEmbedderPolicy: false
    })
  )

  app.use(
    '/api/*',
    cors({
      origin: (origin) =>
        !origin || config.trustedOrigins.includes(origin)
          ? origin || config.webOrigin
          : config.webOrigin,
      credentials: true,
      allowHeaders: ['Content-Type', 'Authorization'],
      allowMethods: ['GET', 'POST', 'OPTIONS'],
      exposeHeaders: ['Content-Length'],
      maxAge: 600
    })
  )

  app.on(['GET', 'POST'], '/api/auth/*', (c) => {
    if (!config.production) {
      return c.json({ error: 'auth_disabled_in_development' }, 404)
    }

    if (!authRuntime) {
      return c.json(
        {
          error: 'auth_not_configured',
          missing: config.missingAuthConfig
        },
        503
      )
    }

    return authRuntime.auth.handler(c.req.raw)
  })

  app.get('/api/status', (c) =>
    c.json({
      status: 'ok',
      version: config.version,
      authConfigured: config.production ? Boolean(authRuntime) : true,
      authMode: config.production ? 'google' : 'development_bypass',
      replicaConfigured: Boolean(replicaRuntime),
      analyticsConfigured: Boolean(volumeAnalyticsRuntime),
      cdc: orderCdcBroker?.getHealth() ?? {
        configured: false,
        startupResolved: true,
        streaming: false
      }
    })
  )

  app.get('/api/me', async (c) => {
    if (!config.production) {
      return c.json({
        authenticated: true,
        authorized: true,
        user: {
          id: 'local-dev-admin',
          name: 'Local Dev Admin',
          email: LOCAL_DEV_ADMIN_EMAIL,
          emailVerified: true,
          image: null
        },
        allowedEmails: ALLOWED_ADMIN_EMAILS,
        routerAdminKeyConfigured: Boolean(config.routerAdminApiKey),
        analyticsConfigured: Boolean(volumeAnalyticsRuntime),
        authMode: 'development_bypass'
      })
    }

    if (!authRuntime) {
      return c.json(
        {
          authenticated: false,
          authorized: false,
          user: null,
          missingAuthConfig: config.missingAuthConfig,
          allowedEmails: ALLOWED_ADMIN_EMAILS,
          analyticsConfigured: Boolean(volumeAnalyticsRuntime)
        },
        503
      )
    }

    const session = await authRuntime.auth.api.getSession({
      headers: c.req.raw.headers
    })
    const email = normalizeAdminEmail(session?.user.email)
    const emailVerified = session?.user.emailVerified === true
    const authorized = Boolean(
      session && email && emailVerified && isAllowedAdminEmail(email)
    )

    return c.json({
      authenticated: Boolean(session),
      authorized,
      user: session
        ? {
            id: session.user.id,
            name: session.user.name,
            email,
            emailVerified,
            image: session.user.image
          }
        : null,
      allowedEmails: ALLOWED_ADMIN_EMAILS,
      routerAdminKeyConfigured: authorized
        ? Boolean(config.routerAdminApiKey)
        : undefined,
      analyticsConfigured: Boolean(volumeAnalyticsRuntime),
      authMode: 'google'
    })
  })

  app.get('/api/orders', async (c) => {
    const admin = await requireAdmin(c, config, authRuntime)
    if (admin instanceof Response) return admin

    if (!replicaRuntime) {
      return c.json({ error: 'replica_database_not_configured' }, 503)
    }

    const limit = parseLimit(c.req.query('limit'), config.orderLimit)
    if (limit instanceof Response) return limit
    const orderType = parseOrderType(c.req.query('orderType'))
    if (orderType instanceof Response) return orderType
    const lifecycleFilter = parseOrderLifecycleFilter(c.req.query('filter'))
    if (lifecycleFilter instanceof Response) return lifecycleFilter
    const cursor = parseOrderCursor(c.req.query('cursor'))
    if (cursor instanceof Response) return cursor
    const includeMetrics = parseBooleanQuery(c.req.query('metrics'), true)
    if (includeMetrics instanceof Response) return includeMetrics

    const page = await fetchOrderPage(
      replicaRuntime,
      limit,
      cursor,
      orderType,
      lifecycleFilter,
      includeMetrics,
      volumeAnalyticsRuntime
        ? () => volumeAnalyticsRuntime.fetchOrderMetrics()
        : undefined
    )
    return c.json({
      orders: page.orders,
      nextCursor: page.nextCursor,
      ...(page.metrics
        ? {
            total: page.metrics.total,
            metrics: page.metrics
          }
        : {}),
      sort: 'created_at_desc'
    })
  })

  app.get('/api/orders/events', async (c) => {
    const admin = await requireAdmin(c, config, authRuntime)
    if (admin instanceof Response) return admin

    if (!replicaRuntime) {
      return c.json({ error: 'replica_database_not_configured' }, 503)
    }

    const limit = parseLimit(c.req.query('limit'), config.orderLimit)
    if (limit instanceof Response) return limit
    const orderType = parseOrderType(c.req.query('orderType'))
    if (orderType instanceof Response) return orderType
    const lifecycleFilter = parseOrderLifecycleFilter(c.req.query('filter'))
    if (lifecycleFilter instanceof Response) return lifecycleFilter
    const snapshot = parseBooleanQuery(c.req.query('snapshot'), true)
    if (snapshot instanceof Response) return snapshot
    return createOrderEventStream(
      c.req.raw.signal,
      replicaRuntime,
      limit,
      orderType,
      lifecycleFilter,
      snapshot,
      orderCdcBroker,
      volumeAnalyticsRuntime
        ? () => volumeAnalyticsRuntime.fetchOrderMetrics()
        : undefined
    )
  })

  app.get('/api/orders/:id', async (c) => {
    const admin = await requireAdmin(c, config, authRuntime)
    if (admin instanceof Response) return admin

    if (!replicaRuntime) {
      return c.json({ error: 'replica_database_not_configured' }, 503)
    }

    const id = c.req.param('id')
    if (!isUuid(id)) {
      return c.json({ error: 'invalid_order_id' }, 400)
    }

    const order = await fetchOrderById(replicaRuntime.pool, id)
    if (!order) return c.json({ error: 'order_not_found' }, 404)
    return c.json({ order })
  })

  app.get('/api/analytics/volume', async (c) => {
    const admin = await requireAdmin(c, config, authRuntime)
    if (admin instanceof Response) return admin

    if (!volumeAnalyticsRuntime) {
      return c.json({ error: 'analytics_database_not_configured' }, 503)
    }

    const bucketSize = parseVolumeBucketSize(c.req.query('bucketSize'))
    if (bucketSize instanceof Response) return bucketSize
    const orderType = parseVolumeOrderType(c.req.query('orderType'))
    if (orderType instanceof Response) return orderType
    const range = parseAnalyticsRange(
      c.req.query('from'),
      c.req.query('to'),
      bucketSize
    )
    if (range instanceof Response) return range

    const buckets = await volumeAnalyticsRuntime.fetchVolumeBuckets({
      bucketSize,
      orderType,
      from: range.from,
      to: range.to
    })
    return c.json({
      bucketSize,
      orderType,
      from: range.from.toISOString(),
      to: range.to.toISOString(),
      buckets
    })
  })

  if (config.serveStatic) {
    app.use('/assets/*', serveStatic({ root: './dist' }))
    app.get('*', serveStatic({ path: './dist/index.html' }))
  }

  return {
    app,
    close: async () => {
      closing = true
      unsubscribeVolumeAnalyticsBackfill?.()
      await Promise.all([
        authRuntime?.close() ?? Promise.resolve(),
        orderCdcBroker?.close() ?? Promise.resolve(),
        volumeAnalyticsRuntime?.close() ?? Promise.resolve(),
        replicaRuntime?.close() ?? Promise.resolve()
      ])
    }
  }
}

async function requireAdmin(
  c: Context<AppBindings>,
  config: AdminDashboardConfig,
  authRuntime: DashboardAuthRuntime | null
): Promise<{ email: string } | Response> {
  if (!config.production) {
    c.set('adminEmail', LOCAL_DEV_ADMIN_EMAIL)
    return { email: LOCAL_DEV_ADMIN_EMAIL }
  }

  if (!authRuntime) {
    return c.json({ error: 'auth_not_configured' }, 503)
  }

  const session = await authRuntime.auth.api.getSession({
    headers: c.req.raw.headers
  })
  const email = normalizeAdminEmail(session?.user.email)

  if (!session) {
    return c.json({ error: 'unauthenticated' }, 401)
  }

  if (!email || session.user.emailVerified !== true || !isAllowedAdminEmail(email)) {
    return c.json({ error: 'forbidden' }, 403)
  }

  c.set('adminEmail', email)
  return { email }
}

function parseLimit(value: string | undefined, defaultLimit: number): number | Response {
  if (!value) return Math.min(defaultLimit, MAX_ORDER_LIMIT)
  if (!/^[1-9][0-9]{0,8}$/.test(value)) {
    return Response.json({ error: 'invalid_limit' }, { status: 400 })
  }
  const parsed = Number(value)
  if (!Number.isSafeInteger(parsed)) {
    return Response.json({ error: 'invalid_limit' }, { status: 400 })
  }
  return Math.min(parsed, MAX_ORDER_LIMIT)
}

export async function fetchOrderPage(
  replicaRuntime: ReplicaDatabaseRuntime,
  limit: number,
  cursor: OrderPageCursor | undefined,
  orderType: OrderTypeFilter | undefined,
  lifecycleFilter: OrderLifecycleFilter | undefined,
  includeMetrics = true,
  fetchMetrics?: () => Promise<OrderMetrics>
) {
  const ordersPromise = fetchOrderFirehose(
    replicaRuntime.pool,
    limit + 1,
    cursor,
    orderType,
    lifecycleFilter
  )
  const [ordersWithLookahead, metrics] = await Promise.all([
    ordersPromise,
    includeMetrics
      ? fetchOrderPageMetrics(replicaRuntime, fetchMetrics)
      : Promise.resolve(undefined)
  ])
  const orders = ordersWithLookahead.slice(0, limit)
  const nextCursor =
    ordersWithLookahead.length > limit
      ? encodeOrderCursor(orders[orders.length - 1])
      : undefined

  return {
    orders,
    nextCursor,
    metrics
  }
}

async function fetchOrderPageMetrics(
  replicaRuntime: ReplicaDatabaseRuntime,
  fetchMetrics?: () => Promise<OrderMetrics>
) {
  if (!fetchMetrics) return fetchReplicaOrderMetrics(replicaRuntime.pool)
  try {
    return await fetchMetrics()
  } catch (error) {
    logError(
      'admin dashboard analytics metrics failed; falling back to replica metrics',
      error
    )
    return fetchReplicaOrderMetrics(replicaRuntime.pool)
  }
}

function parseOrderCursor(value: string | undefined): OrderPageCursor | undefined | Response {
  if (!value) return undefined
  if (
    value.length > MAX_ORDER_CURSOR_LENGTH ||
    !ORDER_CURSOR_BASE64URL_PATTERN.test(value)
  ) {
    return Response.json({ error: 'invalid_cursor' }, { status: 400 })
  }

  try {
    const parsed = JSON.parse(fromBase64Url(value)) as unknown
    if (!isOrderCursor(parsed)) throw new Error('invalid cursor shape')
    return parsed
  } catch (_error) {
    return Response.json({ error: 'invalid_cursor' }, { status: 400 })
  }
}

function parseOrderType(value: string | undefined): OrderTypeFilter | undefined | Response {
  if (!value) return undefined
  if (value === 'market_order' || value === 'limit_order') return value
  return Response.json({ error: 'invalid_order_type' }, { status: 400 })
}

function parseOrderLifecycleFilter(
  value: string | undefined
): OrderLifecycleFilter | undefined | Response {
  if (!value || value === 'firehose') return undefined
  if (
    value === 'in_progress' ||
    value === 'failed' ||
    value === 'refunded' ||
    value === 'manual_refund'
  ) {
    return value
  }
  return Response.json({ error: 'invalid_order_filter' }, { status: 400 })
}

function parseBooleanQuery(
  value: string | undefined,
  defaultValue: boolean
): boolean | Response {
  if (value === undefined) return defaultValue
  if (value === 'true' || value === '1') return true
  if (value === 'false' || value === '0') return false
  return Response.json({ error: 'invalid_boolean_query' }, { status: 400 })
}

function parseVolumeBucketSize(
  value: string | undefined
): VolumeBucketSize | Response {
  if (value === 'minute' || value === 'hour' || value === 'day') return value
  return Response.json({ error: 'invalid_volume_bucket_size' }, { status: 400 })
}

function parseVolumeOrderType(
  value: string | undefined
): VolumeOrderTypeFilter | Response {
  if (!value || value === 'all') return 'all'
  const orderType = parseOrderType(value)
  if (orderType instanceof Response) return orderType
  return orderType ?? Response.json({ error: 'invalid_order_type' }, { status: 400 })
}

export function parseAnalyticsRange(
  fromValue: string | undefined,
  toValue: string | undefined,
  bucketSize: VolumeBucketSize
): { from: Date; to: Date } | Response {
  if (!fromValue || !toValue) {
    return Response.json({ error: 'missing_analytics_range' }, { status: 400 })
  }
  if (
    fromValue.length > MAX_ANALYTICS_TIMESTAMP_LENGTH ||
    toValue.length > MAX_ANALYTICS_TIMESTAMP_LENGTH
  ) {
    return Response.json({ error: 'invalid_analytics_range' }, { status: 400 })
  }
  const from = new Date(fromValue)
  const to = new Date(toValue)
  if (
    !isCanonicalIsoTimestamp(fromValue, from) ||
    !isCanonicalIsoTimestamp(toValue, to)
  ) {
    return Response.json({ error: 'invalid_analytics_range' }, { status: 400 })
  }
  if (from >= to) {
    return Response.json({ error: 'invalid_analytics_range_order' }, { status: 400 })
  }
  if (analyticsRangeBucketCount(from, to, bucketSize) > MAX_ANALYTICS_RANGE_BUCKETS) {
    return Response.json({ error: 'analytics_range_too_large' }, { status: 400 })
  }
  return { from, to }
}

function analyticsRangeBucketCount(
  from: Date,
  to: Date,
  bucketSize: VolumeBucketSize
) {
  return Math.ceil(
    (to.getTime() - from.getTime()) / ANALYTICS_BUCKET_SIZE_MS[bucketSize]
  )
}

function isCanonicalIsoTimestamp(value: string, parsed: Date): boolean {
  return Number.isFinite(parsed.getTime()) && parsed.toISOString() === value
}

function encodeOrderCursor(order: OrderFirehoseRow | undefined): string | undefined {
  if (!order) return undefined
  return toBase64Url(
    JSON.stringify({
      createdAt: order.createdAt,
      id: order.id
    } satisfies OrderPageCursor)
  )
}

function isOrderCursor(value: unknown): value is OrderPageCursor {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false
  const cursor = value as Record<string, unknown>
  const keys = Object.keys(cursor)
  if (keys.length !== 2 || !keys.includes('createdAt') || !keys.includes('id')) {
    return false
  }
  if (
    typeof cursor.createdAt !== 'string' ||
    cursor.createdAt.length > ORDER_CURSOR_CREATED_AT_MAX_LENGTH
  ) {
    return false
  }
  const createdAtMs = Date.parse(cursor.createdAt)
  return (
    Number.isFinite(createdAtMs) &&
    new Date(createdAtMs).toISOString() === cursor.createdAt &&
    typeof cursor.id === 'string' &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
      cursor.id
    )
  )
}

function isUuid(value: string): boolean {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
    value
  )
}

function toBase64Url(value: string): string {
  return btoa(value).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/u, '')
}

function fromBase64Url(value: string): string {
  const normalized = value.replace(/-/g, '+').replace(/_/g, '/')
  const padded = normalized.padEnd(Math.ceil(normalized.length / 4) * 4, '=')
  return atob(padded)
}

export function createOrderEventStream(
  signal: AbortSignal,
  replicaRuntime: ReplicaDatabaseRuntime,
  limit: number,
  orderType: OrderTypeFilter | undefined,
  lifecycleFilter: OrderLifecycleFilter | undefined,
  includeSnapshot: boolean,
  orderCdcBroker: OrderCdcBroker | null,
  fetchMetrics?: () => Promise<OrderMetrics>
): Response {
  const encoder = new TextEncoder()
  let closed = false
  let abortListener: (() => void) | undefined
  let unsubscribe: (() => void) | undefined
  let heartbeat: ReturnType<typeof setInterval> | undefined
  let resolveClosed: () => void = () => undefined
  const closedPromise = new Promise<void>((resolve) => {
    resolveClosed = resolve
  })

  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      const close = () => {
        if (closed) return
        closed = true
        resolveClosed()
        unsubscribe?.()
        unsubscribe = undefined
        if (abortListener) {
          signal.removeEventListener('abort', abortListener)
          abortListener = undefined
        }
        if (heartbeat) {
          clearInterval(heartbeat)
          heartbeat = undefined
        }
        try {
          controller.close()
        } catch (_error) {
          // The client may have already closed the stream.
        }
      }

      abortListener = close
      signal.addEventListener('abort', close, { once: true })

      const write = (event: string, data: unknown) => {
        if (closed) return
        if (isSseStreamBackpressured(controller)) {
          close()
          return
        }
        try {
          controller.enqueue(
            encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`)
          )
        } catch (_error) {
          close()
        }
      }

      const writeComment = (comment: string) => {
        if (closed) return
        if (isSseStreamBackpressured(controller)) {
          close()
          return
        }
        try {
          controller.enqueue(encoder.encode(`: ${comment}\n\n`))
        } catch (_error) {
          close()
        }
      }

      heartbeat = setInterval(() => writeComment('heartbeat'), SSE_HEARTBEAT_MS)

      const loop = async () => {
        try {
          const pending: OrderStreamQueuedEvent[] = []
          let snapshotSent = !includeSnapshot
          const writeOrQueue = (event: OrderStreamQueuedEvent) => {
            if (!snapshotSent) {
              if (pending.length >= MAX_SSE_PENDING_EVENTS) {
                write('error', {
                  message: 'order stream snapshot backlog exceeded'
                })
                close()
                return
              }
              pending.push(event)
              return
            }

            if (event.kind === 'remove') {
              write('remove', {
                id: event.id,
                total: event.total,
                metrics: event.metrics,
                analyticsChanged: event.analyticsChanged,
                sort: event.sort
              })
            } else if (event.kind === 'metrics') {
              write('metrics', {
                total: event.total,
                metrics: event.metrics,
                analyticsChanged: event.analyticsChanged,
                sort: event.sort
              })
            } else {
              write('upsert', event)
            }
          }
          unsubscribe = orderCdcBroker?.subscribe(
            (event) => {
              if (event.kind === 'remove') {
                writeOrQueue(event)
                return
              }
              if (orderType && event.order.orderType !== orderType) {
                writeOrQueue({
                  kind: 'metrics',
                  total: event.total,
                  metrics: event.metrics,
                  analyticsChanged:
                    event.analyticsChanged ||
                    (event.order.status === 'completed' ? true : undefined),
                  sort: event.sort
                })
                return
              }
              if (!orderMatchesLifecycleFilter(event.order, lifecycleFilter)) {
                const removeEvent: OrderCdcRemove = {
                  kind: 'remove',
                  id: event.order.id,
                  total: event.total,
                  metrics: event.metrics,
                  analyticsChanged:
                    event.analyticsChanged ||
                    (event.order.status === 'completed' ? true : undefined),
                  sort: event.sort
                }
                writeOrQueue(removeEvent)
                return
              }
              writeOrQueue(event)
            },
            { ackRequired: false }
          )

          write('ready', { sort: 'created_at_desc' })

          if (includeSnapshot) {
            const page = await fetchOrderPage(
              replicaRuntime,
              limit,
              undefined,
              orderType,
              lifecycleFilter,
              true,
              fetchMetrics
            )
            if (!page.metrics) {
              throw new Error('order snapshot metrics were not loaded')
            }
            write('snapshot', {
              orders: page.orders,
              nextCursor: page.nextCursor,
              total: page.metrics.total,
              metrics: page.metrics,
              sort: 'created_at_desc'
            })
            snapshotSent = true
            for (const event of pending) {
              writeOrQueue(event)
            }
            pending.length = 0
          }

          await waitForAbortOrClose(signal, closedPromise)
          close()
        } catch (error) {
          logError('admin dashboard order SSE stream failed', error)
          write('error', {
            message: 'order stream failed'
          })
          close()
        }
      }

      void loop()
    },
    cancel() {
      if (!closed) {
        closed = true
        resolveClosed()
      }
      unsubscribe?.()
      if (heartbeat) {
        clearInterval(heartbeat)
        heartbeat = undefined
      }
      if (abortListener) {
        signal.removeEventListener('abort', abortListener)
        abortListener = undefined
      }
    }
  })

  return new Response(stream, {
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no'
    }
  })
}

function isSseStreamBackpressured(
  controller: ReadableStreamDefaultController<Uint8Array>
): boolean {
  return (
    typeof controller.desiredSize === 'number' &&
    controller.desiredSize <= -MAX_SSE_BACKPRESSURE_CHUNKS
  )
}

function waitForAbortOrClose(
  signal: AbortSignal,
  closedPromise: Promise<void>
): Promise<void> {
  if (signal.aborted) return Promise.resolve()
  let abortListener: (() => void) | undefined
  return new Promise<void>((resolve) => {
    abortListener = () => resolve(undefined)
    signal.addEventListener('abort', abortListener, { once: true })
    void closedPromise.then(resolve, resolve)
  }).finally(() => {
    if (abortListener) signal.removeEventListener('abort', abortListener)
  })
}
