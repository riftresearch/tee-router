import { Pool, type PoolClient } from 'pg'

import type { AdminDashboardConfig } from './config'
import {
  fetchCompletedOrderAnalyticsUpperBound,
  fetchCompletedOrdersForAnalytics,
  fetchOrderStatusAnalyticsUpperBound,
  fetchOrderStatusesForAnalytics,
  type AssetRef,
  type CompletedOrderAnalyticsBound,
  type CompletedOrderAnalyticsCursor,
  type OrderAnalyticsRow,
  type OrderExecutionLeg,
  type OrderFirehoseRow,
  type OrderMetrics,
  type OrderStatusAnalyticsBound,
  type OrderStatusAnalyticsCursor,
  type OrderStatusAnalyticsRow,
  type OrderTypeFilter
} from './orders'
import {
  parseNonNegativeSafeInteger,
  parseU256Decimal,
  parseU64Decimal
} from './numeric'
import { logError } from './logging'

export type VolumeBucketSize = 'five_minute' | 'minute' | 'hour' | 'day'
export type VolumeOrderTypeFilter = 'all' | OrderTypeFilter

export type VolumeBucketRow = {
  bucketStart: string
  bucketSize: VolumeBucketSize
  orderType: VolumeOrderTypeFilter
  volumeUsdMicro: string
  orderCount: number
}

type VolumeContribution = {
  orderId: string
  orderType: OrderTypeFilter
  completedAt: Date
  volumeUsdMicro: bigint
}

type VolumeBucketDbRow = {
  bucket_start: Date
  volume_usd_micro: string
  order_count: string
}

type StoredContribution = {
  order_id: string
  order_type: OrderTypeFilter
  completed_at: Date
  volume_usd_micro: string
}

type StoredIngestionState = {
  order_id: string
  source_updated_at: Date
  contributes: boolean
}

type StoredStatusState = {
  order_id: string
  source_updated_at: Date
  status: string | null
  present: boolean
}

type ContributionSyncOptions = {
  touchUnchanged?: boolean
}

type BackfillOptions = {
  fullSnapshot?: boolean
}

type VolumeBucketKey = {
  bucketSize: VolumeBucketSize
  bucketStart: Date
  orderType: OrderTypeFilter
}

type BackfillStateRow = {
  backfill_name: string
  completed: boolean
  cursor_created_at: Date | null
  cursor_order_id: string | null
  snapshot_started_at: Date | null
  snapshot_upper_created_at: Date | null
  snapshot_upper_order_id: string | null
}

type PendingBackfill = {
  replicaPool: Pool
  options: BackfillOptions
}

const BUCKET_SIZES: VolumeBucketSize[] = ['minute', 'hour', 'day']
const BACKFILL_PAGE_SIZE = 250
const SNAPSHOT_PRUNE_BATCH_SIZE = 1_000
const COMPLETED_ORDER_BACKFILL_NAME = 'completed_order_volume_v1'
const ORDER_STATUS_BACKFILL_NAME = 'order_status_metrics_v1'
const ACTIVE_ORDER_STATUSES = new Set([
  'pending_funding',
  'funded',
  'executing',
  'refund_required',
  'refunding'
])
const NEEDS_ATTENTION_ORDER_STATUSES = new Set([
  'refund_required',
  'refunding',
  'manual_intervention_required',
  'refund_manual_intervention_required'
])

export class VolumeAnalyticsRuntime {
  private readonly pool: Pool
  private readonly ready: Promise<void>
  private backfillRunning = false
  private pendingBackfill: PendingBackfill | undefined
  private backfillRunPromise: Promise<void> | undefined
  private closed = false
  private backfillRetryTimer: ReturnType<typeof setTimeout> | undefined
  private resolveBackfillRetrySleep: (() => void) | undefined

  constructor(databaseUrl: string) {
    this.pool = new Pool({
      connectionString: databaseUrl,
      max: 5,
      application_name: 'rift-admin-dashboard-analytics'
    })
    this.ready = this.migrate()
  }

  async close() {
    this.closed = true
    if (this.backfillRetryTimer) clearTimeout(this.backfillRetryTimer)
    this.resolveBackfillRetrySleep?.()
    await this.pool.end()
  }

  startBackfill(replicaPool: Pool, options: BackfillOptions = {}) {
    if (this.closed) return Promise.resolve()
    if (this.backfillRunning) {
      this.pendingBackfill = mergeBackfillRequests(this.pendingBackfill, {
        replicaPool,
        options
      })
      return this.backfillRunPromise ?? Promise.resolve()
    }

    this.backfillRunning = true
    this.backfillRunPromise = this.runBackfillRequests({ replicaPool, options }).finally(
      () => {
        this.backfillRunPromise = undefined
      }
    )
    return this.backfillRunPromise
  }

  async ingestOrder(order: OrderFirehoseRow) {
    const changedOrderIds = await this.ingestOrders([order])
    return changedOrderIds.has(order.id)
  }

  async removeOrder(orderId: string, sourceUpdatedAt?: string) {
    await this.ready
    const client = await this.pool.connect()
    try {
      await client.query('BEGIN')
      const affectedBuckets = new Map<string, VolumeBucketKey>()
      const volumeChanged = await this.syncContribution(
        client,
        orderId,
        sourceUpdatedAt
          ? parseSourceUpdatedAt(orderId, sourceUpdatedAt)
          : undefined,
        undefined,
        affectedBuckets
      )
      await this.syncStatus(
        client,
        orderId,
        sourceUpdatedAt
          ? parseSourceUpdatedAt(orderId, sourceUpdatedAt)
          : undefined,
        undefined
      )
      await this.recomputeBuckets(client, affectedBuckets)
      await client.query('COMMIT')
      return volumeChanged
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined)
      throw error
    } finally {
      client.release()
    }
  }

  async ingestOrders(
    orders: OrderAnalyticsRow[],
    options: ContributionSyncOptions = {}
  ): Promise<Set<string>> {
    await this.ready
    const changedOrderIds = new Set<string>()
    if (orders.length === 0) return changedOrderIds

    const client = await this.pool.connect()
    try {
      await client.query('BEGIN')
      const affectedBuckets = new Map<string, VolumeBucketKey>()
      for (const order of orders) {
        const sourceUpdatedAt = orderSourceUpdatedAt(order)
        const volumeChanged = await this.syncContribution(
          client,
          order.id,
          sourceUpdatedAt,
          completedOrderContribution(order),
          affectedBuckets,
          options
        )
        if (volumeChanged) changedOrderIds.add(order.id)
        await this.syncStatus(
          client,
          order.id,
          sourceUpdatedAt,
          order.status,
          options
        )
      }
      await this.recomputeBuckets(client, affectedBuckets)
      await client.query('COMMIT')
      return changedOrderIds
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined)
      throw error
    } finally {
      client.release()
    }
  }

  private async ingestOrderStatuses(
    statuses: OrderStatusAnalyticsRow[],
    options: ContributionSyncOptions = {}
  ) {
    await this.ready
    if (statuses.length === 0) return

    const client = await this.pool.connect()
    try {
      await client.query('BEGIN')
      for (const status of statuses) {
        await this.syncStatus(
          client,
          status.id,
          parseSourceUpdatedAt(status.id, status.updatedAt),
          status.status,
          options
        )
      }
      await client.query('COMMIT')
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined)
      throw error
    } finally {
      client.release()
    }
  }

  async fetchVolumeBuckets({
    bucketSize,
    from,
    to,
    orderType
  }: {
    bucketSize: VolumeBucketSize
    from: Date
    to: Date
    orderType: VolumeOrderTypeFilter
  }): Promise<VolumeBucketRow[]> {
    await this.ready
    if (bucketSize === 'five_minute') {
      const result =
        orderType === 'all'
          ? await this.pool.query<VolumeBucketDbRow>(
              `
              SELECT
                (
                  date_trunc('hour', bucket_start)
                  + floor(date_part('minute', bucket_start) / 5) * interval '5 minutes'
                ) AS bucket_start,
                SUM(volume_usd_micro)::text AS volume_usd_micro,
                SUM(order_count)::text AS order_count
              FROM admin_volume_buckets
              WHERE bucket_size = 'minute'
                AND bucket_start < $2
                AND bucket_start + interval '1 minute' > $1
              GROUP BY 1
              ORDER BY 1 ASC
              `,
              [from, to]
            )
          : await this.pool.query<VolumeBucketDbRow>(
              `
              SELECT
                (
                  date_trunc('hour', bucket_start)
                  + floor(date_part('minute', bucket_start) / 5) * interval '5 minutes'
                ) AS bucket_start,
                SUM(volume_usd_micro)::text AS volume_usd_micro,
                SUM(order_count)::text AS order_count
              FROM admin_volume_buckets
              WHERE bucket_size = 'minute'
                AND order_type = $1
                AND bucket_start < $3
                AND bucket_start + interval '1 minute' > $2
              GROUP BY 1
              ORDER BY 1 ASC
              `,
              [orderType, from, to]
            )

      return result.rows.map((row) => ({
        bucketStart: row.bucket_start.toISOString(),
        bucketSize,
        orderType,
        volumeUsdMicro: row.volume_usd_micro,
        orderCount: parseNonNegativeSafeInteger(row.order_count, 'volume bucket order_count')
      }))
    }

    const result =
      orderType === 'all'
        ? await this.pool.query<VolumeBucketDbRow>(
            `
            SELECT
              bucket_start,
              SUM(volume_usd_micro)::text AS volume_usd_micro,
              SUM(order_count)::text AS order_count
            FROM admin_volume_buckets
            WHERE bucket_size = $1
              AND bucket_start < $3
              AND bucket_start + CASE
                WHEN bucket_size = 'minute' THEN interval '1 minute'
                WHEN bucket_size = 'hour' THEN interval '1 hour'
                ELSE interval '1 day'
              END > $2
            GROUP BY bucket_start
            ORDER BY bucket_start ASC
            `,
            [bucketSize, from, to]
          )
        : await this.pool.query<VolumeBucketDbRow>(
            `
            SELECT
              bucket_start,
              SUM(volume_usd_micro)::text AS volume_usd_micro,
              SUM(order_count)::text AS order_count
            FROM admin_volume_buckets
            WHERE bucket_size = $1
              AND order_type = $2
              AND bucket_start < $4
              AND bucket_start + CASE
                WHEN bucket_size = 'minute' THEN interval '1 minute'
                WHEN bucket_size = 'hour' THEN interval '1 hour'
                ELSE interval '1 day'
              END > $3
            GROUP BY bucket_start
            ORDER BY bucket_start ASC
            `,
            [bucketSize, orderType, from, to]
          )

    return result.rows.map((row) => ({
      bucketStart: row.bucket_start.toISOString(),
      bucketSize,
      orderType,
      volumeUsdMicro: row.volume_usd_micro,
      orderCount: parseNonNegativeSafeInteger(row.order_count, 'volume bucket order_count')
    }))
  }

  async fetchOrderMetrics(): Promise<OrderMetrics> {
    await this.ready
    const result = await this.pool.query<{ status: string; order_count: string }>(
      `
      SELECT status, order_count::text AS order_count
      FROM admin_order_status_counts
      `
    )

    let total = 0
    let active = 0
    let needsAttention = 0
    for (const row of result.rows) {
      const count = parseNonNegativeSafeInteger(
        row.order_count,
        `order status count ${row.status}`
      )
      total = addSafeMetricCount(total, count, 'order metric total')
      if (ACTIVE_ORDER_STATUSES.has(row.status)) {
        active = addSafeMetricCount(active, count, 'order metric active')
      }
      if (NEEDS_ATTENTION_ORDER_STATUSES.has(row.status)) {
        needsAttention = addSafeMetricCount(
          needsAttention,
          count,
          'order metric needs_attention'
        )
      }
    }

    return {
      total,
      active,
      needsAttention
    }
  }

  private async migrate() {
    const client = await this.pool.connect()
    try {
      await client.query('BEGIN')
      await client.query(`
      CREATE TABLE IF NOT EXISTS admin_volume_order_contributions (
        order_id uuid PRIMARY KEY,
        order_type text NOT NULL CHECK (order_type IN ('market_order', 'limit_order')),
        completed_at timestamptz NOT NULL,
        volume_usd_micro numeric NOT NULL CHECK (volume_usd_micro >= 0),
        updated_at timestamptz NOT NULL DEFAULT now()
      );

      CREATE TABLE IF NOT EXISTS admin_volume_order_ingestion_state (
        order_id uuid PRIMARY KEY,
        source_updated_at timestamptz NOT NULL,
        contributes boolean NOT NULL,
        updated_at timestamptz NOT NULL DEFAULT now()
      );

      CREATE TABLE IF NOT EXISTS admin_volume_buckets (
        bucket_size text NOT NULL CHECK (bucket_size IN ('minute', 'hour', 'day')),
        bucket_start timestamptz NOT NULL,
        order_type text NOT NULL CHECK (order_type IN ('market_order', 'limit_order')),
        volume_usd_micro numeric NOT NULL DEFAULT 0 CHECK (volume_usd_micro >= 0),
        order_count bigint NOT NULL DEFAULT 0 CHECK (order_count >= 0),
        updated_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (bucket_size, bucket_start, order_type)
      );

      DROP INDEX IF EXISTS idx_admin_volume_buckets_lookup;

      CREATE INDEX IF NOT EXISTS idx_admin_volume_buckets_type_lookup
        ON admin_volume_buckets (bucket_size, order_type, bucket_start);

      CREATE INDEX IF NOT EXISTS idx_admin_volume_order_contributions_bucket_lookup
        ON admin_volume_order_contributions (order_type, completed_at);

      CREATE INDEX IF NOT EXISTS idx_admin_volume_order_contributions_updated_at
        ON admin_volume_order_contributions (updated_at);

      CREATE INDEX IF NOT EXISTS idx_admin_volume_order_ingestion_state_updated_at
        ON admin_volume_order_ingestion_state (updated_at);

      CREATE TABLE IF NOT EXISTS admin_order_status_counts (
        status text PRIMARY KEY,
        order_count bigint NOT NULL DEFAULT 0 CHECK (order_count >= 0),
        updated_at timestamptz NOT NULL DEFAULT now()
      );

      CREATE TABLE IF NOT EXISTS admin_order_status_ingestion_state (
        order_id uuid PRIMARY KEY,
        source_updated_at timestamptz NOT NULL,
        status text,
        present boolean NOT NULL,
        updated_at timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT admin_order_status_state_present_status CHECK (
          (present AND status IS NOT NULL) OR ((NOT present) AND status IS NULL)
        )
      );

      CREATE INDEX IF NOT EXISTS idx_admin_order_status_ingestion_state_updated_at
        ON admin_order_status_ingestion_state (updated_at);

      CREATE INDEX IF NOT EXISTS idx_admin_order_status_ingestion_state_stale_present
        ON admin_order_status_ingestion_state (updated_at)
        WHERE present = true AND status IS NOT NULL;

      CREATE TABLE IF NOT EXISTS admin_volume_backfill_state (
        backfill_name text PRIMARY KEY,
        completed boolean NOT NULL DEFAULT false,
        cursor_created_at timestamptz,
        cursor_order_id uuid,
        snapshot_started_at timestamptz,
        snapshot_upper_created_at timestamptz,
        snapshot_upper_order_id uuid,
        updated_at timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT admin_volume_backfill_state_cursor_pair CHECK (
          completed
          OR (
            cursor_created_at IS NULL
            AND cursor_order_id IS NULL
          )
          OR (
            cursor_created_at IS NOT NULL
            AND cursor_order_id IS NOT NULL
          )
        ),
        CONSTRAINT admin_volume_backfill_state_snapshot_upper_pair CHECK (
          completed
          OR (
            snapshot_upper_created_at IS NULL
            AND snapshot_upper_order_id IS NULL
          )
          OR (
            snapshot_upper_created_at IS NOT NULL
            AND snapshot_upper_order_id IS NOT NULL
          )
        )
      );

      DO $$
      BEGIN
        IF EXISTS (
          SELECT 1
          FROM information_schema.columns
          WHERE table_schema = 'public'
            AND table_name = 'admin_volume_buckets'
            AND column_name = 'order_count'
            AND data_type <> 'bigint'
        ) THEN
          ALTER TABLE admin_volume_buckets
            ALTER COLUMN order_count TYPE bigint
            USING order_count::bigint;
        END IF;

        IF NOT EXISTS (
          SELECT 1
          FROM pg_constraint
          WHERE conname = 'admin_volume_buckets_volume_nonnegative'
        ) THEN
          ALTER TABLE admin_volume_buckets
            ADD CONSTRAINT admin_volume_buckets_volume_nonnegative
            CHECK (volume_usd_micro >= 0);
        END IF;

        IF NOT EXISTS (
          SELECT 1
          FROM pg_constraint
          WHERE conname = 'admin_volume_buckets_order_count_nonnegative'
        ) THEN
          ALTER TABLE admin_volume_buckets
            ADD CONSTRAINT admin_volume_buckets_order_count_nonnegative
            CHECK (order_count >= 0);
        END IF;

        IF NOT EXISTS (
          SELECT 1
          FROM information_schema.columns
          WHERE table_schema = 'public'
            AND table_name = 'admin_volume_backfill_state'
            AND column_name = 'snapshot_started_at'
        ) THEN
          ALTER TABLE admin_volume_backfill_state
            ADD COLUMN snapshot_started_at timestamptz;
        END IF;

        IF NOT EXISTS (
          SELECT 1
          FROM information_schema.columns
          WHERE table_schema = 'public'
            AND table_name = 'admin_volume_backfill_state'
            AND column_name = 'snapshot_upper_created_at'
        ) THEN
          ALTER TABLE admin_volume_backfill_state
            ADD COLUMN snapshot_upper_created_at timestamptz;
        END IF;

        IF NOT EXISTS (
          SELECT 1
          FROM information_schema.columns
          WHERE table_schema = 'public'
            AND table_name = 'admin_volume_backfill_state'
            AND column_name = 'snapshot_upper_order_id'
        ) THEN
          ALTER TABLE admin_volume_backfill_state
            ADD COLUMN snapshot_upper_order_id uuid;
        END IF;

        IF NOT EXISTS (
          SELECT 1
          FROM pg_constraint
          WHERE conname = 'admin_volume_backfill_state_cursor_pair'
        ) THEN
          ALTER TABLE admin_volume_backfill_state
            ADD CONSTRAINT admin_volume_backfill_state_cursor_pair
            CHECK (
              completed
              OR (
                cursor_created_at IS NULL
                AND cursor_order_id IS NULL
              )
              OR (
                cursor_created_at IS NOT NULL
                AND cursor_order_id IS NOT NULL
              )
            );
        END IF;

        IF NOT EXISTS (
          SELECT 1
          FROM pg_constraint
          WHERE conname = 'admin_volume_backfill_state_snapshot_upper_pair'
        ) THEN
          ALTER TABLE admin_volume_backfill_state
            ADD CONSTRAINT admin_volume_backfill_state_snapshot_upper_pair
            CHECK (
              completed
              OR (
                snapshot_upper_created_at IS NULL
                AND snapshot_upper_order_id IS NULL
              )
              OR (
                snapshot_upper_created_at IS NOT NULL
                AND snapshot_upper_order_id IS NOT NULL
              )
            );
        END IF;
      END
      $$;
    `)
      await this.rebuildBucketsFromContributions(client)
      await this.rebuildStatusCounts(client)
      await client.query('COMMIT')
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined)
      throw error
    } finally {
      client.release()
    }
  }

  private async runBackfillRequests(initialRequest: PendingBackfill) {
    let request: PendingBackfill | undefined = initialRequest
    try {
      while (request && !this.closed) {
        await this.runBackfill(request.replicaPool, request.options)
        request = this.pendingBackfill
        this.pendingBackfill = undefined
      }
    } finally {
      this.backfillRunning = false
    }
  }

  private async runBackfill(replicaPool: Pool, options: BackfillOptions) {
    await this.ready
    let retryDelayMs = 1_000
    let volumeBackfillComplete = false
    let statusBackfillComplete = false

    while (!this.closed) {
      try {
        if (!volumeBackfillComplete) {
          await this.backfillVolumeOnce(replicaPool, options)
          volumeBackfillComplete = true
        }
        if (!statusBackfillComplete) {
          await this.backfillStatusOnce(replicaPool, options)
          statusBackfillComplete = true
        }
        return
      } catch (error) {
        if (this.closed) return
        logError('admin volume analytics backfill failed; retrying', error)
        await this.sleepForBackfillRetry(retryDelayMs)
        retryDelayMs = Math.min(retryDelayMs * 2, 30_000)
      }
    }
  }

  private async sleepForBackfillRetry(ms: number) {
    if (this.closed) return
    await new Promise<void>((resolve) => {
      const finish = () => {
        if (this.backfillRetryTimer === timer) this.backfillRetryTimer = undefined
        if (this.resolveBackfillRetrySleep === finish) {
          this.resolveBackfillRetrySleep = undefined
        }
        resolve()
      }
      const timer = setTimeout(finish, ms)
      this.backfillRetryTimer = timer
      this.resolveBackfillRetrySleep = finish
    })
  }

  private async backfillVolumeOnce(
    replicaPool: Pool,
    options: BackfillOptions = { fullSnapshot: true }
  ) {
    let fullSnapshot = options.fullSnapshot ?? true
    let cursor: CompletedOrderAnalyticsCursor | undefined
    let snapshotStartedAt: Date | undefined
    let snapshotUpperBound: CompletedOrderAnalyticsBound | undefined

    if (fullSnapshot) {
      snapshotStartedAt = await this.readDatabaseTimestamp()
      snapshotUpperBound = await fetchCompletedOrderAnalyticsUpperBound(replicaPool)
    } else {
      const state = await this.readBackfillState(COMPLETED_ORDER_BACKFILL_NAME)
      if (state?.completed) return
      cursor = backfillCursorFromState(state)
      snapshotStartedAt = state?.snapshot_started_at ?? undefined
      snapshotUpperBound = backfillUpperBoundFromState(state)
      if (!cursor || !snapshotStartedAt) {
        fullSnapshot = true
        snapshotStartedAt = await this.readDatabaseTimestamp()
        snapshotUpperBound = await fetchCompletedOrderAnalyticsUpperBound(replicaPool)
      }
    }

    if (fullSnapshot && !snapshotUpperBound) {
      await this.pruneSnapshotStaleContributions(
        snapshotStartedAt ?? (await this.readDatabaseTimestamp())
      )
      await this.markBackfillComplete(COMPLETED_ORDER_BACKFILL_NAME, undefined)
      return
    }

    for (;;) {
      const orders = await fetchCompletedOrdersForAnalytics(
        replicaPool,
        BACKFILL_PAGE_SIZE,
        cursor,
        snapshotUpperBound
      )
      if (orders.length === 0) {
        if (fullSnapshot) {
          await this.pruneSnapshotStaleContributions(
            snapshotStartedAt ?? (await this.readDatabaseTimestamp())
          )
        }
        await this.markBackfillComplete(COMPLETED_ORDER_BACKFILL_NAME, cursor)
        return
      }
      await this.ingestOrders(orders, { touchUnchanged: fullSnapshot })
      const last = orders[orders.length - 1]
      cursor = { createdAt: last.createdAt, id: last.id }
      if (orders.length < BACKFILL_PAGE_SIZE) {
        if (fullSnapshot) {
          await this.pruneSnapshotStaleContributions(
            snapshotStartedAt ?? (await this.readDatabaseTimestamp())
          )
        }
        await this.markBackfillComplete(COMPLETED_ORDER_BACKFILL_NAME, cursor)
        return
      }
      await this.saveBackfillCursor(
        COMPLETED_ORDER_BACKFILL_NAME,
        cursor,
        snapshotStartedAt,
        snapshotUpperBound
      )
    }
  }

  private async backfillOnce(
    replicaPool: Pool,
    options: BackfillOptions = { fullSnapshot: true }
  ) {
    await this.backfillVolumeOnce(replicaPool, options)
  }

  private async backfillStatusOnce(
    replicaPool: Pool,
    options: BackfillOptions = { fullSnapshot: true }
  ) {
    let fullSnapshot = options.fullSnapshot ?? true
    let cursor: OrderStatusAnalyticsCursor | undefined
    let snapshotStartedAt: Date | undefined
    let snapshotUpperBound: OrderStatusAnalyticsBound | undefined

    if (fullSnapshot) {
      snapshotStartedAt = await this.readDatabaseTimestamp()
      snapshotUpperBound = await fetchOrderStatusAnalyticsUpperBound(replicaPool)
    } else {
      const state = await this.readBackfillState(ORDER_STATUS_BACKFILL_NAME)
      if (state?.completed) return
      cursor = backfillCursorFromState(state)
      snapshotStartedAt = state?.snapshot_started_at ?? undefined
      snapshotUpperBound = backfillUpperBoundFromState(state)
      if (!cursor || !snapshotStartedAt) {
        fullSnapshot = true
        snapshotStartedAt = await this.readDatabaseTimestamp()
        snapshotUpperBound = await fetchOrderStatusAnalyticsUpperBound(replicaPool)
      }
    }

    if (fullSnapshot && !snapshotUpperBound) {
      await this.pruneSnapshotStaleStatuses(
        snapshotStartedAt ?? (await this.readDatabaseTimestamp())
      )
      await this.markBackfillComplete(ORDER_STATUS_BACKFILL_NAME, undefined)
      return
    }

    for (;;) {
      const statuses = await fetchOrderStatusesForAnalytics(
        replicaPool,
        BACKFILL_PAGE_SIZE,
        cursor,
        snapshotUpperBound
      )
      if (statuses.length === 0) {
        if (fullSnapshot) {
          await this.pruneSnapshotStaleStatuses(
            snapshotStartedAt ?? (await this.readDatabaseTimestamp())
          )
        }
        await this.markBackfillComplete(ORDER_STATUS_BACKFILL_NAME, cursor)
        return
      }
      await this.ingestOrderStatuses(statuses, { touchUnchanged: fullSnapshot })
      const last = statuses[statuses.length - 1]
      cursor = { createdAt: last.createdAt, id: last.id }
      if (statuses.length < BACKFILL_PAGE_SIZE) {
        if (fullSnapshot) {
          await this.pruneSnapshotStaleStatuses(
            snapshotStartedAt ?? (await this.readDatabaseTimestamp())
          )
        }
        await this.markBackfillComplete(ORDER_STATUS_BACKFILL_NAME, cursor)
        return
      }
      await this.saveBackfillCursor(
        ORDER_STATUS_BACKFILL_NAME,
        cursor,
        snapshotStartedAt,
        snapshotUpperBound
      )
    }
  }

  private async readBackfillState(
    backfillName: string = COMPLETED_ORDER_BACKFILL_NAME
  ): Promise<BackfillStateRow | undefined> {
    await this.ready
    const result = await this.pool.query<BackfillStateRow>(
      `
      SELECT
        backfill_name,
        completed,
        cursor_created_at,
        cursor_order_id::text,
        snapshot_started_at,
        snapshot_upper_created_at,
        snapshot_upper_order_id::text
      FROM admin_volume_backfill_state
      WHERE backfill_name = $1
      `,
      [backfillName]
    )
    return result.rows[0]
  }

  private async saveBackfillCursor(
    backfillNameOrCursor: string | CompletedOrderAnalyticsCursor,
    cursorOrSnapshotStartedAt?: CompletedOrderAnalyticsCursor | Date,
    snapshotStartedAtOrUpperBound?: Date | CompletedOrderAnalyticsBound,
    snapshotUpperBoundMaybe?: CompletedOrderAnalyticsBound
  ) {
    const backfillName =
      typeof backfillNameOrCursor === 'string'
        ? backfillNameOrCursor
        : COMPLETED_ORDER_BACKFILL_NAME
    const cursor =
      typeof backfillNameOrCursor === 'string'
        ? (cursorOrSnapshotStartedAt as CompletedOrderAnalyticsCursor)
        : backfillNameOrCursor
    const snapshotStartedAt =
      typeof backfillNameOrCursor === 'string'
        ? (snapshotStartedAtOrUpperBound as Date | undefined)
        : (cursorOrSnapshotStartedAt as Date | undefined)
    const snapshotUpperBound =
      typeof backfillNameOrCursor === 'string'
        ? snapshotUpperBoundMaybe
        : (snapshotStartedAtOrUpperBound as CompletedOrderAnalyticsBound | undefined)
    await this.ready
    const snapshotTimestamp = snapshotStartedAt ?? (await this.readDatabaseTimestamp())
    await this.pool.query(
      `
      INSERT INTO admin_volume_backfill_state (
        backfill_name,
        completed,
        cursor_created_at,
        cursor_order_id,
        snapshot_started_at,
        snapshot_upper_created_at,
        snapshot_upper_order_id,
        updated_at
      )
      VALUES ($1, false, $2, $3, $4, $5, $6, now())
      ON CONFLICT (backfill_name) DO UPDATE
      SET
        completed = false,
        cursor_created_at = EXCLUDED.cursor_created_at,
        cursor_order_id = EXCLUDED.cursor_order_id,
        snapshot_started_at = EXCLUDED.snapshot_started_at,
        snapshot_upper_created_at = EXCLUDED.snapshot_upper_created_at,
        snapshot_upper_order_id = EXCLUDED.snapshot_upper_order_id,
        updated_at = now()
      `,
      [
        backfillName,
        cursor.createdAt,
        cursor.id,
        snapshotTimestamp,
        snapshotUpperBound?.createdAt ?? null,
        snapshotUpperBound?.id ?? null
      ]
    )
  }

  private async markBackfillComplete(
    backfillNameOrCursor?: string | CompletedOrderAnalyticsCursor,
    cursorMaybe?: CompletedOrderAnalyticsCursor
  ) {
    const backfillName =
      typeof backfillNameOrCursor === 'string'
        ? backfillNameOrCursor
        : COMPLETED_ORDER_BACKFILL_NAME
    const cursor =
      typeof backfillNameOrCursor === 'string' ? cursorMaybe : backfillNameOrCursor
    await this.ready
    await this.pool.query(
      `
      INSERT INTO admin_volume_backfill_state (
        backfill_name,
        completed,
        cursor_created_at,
        cursor_order_id,
        snapshot_started_at,
        snapshot_upper_created_at,
        snapshot_upper_order_id,
        updated_at
      )
      VALUES ($1, true, $2, $3, null, null, null, now())
      ON CONFLICT (backfill_name) DO UPDATE
      SET
        completed = true,
        cursor_created_at = EXCLUDED.cursor_created_at,
        cursor_order_id = EXCLUDED.cursor_order_id,
        snapshot_started_at = null,
        snapshot_upper_created_at = null,
        snapshot_upper_order_id = null,
        updated_at = now()
      `,
      [backfillName, cursor?.createdAt ?? null, cursor?.id ?? null]
    )
  }

  private async syncContribution(
    client: PoolClient,
    orderId: string,
    sourceUpdatedAt: Date | undefined,
    contribution: VolumeContribution | undefined,
    affectedBuckets: Map<string, VolumeBucketKey>,
    options: ContributionSyncOptions = {}
  ): Promise<boolean> {
    await client.query('SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))', [
      orderId
    ])
    const effectiveSourceUpdatedAt =
      sourceUpdatedAt ?? (await readClientTimestamp(client))
    const state = await client.query<StoredIngestionState>(
      `
      SELECT order_id, source_updated_at, contributes
      FROM admin_volume_order_ingestion_state
      WHERE order_id = $1
      FOR UPDATE
      `,
      [orderId]
    )
    const previousState = state.rows[0]
    if (
      previousState &&
      previousIngestionStateDominates(
        previousState,
        effectiveSourceUpdatedAt,
        Boolean(contribution)
      )
    ) {
      if (options.touchUnchanged) {
        await this.touchDominatedContribution(
          client,
          orderId,
          previousState.contributes
        )
      }
      return false
    }

    const existing = await client.query<StoredContribution>(
      `
      SELECT order_id, order_type, completed_at, volume_usd_micro
      FROM admin_volume_order_contributions
      WHERE order_id = $1
      FOR UPDATE
      `,
      [orderId]
    )

    const previous = existing.rows[0]
    if (previous && contributionMatches(previous, contribution)) {
      await this.upsertIngestionState(
        client,
        orderId,
        effectiveSourceUpdatedAt,
        Boolean(contribution)
      )
      if (options.touchUnchanged) {
        await client.query(
          `
          UPDATE admin_volume_order_contributions
          SET updated_at = now()
          WHERE order_id = $1
          `,
          [orderId]
        )
      }
      return false
    }

    const volumeChanged = Boolean(previous) || Boolean(contribution)
    if (previous) addAffectedBuckets(affectedBuckets, previous)
    if (contribution) addAffectedBuckets(affectedBuckets, contribution)

    if (!contribution) {
      await client.query(
        `
        DELETE FROM admin_volume_order_contributions
        WHERE order_id = $1
        `,
        [orderId]
      )
      await this.upsertIngestionState(client, orderId, effectiveSourceUpdatedAt, false)
      return volumeChanged
    }

    await client.query(
      `
      INSERT INTO admin_volume_order_contributions (
        order_id,
        order_type,
        completed_at,
        volume_usd_micro,
        updated_at
      )
      VALUES ($1, $2, $3, $4, now())
      ON CONFLICT (order_id) DO UPDATE
      SET
        order_type = EXCLUDED.order_type,
        completed_at = EXCLUDED.completed_at,
        volume_usd_micro = EXCLUDED.volume_usd_micro,
        updated_at = now()
      `,
      [
        orderId,
        contribution.orderType,
        contribution.completedAt,
        contribution.volumeUsdMicro.toString()
      ]
    )
    await this.upsertIngestionState(client, orderId, effectiveSourceUpdatedAt, true)
    return volumeChanged
  }

  private async syncStatus(
    client: PoolClient,
    orderId: string,
    sourceUpdatedAt: Date | undefined,
    status: string | undefined,
    options: ContributionSyncOptions = {}
  ) {
    await client.query('SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))', [
      orderId
    ])
    const effectiveSourceUpdatedAt =
      sourceUpdatedAt ?? (await readClientTimestamp(client))
    const incomingPresent = status !== undefined
    const state = await client.query<StoredStatusState>(
      `
      SELECT order_id, source_updated_at, status, present
      FROM admin_order_status_ingestion_state
      WHERE order_id = $1
      FOR UPDATE
      `,
      [orderId]
    )
    const previous = state.rows[0]
    if (
      previous &&
      previousStatusStateDominates(previous, effectiveSourceUpdatedAt, incomingPresent)
    ) {
      if (options.touchUnchanged) {
        await this.touchDominatedStatus(client, orderId, previous.present)
      }
      return
    }

    if (
      previous?.present === incomingPresent &&
      previous?.status === (status ?? null)
    ) {
      await this.upsertStatusState(
        client,
        orderId,
        effectiveSourceUpdatedAt,
        status
      )
      if (options.touchUnchanged) {
        await client.query(
          `
          UPDATE admin_order_status_ingestion_state
          SET updated_at = now()
          WHERE order_id = $1
          `,
          [orderId]
        )
      }
      return
    }

    if (previous?.present && previous.status) {
      await this.applyStatusDelta(client, previous.status, -1)
    }
    if (status) {
      await this.applyStatusDelta(client, status, 1)
    }
    await this.upsertStatusState(
      client,
      orderId,
      effectiveSourceUpdatedAt,
      status
    )
  }

  private async upsertIngestionState(
    client: PoolClient,
    orderId: string,
    sourceUpdatedAt: Date,
    contributes: boolean
  ) {
    await client.query(
      `
      INSERT INTO admin_volume_order_ingestion_state (
        order_id,
        source_updated_at,
        contributes,
        updated_at
      )
      VALUES ($1, $2, $3, now())
      ON CONFLICT (order_id) DO UPDATE
      SET
        source_updated_at = EXCLUDED.source_updated_at,
        contributes = EXCLUDED.contributes,
        updated_at = now()
      `,
      [orderId, sourceUpdatedAt, contributes]
    )
  }

  private async touchDominatedContribution(
    client: PoolClient,
    orderId: string,
    contributes: boolean
  ) {
    if (contributes) {
      await client.query(
        `
        UPDATE admin_volume_order_contributions
        SET updated_at = now()
        WHERE order_id = $1
        `,
        [orderId]
      )
    }
    await client.query(
      `
      UPDATE admin_volume_order_ingestion_state
      SET updated_at = now()
      WHERE order_id = $1
      `,
      [orderId]
    )
  }

  private async touchDominatedStatus(
    client: PoolClient,
    orderId: string,
    present: boolean
  ) {
    if (!present) return
    await client.query(
      `
      UPDATE admin_order_status_ingestion_state
      SET updated_at = now()
      WHERE order_id = $1
      `,
      [orderId]
    )
  }

  private async upsertStatusState(
    client: PoolClient,
    orderId: string,
    sourceUpdatedAt: Date,
    status: string | undefined
  ) {
    await client.query(
      `
      INSERT INTO admin_order_status_ingestion_state (
        order_id,
        source_updated_at,
        status,
        present,
        updated_at
      )
      VALUES ($1, $2, $3, $4, now())
      ON CONFLICT (order_id) DO UPDATE
      SET
        source_updated_at = EXCLUDED.source_updated_at,
        status = EXCLUDED.status,
        present = EXCLUDED.present,
        updated_at = now()
      `,
      [orderId, sourceUpdatedAt, status ?? null, status !== undefined]
    )
  }

  private async applyStatusDelta(
    client: PoolClient,
    status: string,
    delta: 1 | -1
  ) {
    if (delta > 0) {
      await client.query(
        `
        INSERT INTO admin_order_status_counts (status, order_count, updated_at)
        VALUES ($1, 1, now())
        ON CONFLICT (status) DO UPDATE
        SET
          order_count = admin_order_status_counts.order_count + 1,
          updated_at = now()
        `,
        [status]
      )
      return
    }

    const decremented = await client.query<{ order_count: string }>(
      `
      UPDATE admin_order_status_counts
      SET
        order_count = order_count - 1,
        updated_at = now()
      WHERE status = $1
      RETURNING order_count::text AS order_count
      `,
      [status]
    )
    const row = decremented.rows[0]
    if (!row) {
      throw new Error(`admin order status count missing for decrement: ${status}`)
    }
    if (BigInt(row.order_count) < 0n) {
      throw new Error(`admin order status count went negative: ${status}`)
    }
    if (row.order_count === '0') {
      await client.query('DELETE FROM admin_order_status_counts WHERE status = $1', [
        status
      ])
    }
  }

  private async pruneSnapshotStaleContributions(snapshotStartedAt: Date) {
    for (;;) {
      const pruned = await this.pruneSnapshotStaleContributionBatch(snapshotStartedAt)
      if (pruned < SNAPSHOT_PRUNE_BATCH_SIZE) return
    }
  }

  private async pruneSnapshotStaleContributionBatch(snapshotStartedAt: Date) {
    await this.ready
    const client = await this.pool.connect()
    try {
      await client.query('BEGIN')
      const deleted = await client.query<StoredContribution>(
        `
        WITH stale AS (
          SELECT order_id
          FROM admin_volume_order_contributions
          WHERE updated_at < $1
          ORDER BY updated_at ASC, order_id ASC
          LIMIT $2
          FOR UPDATE SKIP LOCKED
        ),
        deleted AS (
          DELETE FROM admin_volume_order_contributions contributions
          USING stale
          WHERE contributions.order_id = stale.order_id
          RETURNING
            contributions.order_id,
            contributions.order_type,
            contributions.completed_at,
            contributions.volume_usd_micro
        )
        SELECT order_id, order_type, completed_at, volume_usd_micro
        FROM deleted
        `,
        [snapshotStartedAt, SNAPSHOT_PRUNE_BATCH_SIZE]
      )
      const affectedBuckets = new Map<string, VolumeBucketKey>()
      for (const contribution of deleted.rows) {
        addAffectedBuckets(affectedBuckets, contribution)
      }
      if (deleted.rows.length > 0) {
        await client.query(
          `
          UPDATE admin_volume_order_ingestion_state
          SET
            source_updated_at = GREATEST(source_updated_at, $2::timestamptz),
            contributes = false,
            updated_at = now()
          WHERE order_id = ANY($1::uuid[])
          `,
          [deleted.rows.map((row) => row.order_id), snapshotStartedAt]
        )
      }
      await this.recomputeBuckets(client, affectedBuckets)
      await client.query('COMMIT')
      return deleted.rows.length
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined)
      throw error
    } finally {
      client.release()
    }
  }

  private async pruneSnapshotStaleStatuses(snapshotStartedAt: Date) {
    for (;;) {
      const pruned = await this.pruneSnapshotStaleStatusBatch(snapshotStartedAt)
      if (pruned < SNAPSHOT_PRUNE_BATCH_SIZE) return
    }
  }

  private async pruneSnapshotStaleStatusBatch(snapshotStartedAt: Date) {
    await this.ready
    const client = await this.pool.connect()
    try {
      await client.query('BEGIN')
      const stale = await client.query<{ order_id: string; status: string }>(
        `
        WITH stale AS (
          SELECT order_id, status
          FROM admin_order_status_ingestion_state
          WHERE present = true
            AND updated_at < $1
            AND status IS NOT NULL
          ORDER BY updated_at ASC, order_id ASC
          LIMIT $2
          FOR UPDATE SKIP LOCKED
        )
        SELECT order_id, status
        FROM stale
        `,
        [snapshotStartedAt, SNAPSHOT_PRUNE_BATCH_SIZE]
      )
      for (const row of stale.rows) {
        await this.applyStatusDelta(client, row.status, -1)
      }
      if (stale.rows.length > 0) {
        await client.query(
          `
          UPDATE admin_order_status_ingestion_state
          SET
            source_updated_at = GREATEST(source_updated_at, $2::timestamptz),
            status = null,
            present = false,
            updated_at = now()
          WHERE order_id = ANY($1::uuid[])
          `,
          [stale.rows.map((row) => row.order_id), snapshotStartedAt]
        )
      }
      await client.query('COMMIT')
      return stale.rows.length
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined)
      throw error
    } finally {
      client.release()
    }
  }

  private async readDatabaseTimestamp() {
    await this.ready
    const result = await this.pool.query<{ timestamp: Date }>(
      'SELECT clock_timestamp() AS timestamp'
    )
    return result.rows[0].timestamp
  }

  private async recomputeBuckets(
    client: PoolClient,
    buckets: Map<string, VolumeBucketKey>
  ) {
    if (buckets.size === 0) return

    const affectedBuckets = [...buckets.values()].map((bucket) => ({
      bucket_size: bucket.bucketSize,
      bucket_start: bucket.bucketStart.toISOString(),
      bucket_end: bucketEnd(bucket.bucketStart, bucket.bucketSize).toISOString(),
      order_type: bucket.orderType
    }))

    await client.query(
      `
      WITH affected AS (
        SELECT
          bucket_size,
          bucket_start,
          bucket_end,
          order_type
        FROM jsonb_to_recordset($1::jsonb) AS affected(
          bucket_size text,
          bucket_start timestamptz,
          bucket_end timestamptz,
          order_type text
        )
      ),
      aggregated AS (
        SELECT
          affected.bucket_size,
          affected.bucket_start,
          affected.order_type,
          COALESCE(SUM(contributions.volume_usd_micro), 0) AS volume_usd_micro,
          COUNT(contributions.order_id)::bigint AS order_count
        FROM affected
        LEFT JOIN admin_volume_order_contributions contributions
          ON contributions.order_type = affected.order_type
         AND contributions.completed_at >= affected.bucket_start
         AND contributions.completed_at < affected.bucket_end
        GROUP BY
          affected.bucket_size,
          affected.bucket_start,
          affected.order_type
      ),
      upserted AS (
        INSERT INTO admin_volume_buckets (
          bucket_size,
          bucket_start,
          order_type,
          volume_usd_micro,
          order_count,
          updated_at
        )
        SELECT
          bucket_size,
          bucket_start,
          order_type,
          volume_usd_micro,
          order_count,
          now()
        FROM aggregated
        WHERE order_count > 0
        ON CONFLICT (bucket_size, bucket_start, order_type) DO UPDATE
        SET
          volume_usd_micro = EXCLUDED.volume_usd_micro,
          order_count = EXCLUDED.order_count,
          updated_at = now()
        RETURNING bucket_size, bucket_start, order_type
      )
      DELETE FROM admin_volume_buckets existing
      USING aggregated
      WHERE aggregated.order_count = 0
        AND existing.bucket_size = aggregated.bucket_size
        AND existing.bucket_start = aggregated.bucket_start
        AND existing.order_type = aggregated.order_type
      `,
      [JSON.stringify(affectedBuckets)]
    )
  }

  private async rebuildBucketsFromContributions(client: PoolClient) {
    await client.query('TRUNCATE admin_volume_buckets')
    await client.query(`
      INSERT INTO admin_volume_buckets (
        bucket_size,
        bucket_start,
        order_type,
        volume_usd_micro,
        order_count,
        updated_at
      )
      SELECT
        buckets.bucket_size,
        CASE buckets.bucket_size
          WHEN 'minute' THEN date_trunc('minute', contributions.completed_at)
          WHEN 'hour' THEN date_trunc('hour', contributions.completed_at)
          ELSE date_trunc('day', contributions.completed_at)
        END AS bucket_start,
        contributions.order_type,
        SUM(contributions.volume_usd_micro) AS volume_usd_micro,
        COUNT(*)::bigint AS order_count,
        now() AS updated_at
      FROM admin_volume_order_contributions contributions
      CROSS JOIN (
        VALUES ('minute'), ('hour'), ('day')
      ) AS buckets(bucket_size)
      GROUP BY 1, 2, 3
    `)
  }

  private async rebuildStatusCounts(client: PoolClient) {
    await client.query('TRUNCATE admin_order_status_counts')
    await client.query(`
      INSERT INTO admin_order_status_counts (
        status,
        order_count,
        updated_at
      )
      SELECT
        status,
        COUNT(*)::bigint,
        now()
      FROM admin_order_status_ingestion_state
      WHERE present = true
        AND status IS NOT NULL
      GROUP BY status
    `)
  }
}

function contributionMatches(
  stored: StoredContribution,
  contribution: VolumeContribution | undefined
) {
  return (
    contribution !== undefined &&
    stored.order_type === contribution.orderType &&
    stored.completed_at.getTime() === contribution.completedAt.getTime() &&
    stored.volume_usd_micro === contribution.volumeUsdMicro.toString()
  )
}

function previousIngestionStateDominates(
  previousState: StoredIngestionState,
  sourceUpdatedAt: Date,
  contributes: boolean
) {
  const previousTime = previousState.source_updated_at.getTime()
  const incomingTime = sourceUpdatedAt.getTime()
  if (previousTime > incomingTime) return true
  if (previousTime < incomingTime) return false

  return previousState.contributes === false && contributes
}

function previousStatusStateDominates(
  previousState: StoredStatusState,
  sourceUpdatedAt: Date,
  incomingPresent: boolean
) {
  const previousTime = previousState.source_updated_at.getTime()
  const incomingTime = sourceUpdatedAt.getTime()
  if (previousTime > incomingTime) return true
  if (previousTime < incomingTime) return false

  return previousState.present === false && incomingPresent
}

function addAffectedBuckets(
  buckets: Map<string, VolumeBucketKey>,
  contribution: StoredContribution | VolumeContribution
) {
  const orderType =
    'order_type' in contribution ? contribution.order_type : contribution.orderType
  const completedAt =
    'completed_at' in contribution
      ? contribution.completed_at
      : contribution.completedAt

  for (const bucketSize of BUCKET_SIZES) {
    const start = bucketStart(completedAt, bucketSize)
    buckets.set(`${bucketSize}:${orderType}:${start.toISOString()}`, {
      bucketSize,
      bucketStart: start,
      orderType
    })
  }
}

export function createVolumeAnalyticsRuntime(
  config: AdminDashboardConfig
): VolumeAnalyticsRuntime | null {
  return config.analyticsDatabaseUrl
    ? new VolumeAnalyticsRuntime(config.analyticsDatabaseUrl)
    : null
}

export function completedOrderContribution(
  order: OrderAnalyticsRow
): VolumeContribution | undefined {
  if (order.status !== 'completed') return undefined
  if (order.orderType !== 'market_order' && order.orderType !== 'limit_order') {
    return undefined
  }

  const completedLegs = order.executionLegs
    .filter((leg) => leg.status === 'completed')
    .sort(
      (left, right) =>
        left.legIndex - right.legIndex ||
        left.createdAt.localeCompare(right.createdAt) ||
        left.id.localeCompare(right.id)
    )
  const firstLeg = completedLegs[0]
  if (!firstLeg) return undefined

  if (!isPositiveRawAmount(firstLeg.actualAmountIn)) return undefined
  const inputAmount = firstLeg.actualAmountIn
  const valuation = valuationForAmount(inputAmount, firstLeg.input, firstLeg)
  if (valuation === undefined) return undefined

  return {
    orderId: order.id,
    orderType: order.orderType,
    completedAt: orderCompletedAt(order, completedLegs),
    volumeUsdMicro: valuation
  }
}

function orderSourceUpdatedAt(order: OrderAnalyticsRow) {
  return parseSourceUpdatedAt(order.id, order.updatedAt)
}

function parseSourceUpdatedAt(orderId: string, value: string) {
  const updatedAt = new Date(value)
  if (Number.isNaN(updatedAt.getTime())) {
    throw new Error(`order ${orderId} has invalid source updatedAt ${value}`)
  }
  return updatedAt
}

async function readClientTimestamp(client: PoolClient) {
  const result = await client.query<{ timestamp: Date }>(
    'SELECT clock_timestamp() AS timestamp'
  )
  return result.rows[0].timestamp
}

export function isPositiveRawAmount(value: string | undefined): value is string {
  const parsed = parseU256Decimal(value)
  return parsed !== undefined && parsed > 0n
}

function orderCompletedAt(
  order: OrderAnalyticsRow,
  completedLegs: OrderExecutionLeg[]
) {
  const completedAt = completedLegs
    .map((leg) => leg.completedAt)
    .filter((value): value is string => Boolean(value))
    .sort()
    .at(-1)
  return new Date(completedAt ?? order.updatedAt)
}

function valuationForAmount(
  rawAmount: string,
  asset: AssetRef,
  leg: OrderExecutionLeg
): bigint | undefined {
  const valuation = asRecord(leg.usdValuation)
  const amounts = asRecord(valuation?.amounts)
  if (!amounts) return undefined

  const priced = asRecord(amounts.actualInput) ?? asRecord(amounts.plannedInput)
  if (!priced || !sameAsset(priced.asset, asset)) return undefined
  const exact = stringField(priced, 'raw') === rawAmount ? priced : undefined

  const exactUsd = stringField(exact, 'amountUsdMicro')
  if (exactUsd !== undefined) {
    return parseU256Decimal(exactUsd)
  }

  const unitUsdMicro = parseU64Decimal(stringField(priced, 'unitUsdMicro'))
  const parsedRawAmount = parseU256Decimal(rawAmount)
  const decimals = tokenDecimalsField(priced, 'decimals')
  if (
    unitUsdMicro === undefined ||
    parsedRawAmount === undefined ||
    decimals === undefined
  ) {
    return undefined
  }
  return (parsedRawAmount * unitUsdMicro) / 10n ** BigInt(decimals)
}

function bucketStart(value: Date, bucketSize: VolumeBucketSize) {
  const bucket = new Date(value)
  bucket.setUTCSeconds(0, 0)
  if (bucketSize === 'hour' || bucketSize === 'day') bucket.setUTCMinutes(0)
  if (bucketSize === 'day') bucket.setUTCHours(0)
  return bucket
}

function bucketEnd(value: Date, bucketSize: VolumeBucketSize) {
  const end = new Date(value)
  if (bucketSize === 'five_minute') end.setUTCMinutes(end.getUTCMinutes() + 5)
  if (bucketSize === 'minute') end.setUTCMinutes(end.getUTCMinutes() + 1)
  if (bucketSize === 'hour') end.setUTCHours(end.getUTCHours() + 1)
  if (bucketSize === 'day') end.setUTCDate(end.getUTCDate() + 1)
  return end
}

function backfillCursorFromState(
  state: BackfillStateRow | undefined
): CompletedOrderAnalyticsCursor | undefined {
  if (!state?.cursor_created_at || !state.cursor_order_id) return undefined
  return {
    createdAt: state.cursor_created_at.toISOString(),
    id: state.cursor_order_id
  }
}

function backfillUpperBoundFromState(
  state: BackfillStateRow | undefined
): CompletedOrderAnalyticsBound | undefined {
  if (!state?.snapshot_upper_created_at || !state.snapshot_upper_order_id) {
    return undefined
  }
  return {
    createdAt: state.snapshot_upper_created_at.toISOString(),
    id: state.snapshot_upper_order_id
  }
}

function mergeBackfillRequests(
  pending: PendingBackfill | undefined,
  next: PendingBackfill
): PendingBackfill {
  if (!pending) return next
  return {
    replicaPool: next.replicaPool,
    options: {
      fullSnapshot:
        backfillRequestIsFullSnapshot(pending.options) ||
        backfillRequestIsFullSnapshot(next.options)
    }
  }
}

function backfillRequestIsFullSnapshot(options: BackfillOptions) {
  return options.fullSnapshot ?? true
}

export function addSafeMetricCount(
  left: number,
  right: number,
  field: string
): number {
  const value = left + right
  if (!Number.isSafeInteger(value)) {
    throw new Error(`${field} exceeds JavaScript safe integer range`)
  }
  return value
}

type JsonRecord = Record<string, unknown>

function asRecord(value: unknown): JsonRecord | undefined {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? (value as JsonRecord)
    : undefined
}

function stringField(record: JsonRecord | undefined, key: string) {
  const value = record?.[key]
  return typeof value === 'string' ? value : undefined
}

function tokenDecimalsField(record: JsonRecord | undefined, key: string) {
  const value = record?.[key]
  return typeof value === 'number' && Number.isInteger(value) && value >= 0 && value <= 36
    ? value
    : undefined
}

function sameAsset(value: unknown, asset: AssetRef) {
  const candidate = asRecord(value)
  return (
    stringField(candidate, 'chainId')?.toLowerCase() === asset.chainId.toLowerCase() &&
    stringField(candidate, 'assetId')?.toLowerCase() === asset.assetId.toLowerCase()
  )
}
