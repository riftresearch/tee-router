import {
  LogicalReplicationService,
  PgoutputPlugin
} from 'pg-logical-replication'
import type { Pool } from 'pg'

import type { AdminDashboardConfig } from './config'
import {
  fetchOrderById,
  fetchOrderMetrics,
  type OrderFirehoseRow,
  type OrderMetrics
} from './orders'

export type OrderCdcUpsert = {
  order: OrderFirehoseRow
  total?: number
  metrics?: OrderMetrics
  sort: 'created_at_desc'
}

type Listener = (event: OrderCdcUpsert) => void

type RouterCdcMessage = {
  version: number
  schema: string
  table: string
  op: string
  id?: string | null
  orderId?: string | null
  watchId?: string | null
  providerOperationId?: string | null
}

export class OrderCdcBroker {
  private readonly listeners = new Set<Listener>()
  private service?: LogicalReplicationService
  private stopped = false
  private loopStarted = false

  constructor(
    private readonly pool: Pool,
    private readonly config: AdminDashboardConfig
  ) {}

  start() {
    if (this.loopStarted || !this.config.replicaDatabaseUrl) return
    this.loopStarted = true
    void this.run()
  }

  subscribe(listener: Listener) {
    this.listeners.add(listener)
    return () => {
      this.listeners.delete(listener)
    }
  }

  async close() {
    this.stopped = true
    await this.service?.destroy()
  }

  private async run() {
    while (!this.stopped) {
      try {
        await ensureCdcSlot(this.pool, this.config.cdcSlotName)
        const service = new LogicalReplicationService(
          {
            connectionString: this.config.replicaDatabaseUrl,
            application_name: 'rift-admin-dashboard-cdc'
          },
          {
            acknowledge: { auto: true, timeoutSeconds: 1 },
            flowControl: { enabled: true }
          }
        )
        this.service = service

        service.on('data', async (_lsn: string, message: unknown) => {
          await this.handleReplicationMessage(message)
        })
        service.on('error', (error: Error) => {
          console.error('admin dashboard order CDC stream error', error)
        })

        const plugin = new PgoutputPlugin({
          protoVersion: 1,
          publicationNames: [this.config.cdcPublicationName],
          messages: true
        })
        await service.subscribe(plugin, this.config.cdcSlotName)
      } catch (error) {
        await this.service?.destroy().catch(() => undefined)
        this.service = undefined
        if (!this.stopped) {
          console.error('admin dashboard order CDC stream stopped', error)
          await sleep(1_000)
        }
      }
    }
  }

  private async handleReplicationMessage(message: unknown) {
    const change = parseRouterCdcMessage(message, this.config.cdcMessagePrefix)
    if (!change?.orderId) return

    const order = await fetchOrderById(this.pool, change.orderId)
    if (!order) return

    const metrics = await fetchOrderMetrics(this.pool)
    const event: OrderCdcUpsert = {
      order,
      total: metrics.total,
      metrics,
      sort: 'created_at_desc'
    }
    for (const listener of this.listeners) {
      listener(event)
    }
  }
}

async function ensureCdcSlot(pool: Pool, slotName: string) {
  const existing = await pool.query<{ plugin: string | null }>(
    `
    SELECT plugin
    FROM pg_catalog.pg_replication_slots
    WHERE slot_name = $1
    `,
    [slotName]
  )

  const plugin = existing.rows[0]?.plugin
  if (plugin) {
    if (plugin !== 'pgoutput') {
      throw new Error(
        `CDC slot ${slotName} uses plugin ${plugin}; expected pgoutput`
      )
    }
    return
  }

  await pool.query(
    `
    SELECT slot_name
    FROM pg_catalog.pg_create_logical_replication_slot($1::name, 'pgoutput')
    `,
    [slotName]
  )
}

function parseRouterCdcMessage(
  message: unknown,
  expectedPrefix: string
): RouterCdcMessage | undefined {
  if (!isRecord(message) || message.tag !== 'message') return undefined
  if (message.prefix !== expectedPrefix) return undefined
  if (!(message.content instanceof Uint8Array)) return undefined

  const payload = new TextDecoder().decode(message.content)
  const parsed = JSON.parse(payload) as unknown
  if (!isRecord(parsed)) return undefined

  return {
    version: Number(parsed.version),
    schema: String(parsed.schema),
    table: String(parsed.table),
    op: String(parsed.op),
    id: optionalString(parsed.id),
    orderId: optionalString(parsed.orderId),
    watchId: optionalString(parsed.watchId),
    providerOperationId: optionalString(parsed.providerOperationId)
  }
}

function optionalString(value: unknown): string | null {
  return typeof value === 'string' && value.length > 0 ? value : null
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value)
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
