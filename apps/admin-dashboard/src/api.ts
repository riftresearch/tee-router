import type {
  MeResponse,
  OrdersResponse,
  SnapshotEvent,
  UpsertEvent
} from './types'

export async function fetchMe(): Promise<MeResponse> {
  const response = await fetch('/api/me', {
    credentials: 'include'
  })

  if (!response.ok && response.status !== 401 && response.status !== 503) {
    throw new Error(`Failed to load session: HTTP ${response.status}`)
  }

  return (await response.json()) as MeResponse
}

export async function fetchOrders(limit = 200): Promise<OrdersResponse> {
  const response = await fetch(`/api/orders?limit=${limit}`, {
    credentials: 'include'
  })

  if (!response.ok) {
    throw new Error(`Failed to load orders: HTTP ${response.status}`)
  }

  return (await response.json()) as OrdersResponse
}

export function parseSnapshotEvent(event: MessageEvent<string>): SnapshotEvent {
  return JSON.parse(event.data) as SnapshotEvent
}

export function parseUpsertEvent(event: MessageEvent<string>): UpsertEvent {
  return JSON.parse(event.data) as UpsertEvent
}
