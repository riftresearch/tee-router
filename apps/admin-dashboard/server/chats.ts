import type { AdminDashboardConfig } from './config'

const MAX_RESPONSE_BYTES = 4 * 1024 * 1024
const MAX_MESSAGE_LENGTH = 8_000
const REQUEST_TIMEOUT_MS = 15_000

export type ChatMessageRole = 'user' | 'admin'

export type ChatMessage = {
  role: ChatMessageRole
  message: string
  ts: string
}

export type ChatThread = {
  id: string
  user_eth_address: string
  meta: unknown
  last_message_at: string
  created_at?: string
  updated_at?: string
  last_admin_msg_at?: string
  user_unread_count?: number
  has_unread?: boolean
  messages: ChatMessage[]
}

export type ChatProxyConfig = Pick<
  AdminDashboardConfig,
  'supabaseChatsUrl' | 'supabaseAnonKey' | 'supabaseChatAdminSecret'
>

export class ChatProxyError extends Error {
  readonly status: number
  readonly upstreamStatus?: number
  readonly upstreamBody?: string

  constructor(
    status: number,
    message: string,
    upstream?: { status: number; body?: string }
  ) {
    super(message)
    this.name = 'ChatProxyError'
    this.status = status
    this.upstreamStatus = upstream?.status
    this.upstreamBody = upstream?.body
  }
}

export function ensureChatAdminConfigured(config: ChatProxyConfig): void {
  if (!config.supabaseChatAdminSecret) {
    throw new ChatProxyError(503, 'chat_admin_not_configured')
  }
  if (!config.supabaseAnonKey) {
    throw new ChatProxyError(503, 'chat_anon_key_not_configured')
  }
}

export async function listAdminChats(
  config: ChatProxyConfig
): Promise<ChatThread[]> {
  ensureChatAdminConfigured(config)
  const payload = await requestUpstream(
    config,
    'GET',
    `${config.supabaseChatsUrl}/admin/list`
  )
  if (!Array.isArray(payload)) {
    throw new ChatProxyError(502, 'invalid_upstream_payload')
  }
  return payload.filter(isChatThread)
}

export async function getAdminThread(
  config: ChatProxyConfig,
  chatId: string
): Promise<ChatThread> {
  ensureChatAdminConfigured(config)
  if (!isUuid(chatId)) {
    throw new ChatProxyError(400, 'invalid_chat_id')
  }
  const payload = await requestUpstream(
    config,
    'GET',
    `${config.supabaseChatsUrl}/admin/thread/${chatId}`
  )
  if (!isChatThread(payload)) {
    throw new ChatProxyError(502, 'invalid_upstream_payload')
  }
  return payload
}

export async function appendAdminMessage(
  config: ChatProxyConfig,
  chatId: string,
  userAddress: string,
  message: string
): Promise<unknown> {
  ensureChatAdminConfigured(config)
  if (!isUuid(chatId)) {
    throw new ChatProxyError(400, 'invalid_chat_id')
  }
  const normalizedAddress = normalizeAddress(userAddress)
  if (!normalizedAddress) {
    throw new ChatProxyError(400, 'invalid_user_address')
  }
  const trimmed = message.trim()
  if (!trimmed) {
    throw new ChatProxyError(400, 'empty_message')
  }
  if (trimmed.length > MAX_MESSAGE_LENGTH) {
    throw new ChatProxyError(400, 'message_too_long')
  }

  return requestUpstream(config, 'POST', `${config.supabaseChatsUrl}/append`, {
    chat_id: chatId,
    address: normalizedAddress,
    role: 'admin',
    message: trimmed
  })
}

async function requestUpstream(
  config: ChatProxyConfig,
  method: 'GET' | 'POST',
  url: string,
  body?: unknown
): Promise<unknown> {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS)
  let response: Response
  try {
    response = await fetch(url, {
      method,
      headers: {
        'content-type': 'application/json',
        apikey: config.supabaseAnonKey ?? '',
        authorization: `Bearer ${config.supabaseAnonKey ?? ''}`,
        'x-admin-secret': config.supabaseChatAdminSecret ?? ''
      },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: controller.signal
    })
  } catch (error) {
    if (error instanceof Error && error.name === 'AbortError') {
      throw new ChatProxyError(504, 'upstream_timeout')
    }
    throw new ChatProxyError(502, 'upstream_unreachable')
  } finally {
    clearTimeout(timer)
  }

  const text = await readLimitedText(response, MAX_RESPONSE_BYTES)
  if (!response.ok) {
    throw new ChatProxyError(
      mapUpstreamStatus(response.status),
      'upstream_error',
      { status: response.status, body: text.slice(0, 1024) }
    )
  }
  if (!text) return null
  try {
    return JSON.parse(text) as unknown
  } catch (_error) {
    throw new ChatProxyError(502, 'invalid_upstream_json', {
      status: response.status,
      body: text.slice(0, 1024)
    })
  }
}

async function readLimitedText(response: Response, maxBytes: number) {
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
      throw new ChatProxyError(502, 'upstream_response_too_large')
    }
    chunks.push(decoder.decode(result.value, { stream: true }))
  }
  chunks.push(decoder.decode())
  return chunks.join('')
}

function mapUpstreamStatus(status: number): number {
  if (status === 401 || status === 403) return 502
  if (status === 404) return 404
  if (status >= 500) return 502
  return 502
}

function isChatThread(value: unknown): value is ChatThread {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false
  const v = value as Record<string, unknown>
  if (typeof v.id !== 'string') return false
  if (typeof v.user_eth_address !== 'string') return false
  if (typeof v.last_message_at !== 'string') return false
  if (!Array.isArray(v.messages)) return false
  for (const msg of v.messages) {
    if (!isChatMessage(msg)) return false
  }
  return true
}

function isChatMessage(value: unknown): value is ChatMessage {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false
  const v = value as Record<string, unknown>
  return (
    (v.role === 'user' || v.role === 'admin') &&
    typeof v.message === 'string' &&
    typeof v.ts === 'string'
  )
}

function normalizeAddress(value: string): string | null {
  const trimmed = value.trim().toLowerCase()
  if (!/^0x[0-9a-f]{40}$/.test(trimmed)) return null
  return trimmed
}

function isUuid(value: string): boolean {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
    value
  )
}
