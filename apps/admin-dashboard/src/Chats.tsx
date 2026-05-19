import {
  Check,
  Copy,
  MessageSquare,
  RefreshCw,
  Send
} from 'lucide-react'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'

import { fetchChatList, fetchChatThread, sendChatReply } from './api'
import type { ChatThread } from './types'

const POLL_INTERVAL_MS = 10_000

type ChatsProps = {
  configured: boolean
}

export function ChatsView({ configured }: ChatsProps) {
  const [chats, setChats] = useState<ChatThread[]>([])
  const [loading, setLoading] = useState(true)
  const [listError, setListError] = useState<string | null>(null)
  const [selectedChatId, setSelectedChatId] = useState<string | null>(null)
  const [selectedThread, setSelectedThread] = useState<ChatThread | null>(null)
  const [threadLoading, setThreadLoading] = useState(false)
  const [threadError, setThreadError] = useState<string | null>(null)
  const [reply, setReply] = useState('')
  const [sending, setSending] = useState(false)
  const [sendError, setSendError] = useState<string | null>(null)
  const [copiedAddress, setCopiedAddress] = useState<string | null>(null)
  const [refreshing, setRefreshing] = useState(false)
  const messagesRef = useRef<HTMLDivElement | null>(null)
  const shouldScrollRef = useRef(false)
  const lastSelectedIdRef = useRef<string | null>(null)

  const refreshList = useCallback(async () => {
    if (!configured) return
    try {
      const list = await fetchChatList()
      list.sort((a, b) => b.last_message_at.localeCompare(a.last_message_at))
      setChats(list)
      setListError(null)
    } catch (error) {
      setListError(messageFor(error))
    }
  }, [configured])

  const loadThread = useCallback(
    async (chatId: string, options?: { scroll?: boolean }) => {
      if (!configured) return
      setThreadLoading(true)
      try {
        const thread = await fetchChatThread(chatId)
        setSelectedThread(thread)
        setThreadError(null)
        if (options?.scroll) shouldScrollRef.current = true
      } catch (error) {
        setThreadError(messageFor(error))
      } finally {
        setThreadLoading(false)
      }
    },
    [configured]
  )

  useEffect(() => {
    if (!configured) {
      setLoading(false)
      return
    }
    setLoading(true)
    void refreshList().finally(() => setLoading(false))
  }, [configured, refreshList])

  useEffect(() => {
    if (!configured) return
    const handle = window.setInterval(() => {
      void refreshList()
      if (selectedChatId) void loadThread(selectedChatId)
    }, POLL_INTERVAL_MS)
    return () => window.clearInterval(handle)
  }, [configured, loadThread, refreshList, selectedChatId])

  useEffect(() => {
    if (!selectedChatId) {
      setSelectedThread(null)
      setReply('')
      setSendError(null)
      lastSelectedIdRef.current = null
      return
    }
    const opened = lastSelectedIdRef.current !== selectedChatId
    lastSelectedIdRef.current = selectedChatId
    void loadThread(selectedChatId, { scroll: opened })
    if (opened) {
      setReply('')
      setSendError(null)
    }
  }, [loadThread, selectedChatId])

  useEffect(() => {
    if (!shouldScrollRef.current) return
    const el = messagesRef.current
    if (!el) return
    el.scrollTop = el.scrollHeight
    shouldScrollRef.current = false
  }, [selectedThread])

  const onSendReply = useCallback(async () => {
    if (!selectedChatId || !selectedThread || sending) return
    const trimmed = reply.trim()
    if (!trimmed) return
    setSending(true)
    setSendError(null)
    try {
      await sendChatReply(
        selectedChatId,
        selectedThread.user_eth_address,
        trimmed
      )
      setReply('')
      shouldScrollRef.current = true
      await loadThread(selectedChatId, { scroll: true })
      void refreshList()
    } catch (error) {
      setSendError(messageFor(error))
    } finally {
      setSending(false)
    }
  }, [loadThread, refreshList, reply, selectedChatId, selectedThread, sending])

  const onCopyAddress = useCallback(async (address: string) => {
    try {
      await navigator.clipboard.writeText(address)
      setCopiedAddress(address)
      window.setTimeout(() => {
        setCopiedAddress((current) => (current === address ? null : current))
      }, 1500)
    } catch (_error) {
      // ignore
    }
  }, [])

  const onManualRefresh = useCallback(async () => {
    setRefreshing(true)
    try {
      await refreshList()
      if (selectedChatId) await loadThread(selectedChatId)
    } finally {
      setRefreshing(false)
    }
  }, [loadThread, refreshList, selectedChatId])

  const totalUnread = useMemo(
    () =>
      chats.reduce(
        (sum, chat) =>
          sum +
          (typeof chat.user_unread_count === 'number'
            ? Math.max(0, chat.user_unread_count)
            : 0),
        0
      ),
    [chats]
  )

  if (!configured) {
    return (
      <main className="dashboard">
        <section className="orders-surface chats-empty-surface">
          <div className="chats-empty">
            <MessageSquare size={32} />
            <h3>Chat admin not configured</h3>
            <p>
              Set <code className="mono">SUPABASE_CHAT_ADMIN_SECRET</code> and{' '}
              <code className="mono">SUPABASE_ANON_KEY</code> in the admin
              dashboard environment to enable the chat reader.
            </p>
          </div>
        </section>
      </main>
    )
  }

  return (
    <main className="dashboard">
      <section className="orders-surface chats-surface" aria-label="Chats">
        <div className="chats-toolbar">
          <div className="chats-toolbar-title">
            <MessageSquare size={16} />
            <h2>Feedback Chats</h2>
            <span className="chats-count">
              {chats.length} chat{chats.length === 1 ? '' : 's'}
              {totalUnread > 0 ? ` · ${totalUnread} awaiting reply` : ''}
            </span>
          </div>
          <button
            type="button"
            className="chats-icon-button"
            onClick={() => void onManualRefresh()}
            disabled={refreshing}
            aria-label="Refresh chats"
            title="Refresh"
          >
            <RefreshCw size={15} className={refreshing ? 'spin' : ''} />
          </button>
        </div>

        {listError ? <div className="notice error">{listError}</div> : null}

        <div className="chats-layout">
          <aside className="chats-sidebar" aria-label="Chat list">
            {loading ? (
              <div className="chats-empty subtle">
                <RefreshCw size={18} className="spin" />
                <span>Loading chats</span>
              </div>
            ) : chats.length === 0 ? (
              <div className="chats-empty subtle">
                <MessageSquare size={20} />
                <span>No chats yet</span>
              </div>
            ) : (
              <ul className="chats-list">
                {chats.map((chat) => {
                  const unread =
                    typeof chat.user_unread_count === 'number'
                      ? Math.max(0, chat.user_unread_count)
                      : 0
                  const last = chat.messages[chat.messages.length - 1]
                  const preview = last ? last.message : 'No messages yet'
                  const active = chat.id === selectedChatId
                  return (
                    <li key={chat.id}>
                      <button
                        type="button"
                        className={`chats-list-item${active ? ' active' : ''}`}
                        onClick={() => setSelectedChatId(chat.id)}
                      >
                        <div className="chats-list-row">
                          <span className="chats-list-address mono">
                            {shortAddress(chat.user_eth_address)}
                          </span>
                          <span className="chats-list-time mono">
                            {formatRelative(chat.last_message_at)}
                          </span>
                        </div>
                        <div className="chats-list-preview">
                          {last ? (
                            <span className="chats-list-role">
                              {last.role === 'admin' ? 'You: ' : ''}
                            </span>
                          ) : null}
                          <span>{preview}</span>
                        </div>
                        {unread > 0 ? (
                          <span
                            className="chats-list-badge"
                            aria-label={`${unread} awaiting reply`}
                          >
                            {unread > 9 ? '9+' : unread}
                          </span>
                        ) : null}
                      </button>
                    </li>
                  )
                })}
              </ul>
            )}
          </aside>

          <section className="chats-main" aria-label="Chat thread">
            {!selectedChatId ? (
              <div className="chats-empty subtle large">
                <MessageSquare size={26} />
                <span>Select a chat to view messages</span>
              </div>
            ) : (
              <>
                {selectedThread ? (
                  <header className="chats-thread-header">
                    <div className="chats-thread-title">
                      <span className="mono">
                        {shortAddress(selectedThread.user_eth_address, 6, 4)}
                      </span>
                      <button
                        type="button"
                        className="chats-icon-button small"
                        onClick={() =>
                          void onCopyAddress(selectedThread.user_eth_address)
                        }
                        aria-label="Copy visitor address"
                        title="Copy address"
                      >
                        {copiedAddress === selectedThread.user_eth_address ? (
                          <Check size={13} />
                        ) : (
                          <Copy size={13} />
                        )}
                      </button>
                    </div>
                    <div className="chats-thread-meta mono">
                      {selectedThread.messages.length} message
                      {selectedThread.messages.length === 1 ? '' : 's'} · last{' '}
                      {formatRelative(selectedThread.last_message_at)}
                    </div>
                  </header>
                ) : (
                  <header className="chats-thread-header">
                    <div className="chats-thread-title">Loading…</div>
                  </header>
                )}

                {threadError ? (
                  <div className="notice error chats-thread-error">
                    {threadError}
                  </div>
                ) : null}

                <div className="chats-messages" ref={messagesRef}>
                  {threadLoading && !selectedThread ? (
                    <div className="chats-empty subtle">
                      <RefreshCw size={18} className="spin" />
                      <span>Loading thread</span>
                    </div>
                  ) : selectedThread &&
                    selectedThread.messages.length === 0 ? (
                    <div className="chats-empty subtle">No messages yet</div>
                  ) : (
                    selectedThread?.messages.map((msg, idx) => (
                      <div
                        key={`${msg.ts}-${idx}`}
                        className={`chats-msg-row ${msg.role}`}
                      >
                        <div className={`chats-msg ${msg.role}`}>
                          <div className="chats-msg-label mono">
                            {msg.role === 'admin' ? 'YOU' : 'USER'}
                            <span className="chats-msg-time">
                              {' · '}
                              {formatTime(msg.ts)}
                            </span>
                          </div>
                          <div className="chats-msg-text">{msg.message}</div>
                        </div>
                      </div>
                    ))
                  )}
                </div>

                <form
                  className="chats-input-row"
                  onSubmit={(event) => {
                    event.preventDefault()
                    void onSendReply()
                  }}
                >
                  <input
                    type="text"
                    className="chats-input"
                    value={reply}
                    placeholder="Reply to visitor…"
                    onChange={(event) => setReply(event.target.value)}
                    onKeyDown={(event) => {
                      if (event.key === 'Enter' && !event.shiftKey) {
                        event.preventDefault()
                        void onSendReply()
                      }
                    }}
                    disabled={!selectedThread || sending}
                  />
                  <button
                    type="submit"
                    className="chats-send-button"
                    disabled={!selectedThread || sending || !reply.trim()}
                    aria-label="Send reply"
                  >
                    {sending ? (
                      <RefreshCw size={16} className="spin" />
                    ) : (
                      <Send size={16} />
                    )}
                  </button>
                </form>
                {sendError ? (
                  <div className="notice error chats-send-error">
                    {sendError}
                  </div>
                ) : null}
              </>
            )}
          </section>
        </div>
      </section>
    </main>
  )
}

function shortAddress(address: string, head = 6, tail = 4): string {
  if (address.length <= head + tail + 2) return address
  return `${address.slice(0, head)}…${address.slice(-tail)}`
}

function formatTime(value: string): string {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return date.toLocaleString(undefined, {
    hour: '2-digit',
    minute: '2-digit',
    month: 'short',
    day: 'numeric'
  })
}

function formatRelative(value: string): string {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  const diffMs = Date.now() - date.getTime()
  const sec = Math.round(diffMs / 1000)
  if (sec < 60) return 'just now'
  const min = Math.round(sec / 60)
  if (min < 60) return `${min}m ago`
  const hr = Math.round(min / 60)
  if (hr < 24) return `${hr}h ago`
  const day = Math.round(hr / 24)
  if (day < 14) return `${day}d ago`
  return date.toLocaleDateString()
}

function messageFor(error: unknown): string {
  if (error instanceof Error && error.message) return error.message
  return 'Request failed'
}
