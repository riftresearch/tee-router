type SafeErrorSummary = {
  name: string
  message: string
  code?: string
  cause?: unknown
}

const MAX_LOG_FIELD_LENGTH = 512
const MAX_CAUSE_DEPTH = 2

const URL_PATTERN = /\b[a-z][a-z0-9+.-]*:\/\/[^\s"'<>),]+/giu
const BEARER_PATTERN = /\bBearer\s+[A-Za-z0-9._~+/=-]+/giu
const SECRET_ASSIGNMENT_PATTERN =
  /\b(database[_-]?url|password|passwd|pwd|secret|api[_-]?key|token|authorization|private[_-]?key)\b\s*[:=]\s*[^\s,;'"{}]+/giu

export function logError(message: string, error: unknown) {
  console.error(message, sanitizeErrorForLog(error))
}

export function sanitizeErrorForLog(error: unknown, depth = 0): unknown {
  if (error instanceof Error) {
    const record = error as Error & { code?: unknown; cause?: unknown }
    const summary: SafeErrorSummary = {
      name: truncateForLog(error.name || 'Error'),
      message: redactLogString(error.message)
    }
    const code = safeLogString(record.code)
    if (code) summary.code = code
    if (record.cause !== undefined && depth < MAX_CAUSE_DEPTH) {
      summary.cause = sanitizeErrorForLog(record.cause, depth + 1)
    }
    return summary
  }

  if (typeof error === 'string') return redactLogString(error)
  if (typeof error === 'number' || typeof error === 'boolean' || error === null) {
    return error
  }
  if (isRecord(error)) {
    const summary: SafeErrorSummary = {
      name: safeLogString(error.name) ?? 'NonErrorObject',
      message: safeLogString(error.message) ?? 'non-error object redacted'
    }
    const code = safeLogString(error.code)
    if (code) summary.code = code
    if (error.cause !== undefined && depth < MAX_CAUSE_DEPTH) {
      summary.cause = sanitizeErrorForLog(error.cause, depth + 1)
    }
    return summary
  }
  return String(error)
}

function safeLogString(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined
  return redactLogString(value)
}

function redactLogString(value: string): string {
  return truncateForLog(
    value
      .replace(URL_PATTERN, (url) => sanitizeUrlForLog(url))
      .replace(BEARER_PATTERN, 'Bearer <redacted>')
      .replace(SECRET_ASSIGNMENT_PATTERN, (_match, key: string) => {
        return `${key}=<redacted>`
      })
  )
}

function sanitizeUrlForLog(value: string): string {
  try {
    const url = new URL(value)
    if (!url.host) return '<redacted-url>'
    const path = url.pathname && url.pathname !== '/' ? '/<redacted-path>' : ''
    const query = url.search ? '?<redacted-query>' : ''
    const fragment = url.hash ? '#<redacted-fragment>' : ''
    return `${url.protocol}//${url.host}${path}${query}${fragment}`
  } catch (_error) {
    return '<redacted-url>'
  }
}

function truncateForLog(value: string): string {
  if (value.length <= MAX_LOG_FIELD_LENGTH) return value
  return `${value.slice(0, MAX_LOG_FIELD_LENGTH)}...<truncated>`
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value)
}
