import { expect, test } from 'bun:test'

import { logError, sanitizeErrorForLog } from '../internal/logging'

test('sanitizeErrorForLog redacts URLs, bearer tokens, and secret assignments', () => {
  const error = new Error(
    'gateway failed database_url=postgres://gateway:gateway-secret@db.internal:5432/gateway?sslmode=require Authorization: Bearer router-api-key'
  )
  ;(error as Error & { code: string; cause: Error; request: RequestInit }).code =
    'ECONNRESET'
  ;(error as Error & { code: string; cause: Error; request: RequestInit }).cause =
    new Error(
      'upstream https://router.internal/private/path?api_key=secret-token token=cleanup-secret'
    )
  ;(error as Error & { code: string; cause: Error; request: RequestInit }).request =
    {
      headers: {
        authorization: 'Bearer should-not-be-logged'
      }
    }

  const serialized = JSON.stringify(sanitizeErrorForLog(error))

  expect(serialized).toContain('ECONNRESET')
  expect(serialized).toContain(
    'https://router.internal/<redacted-path>?<redacted-query>'
  )
  expect(serialized).not.toContain('gateway-secret')
  expect(serialized).not.toContain('router-api-key')
  expect(serialized).not.toContain('secret-token')
  expect(serialized).not.toContain('cleanup-secret')
  expect(serialized).not.toContain('should-not-be-logged')
  expect(serialized).not.toContain('/private/path')
})

test('logError writes sanitized summaries instead of raw error objects', () => {
  const originalConsoleError = console.error
  const calls: unknown[][] = []
  console.error = (...args: unknown[]) => {
    calls.push(args)
  }
  try {
    const error = new Error('failed with api_key=top-secret')
    ;(error as Error & { cancellationSecret: string }).cancellationSecret =
      'do-not-log-this-property'

    logError('test failure', error)
  } finally {
    console.error = originalConsoleError
  }

  expect(calls).toHaveLength(1)
  expect(calls[0]?.[0]).toBe('test failure')
  const serialized = JSON.stringify(calls[0]?.[1])
  expect(serialized).not.toContain('top-secret')
  expect(serialized).not.toContain('do-not-log-this-property')
})
