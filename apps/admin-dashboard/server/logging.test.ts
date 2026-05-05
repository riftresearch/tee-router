import { expect, test } from 'bun:test'

import { logError, sanitizeErrorForLog } from './logging'

test('sanitizeErrorForLog redacts URLs, bearer tokens, and secret assignments', () => {
  const error = new Error(
    'connect failed DATABASE_URL=postgres://admin:supersecret@db.internal:5432/router?sslmode=require Authorization: Bearer gateway-secret-token'
  )
  ;(error as Error & { code: string; cause: Error; connectionString: string }).code =
    '28P01'
  ;(error as Error & { code: string; cause: Error; connectionString: string }).cause =
    new Error(
      'replica postgres://reader:reader-pass@replica.internal:5432/router_replica?application_name=admin token=cdc-secret'
    )
  ;(
    error as Error & { code: string; cause: Error; connectionString: string }
  ).connectionString = 'postgres://raw:raw-pass@should-not-be-logged.local/db'

  const serialized = JSON.stringify(sanitizeErrorForLog(error))

  expect(serialized).toContain('28P01')
  expect(serialized).toContain('postgres://replica.internal:5432/<redacted-path>?<redacted-query>')
  expect(serialized).not.toContain('supersecret')
  expect(serialized).not.toContain('gateway-secret-token')
  expect(serialized).not.toContain('reader-pass')
  expect(serialized).not.toContain('cdc-secret')
  expect(serialized).not.toContain('raw-pass')
  expect(serialized).not.toContain('router_replica')
  expect(serialized).not.toContain('application_name')
})

test('logError writes sanitized summaries instead of raw error objects', () => {
  const originalConsoleError = console.error
  const calls: unknown[][] = []
  console.error = (...args: unknown[]) => {
    calls.push(args)
  }
  try {
    const error = new Error('failed with secret=top-secret')
    ;(error as Error & { privateValue: string }).privateValue =
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
