import { expect, test } from 'bun:test'

import {
  GatewayConflictError,
  GatewayValidationError,
  normalizeError
} from '../errors'

test('normalizeError hides unexpected internal error details', () => {
  const consoleError = console.error
  console.error = () => undefined
  try {
    const normalized = normalizeError(new Error('private database detail'))

    expect(normalized.status).toBe(500)
    expect(normalized.body.error.message).toBe('Unexpected gateway error')
    expect(JSON.stringify(normalized.body)).not.toContain('private database detail')
  } finally {
    console.error = consoleError
  }
})

test('normalizeError keeps validation errors actionable', () => {
  const normalized = normalizeError(
    new GatewayValidationError('invalid request shape', { field: 'from' })
  )

  expect(normalized.status).toBe(400)
  expect(normalized.body.error.message).toBe('invalid request shape')
  expect(normalized.body.error.details).toEqual({ field: 'from' })
})

test('normalizeError maps conflict errors without leaking details', () => {
  const normalized = normalizeError(
    new GatewayConflictError('refund authorization already used or in progress')
  )

  expect(normalized.status).toBe(409)
  expect(normalized.body.error.code).toBe('CONFLICT')
  expect(normalized.body.error.message).toBe(
    'refund authorization already used or in progress'
  )
})
