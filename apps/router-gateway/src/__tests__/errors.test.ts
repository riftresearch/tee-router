import { expect, test } from 'bun:test'

import {
  GatewayConflictError,
  GatewayValidationError,
  UpstreamHttpError,
  UpstreamInsufficientLiquidityError,
  UpstreamVenueUnavailableError,
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
test('normalizeError preserves actionable upstream 422 errors', () => {
  const normalized = normalizeError(
    new UpstreamHttpError(422, 'balance 0 below required 40125000000000', {
      error: { message: 'balance 0 below required 40125000000000' }
    })
  )

  expect(normalized.status).toBe(422)
  expect(normalized.body.error.code).toBe('UPSTREAM_ERROR')
  expect(normalized.body.error.message).toBe(
    'balance 0 below required 40125000000000'
  )
  expect(normalized.body.error.details).toEqual({ upstreamStatus: 422 })
})

test('normalizeError renders insufficient liquidity with its dedicated code', () => {
  const normalized = normalizeError(new UpstreamInsufficientLiquidityError())

  expect(normalized.status).toBe(422)
  expect(normalized.body).toEqual({
    error: {
      code: 'INSUFFICIENT_LIQUIDITY',
      message: 'insufficient liquidity for the requested size'
    }
  })
})

test('normalizeError renders venue-unavailable cleanly without leaking upstream detail', () => {
  const normalized = normalizeError(new UpstreamVenueUnavailableError())

  expect(normalized.status).toBe(503)
  expect(normalized.body).toEqual({
    error: {
      code: 'SERVICE_UNAVAILABLE',
      message: 'a venue is temporarily unavailable; please retry shortly'
    }
  })
})
