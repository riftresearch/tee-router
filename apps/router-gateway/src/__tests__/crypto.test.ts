import { expect, test } from 'bun:test'

import { RouterCancellationSecretBox } from '../cancellations/crypto'

test('RouterCancellationSecretBox rejects malformed encrypted payload framing', () => {
  const box = new RouterCancellationSecretBox(Buffer.alloc(32, 7))
  const encrypted = box.encryptForOrder(
    '00000000-0000-4000-8000-000000000002',
    '0xsecret'
  )

  expect(
    box.decryptForOrder('00000000-0000-4000-8000-000000000002', encrypted)
  ).toBe('0xsecret')
  expect(() =>
    box.decryptForOrder(
      '00000000-0000-4000-8000-000000000002',
      `${encrypted}:extra`
    )
  ).toThrow(
    'invalid encrypted cancellation secret payload'
  )
  expect(() =>
    box.decryptForOrder(
      '00000000-0000-4000-8000-000000000002',
      'v2:bad:bad:bad'
    )
  ).toThrow(
    'invalid encrypted cancellation secret iv'
  )
})

test('RouterCancellationSecretBox binds ciphertext authentication to router order id', () => {
  const box = new RouterCancellationSecretBox(Buffer.alloc(32, 7))
  const encrypted = box.encryptForOrder(
    '00000000-0000-4000-8000-000000000002',
    '0xsecret'
  )

  expect(() =>
    box.decryptForOrder('00000000-0000-4000-8000-000000000003', encrypted)
  ).toThrow('invalid encrypted cancellation secret authentication')
})

test('RouterCancellationSecretBox rejects malformed base64url key material', () => {
  expect(() =>
    RouterCancellationSecretBox.fromKeyMaterial(`${'A'.repeat(43)}!`)
  ).toThrow(
    'ROUTER_GATEWAY_CANCELLATION_SECRET_KEY must be 32-byte hex or base64url'
  )
})
