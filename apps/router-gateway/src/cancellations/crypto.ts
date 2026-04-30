import {
  createCipheriv,
  createDecipheriv,
  createHash,
  randomBytes,
  timingSafeEqual
} from 'node:crypto'

import { GatewayConfigurationError } from '../errors'

const ENCRYPTION_VERSION = 'v1'
const KEY_BYTES = 32
const IV_BYTES = 12

export class CancellationSecretBox {
  constructor(private readonly key: Buffer) {
    if (key.byteLength !== KEY_BYTES) {
      throw new GatewayConfigurationError(
        'ROUTER_GATEWAY_CANCELLATION_SECRET_KEY must decode to 32 bytes'
      )
    }
  }

  static fromKeyMaterial(value: string): CancellationSecretBox {
    return new CancellationSecretBox(decodeKeyMaterial(value))
  }

  encrypt(plaintext: string): string {
    const iv = randomBytes(IV_BYTES)
    const cipher = createCipheriv('aes-256-gcm', this.key, iv)
    const ciphertext = Buffer.concat([
      cipher.update(plaintext, 'utf8'),
      cipher.final()
    ])
    const tag = cipher.getAuthTag()

    return [
      ENCRYPTION_VERSION,
      iv.toString('base64url'),
      tag.toString('base64url'),
      ciphertext.toString('base64url')
    ].join(':')
  }

  decrypt(payload: string): string {
    const [version, ivRaw, tagRaw, ciphertextRaw] = payload.split(':')
    if (version !== ENCRYPTION_VERSION || !ivRaw || !tagRaw || !ciphertextRaw) {
      throw new Error('invalid encrypted cancellation secret payload')
    }

    const decipher = createDecipheriv(
      'aes-256-gcm',
      this.key,
      Buffer.from(ivRaw, 'base64url')
    )
    decipher.setAuthTag(Buffer.from(tagRaw, 'base64url'))

    return Buffer.concat([
      decipher.update(Buffer.from(ciphertextRaw, 'base64url')),
      decipher.final()
    ]).toString('utf8')
  }
}

export function generateGatewayCancellationToken(): string {
  return `rgt_${randomBytes(32).toString('base64url')}`
}

export function hashGatewayCancellationToken(token: string): string {
  return createHash('sha256').update(token, 'utf8').digest('hex')
}

export function verifyGatewayCancellationToken(
  token: string,
  expectedHash: string
): boolean {
  const actual = Buffer.from(hashGatewayCancellationToken(token), 'hex')
  const expected = Buffer.from(expectedHash, 'hex')
  if (actual.byteLength !== expected.byteLength) return false

  return timingSafeEqual(actual, expected)
}

function decodeKeyMaterial(value: string): Buffer {
  if (/^[a-f0-9]{64}$/i.test(value)) return Buffer.from(value, 'hex')

  try {
    return Buffer.from(value, 'base64url')
  } catch {
    throw new GatewayConfigurationError(
      'ROUTER_GATEWAY_CANCELLATION_SECRET_KEY must be 32-byte hex or base64url'
    )
  }
}
