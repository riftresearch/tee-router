import {
  createCipheriv,
  createDecipheriv,
  createHash,
  randomBytes,
  timingSafeEqual
} from 'node:crypto'

import { GatewayConfigurationError } from '../errors'

const ENCRYPTION_VERSION = 'v2'
const KEY_BYTES = 32
const IV_BYTES = 12
const TAG_BYTES = 16

export class RouterCancellationSecretBox {
  constructor(private readonly key: Buffer) {
    if (key.byteLength !== KEY_BYTES) {
      throw new GatewayConfigurationError(
        'ROUTER_GATEWAY_CANCELLATION_SECRET_KEY must decode to 32 bytes'
      )
    }
  }

  static fromKeyMaterial(value: string): RouterCancellationSecretBox {
    return new RouterCancellationSecretBox(decodeKeyMaterial(value))
  }

  encryptForOrder(routerOrderId: string, plaintext: string): string {
    return this.encryptWithAad(cancellationSecretAad(routerOrderId), plaintext)
  }

  decryptForOrder(routerOrderId: string, payload: string): string {
    return this.decryptWithAad(cancellationSecretAad(routerOrderId), payload)
  }

  encryptRefundTokenForOrder(routerOrderId: string, plaintext: string): string {
    return this.encryptWithAad(refundTokenAad(routerOrderId), plaintext)
  }

  decryptRefundTokenForOrder(routerOrderId: string, payload: string): string {
    return this.decryptWithAad(refundTokenAad(routerOrderId), payload)
  }

  private encryptWithAad(aad: Buffer, plaintext: string): string {
    const iv = randomBytes(IV_BYTES)
    const cipher = createCipheriv('aes-256-gcm', this.key, iv)
    cipher.setAAD(aad)
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

  private decryptWithAad(aad: Buffer, payload: string): string {
    const parts = payload.split(':')
    if (parts.length !== 4) {
      throw new Error('invalid encrypted cancellation secret payload')
    }
    const [version, ivRaw, tagRaw, ciphertextRaw] = parts
    if (version !== ENCRYPTION_VERSION || !ivRaw || !tagRaw || !ciphertextRaw) {
      throw new Error('invalid encrypted cancellation secret payload')
    }

    const iv = decodeBase64UrlSegment(ivRaw, IV_BYTES, 'iv')
    const tag = decodeBase64UrlSegment(tagRaw, TAG_BYTES, 'tag')
    const ciphertext = decodeBase64UrlSegment(ciphertextRaw, undefined, 'ciphertext')

    const decipher = createDecipheriv('aes-256-gcm', this.key, iv)
    decipher.setAAD(aad)
    decipher.setAuthTag(tag)

    try {
      return Buffer.concat([
        decipher.update(ciphertext),
        decipher.final()
      ]).toString('utf8')
    } catch {
      throw new Error('invalid encrypted cancellation secret authentication')
    }
  }
}

export function generateRefundToken(): string {
  return `rgt_${randomBytes(32).toString('base64url')}`
}

export function hashRefundToken(token: string): string {
  return createHash('sha256').update(token, 'utf8').digest('hex')
}

export function hashCancellationSecret(routerOrderId: string, secret: string): string {
  return createHash('sha256')
    .update(`rift-router-gateway:cancellation-secret:${routerOrderId}:`, 'utf8')
    .update(secret, 'utf8')
    .digest('hex')
}

export function verifyRefundToken(
  token: string,
  expectedHash: string
): boolean {
  const actual = Buffer.from(hashRefundToken(token), 'hex')
  const expected = Buffer.from(expectedHash, 'hex')
  if (actual.byteLength !== expected.byteLength) return false

  return timingSafeEqual(actual, expected)
}

function decodeKeyMaterial(value: string): Buffer {
  if (/^[a-f0-9]{64}$/i.test(value)) return Buffer.from(value, 'hex')
  if (!/^[A-Za-z0-9_-]+={0,2}$/.test(value)) {
    throw new GatewayConfigurationError(
      'ROUTER_GATEWAY_CANCELLATION_SECRET_KEY must be 32-byte hex or base64url'
    )
  }

  try {
    return Buffer.from(value, 'base64url')
  } catch {
    throw new GatewayConfigurationError(
      'ROUTER_GATEWAY_CANCELLATION_SECRET_KEY must be 32-byte hex or base64url'
    )
  }
}

function decodeBase64UrlSegment(
  value: string,
  expectedBytes: number | undefined,
  field: string
): Buffer {
  if (!/^[A-Za-z0-9_-]+$/.test(value)) {
    throw new Error(`invalid encrypted cancellation secret ${field}`)
  }
  const decoded = Buffer.from(value, 'base64url')
  if (expectedBytes !== undefined && decoded.byteLength !== expectedBytes) {
    throw new Error(`invalid encrypted cancellation secret ${field}`)
  }
  if (expectedBytes === undefined && decoded.byteLength === 0) {
    throw new Error(`invalid encrypted cancellation secret ${field}`)
  }
  return decoded
}

function cancellationSecretAad(routerOrderId: string): Buffer {
  return Buffer.from(
    `rift-router-gateway:cancellation-secret:${routerOrderId}`,
    'utf8'
  )
}

function refundTokenAad(routerOrderId: string): Buffer {
  return Buffer.from(`rift-router-gateway:refund-token:${routerOrderId}`, 'utf8')
}
