import { createHash, randomUUID } from 'node:crypto'
import { getAddress, isAddress, verifyTypedData, type Hex } from 'viem'

import {
  GatewayAuthenticationError,
  GatewayConflictError,
  GatewayValidationError
} from '../errors'
import {
  generateRefundToken,
  hashCancellationSecret,
  hashRefundToken,
  RouterCancellationSecretBox,
  verifyRefundToken
} from './crypto'
import type { RefundAuthorizationStore, RefundMode } from './store'
import type { StoredRefundAuthorization } from './store'

export const REFUND_EIP712_DOMAIN_NAME = 'TEE Router Gateway'
export const REFUND_EIP712_DOMAIN_VERSION = '2'
export const DEFAULT_REFUND_SIGNATURE_GATEWAY = 'http://localhost:3000'

export function refundEip712Domain(gatewayBaseUrl = DEFAULT_REFUND_SIGNATURE_GATEWAY) {
  return {
    name: REFUND_EIP712_DOMAIN_NAME,
    version: REFUND_EIP712_DOMAIN_VERSION,
    salt: gatewayDomainSalt(gatewayBaseUrl)
  } as const
}

export const REFUND_EIP712_DOMAIN = refundEip712Domain()

export const REFUND_EIP712_TYPES = {
  CancelOrder: [
    { name: 'orderId', type: 'string' },
    { name: 'gateway', type: 'string' },
    { name: 'deadline', type: 'uint256' }
  ]
} as const

export type CreateRefundAuthorizationInput = {
  routerOrderId: string
  cancellationSecret: string
  refundMode: RefundMode
  refundAuthorizer: string | null
}

export type CreateRefundAuthorizationResult = {
  refundMode: RefundMode
  refundAuthorizer: string | null
  refundToken?: string
}

export type ResolveEvmSignatureInput = {
  routerOrderId: string
  refundSignature: string
  refundSignatureDeadline: number
}

export type ResolvedCancellationAuthorization = {
  cancellationSecret: string
  claimId: string
}

export const DEFAULT_CANCELLATION_CLAIM_TIMEOUT_MS = 5 * 60 * 1000
export const MAX_REFUND_SIGNATURE_DEADLINE_SECONDS = 10 * 60

export type RefundAuthorizationServiceOptions = {
  cancellationClaimTimeoutMs?: number
  publicBaseUrl?: string
}

export class RefundAuthorizationService {
  private readonly cancellationClaimTimeoutMs: number
  private readonly publicBaseUrl: string

  constructor(
    private readonly store: RefundAuthorizationStore,
    private readonly secretBox: RouterCancellationSecretBox,
    options: RefundAuthorizationServiceOptions = {}
  ) {
    this.cancellationClaimTimeoutMs = normalizeCancellationClaimTimeout(
      options.cancellationClaimTimeoutMs
    )
    this.publicBaseUrl = normalizeGatewayBaseUrl(
      options.publicBaseUrl ?? DEFAULT_REFUND_SIGNATURE_GATEWAY
    )
  }

  async createRefundAuthorization(
    input: CreateRefundAuthorizationInput
  ): Promise<CreateRefundAuthorizationResult> {
    if (input.refundMode === 'token') {
      if (input.refundAuthorizer !== null) {
        throw new GatewayValidationError(
          'refundAuthorizer must be null when refundMode is token'
        )
      }

      const refundToken = generateRefundToken()
      const authorization = await this.saveRefundAuthorizationOrThrow({
        routerOrderId: input.routerOrderId,
        refundMode: 'token',
        refundAuthorizer: null,
        encryptedCancellationSecret: this.secretBox.encryptForOrder(
          input.routerOrderId,
          input.cancellationSecret
        ),
        cancellationSecretHash: hashCancellationSecret(
          input.routerOrderId,
          input.cancellationSecret
        ),
        refundTokenHash: hashRefundToken(refundToken),
        encryptedRefundToken: this.secretBox.encryptRefundTokenForOrder(
          input.routerOrderId,
          refundToken
        )
      })

      return {
        refundMode: 'token',
        refundAuthorizer: null,
        refundToken: decryptStoredRefundToken(
          this.secretBox,
          input.routerOrderId,
          authorization
        )
      }
    }

    const refundAuthorizer = normalizeEvmAddress(input.refundAuthorizer)
    await this.saveRefundAuthorizationOrThrow({
      routerOrderId: input.routerOrderId,
      refundMode: 'evmSignature',
      refundAuthorizer,
      encryptedCancellationSecret: this.secretBox.encryptForOrder(
        input.routerOrderId,
        input.cancellationSecret
      ),
      cancellationSecretHash: hashCancellationSecret(
        input.routerOrderId,
        input.cancellationSecret
      )
    })

    return {
      refundMode: 'evmSignature',
      refundAuthorizer
    }
  }

  async resolveTokenCancellationSecret(
    routerOrderId: string,
    refundToken: string
  ): Promise<ResolvedCancellationAuthorization> {
    const record = await this.store.findRefundAuthorization(routerOrderId)
    if (!record) {
      throw new GatewayValidationError(
        'order does not have refund authorization configured'
      )
    }

    if (record.refundMode !== 'token' || !record.refundTokenHash) {
      throw new GatewayAuthenticationError('order does not use token refund mode')
    }
    ensureCancellationNotRequested(record.cancellationRequestedAt)

    if (!verifyRefundToken(refundToken, record.refundTokenHash)) {
      throw new GatewayAuthenticationError('invalid refund token')
    }

    const cancellationSecret = this.secretBox.decryptForOrder(
      record.routerOrderId,
      record.encryptedCancellationSecret
    )
    const claimId = await this.claimCancellationAttempt(record)
    return {
      cancellationSecret,
      claimId
    }
  }

  async resolveEvmSignatureCancellationSecret(
    input: ResolveEvmSignatureInput
  ): Promise<ResolvedCancellationAuthorization> {
    const record = await this.store.findRefundAuthorization(input.routerOrderId)
    if (!record) {
      throw new GatewayValidationError(
        'order does not have refund authorization configured'
      )
    }

    if (record.refundMode !== 'evmSignature' || !record.refundAuthorizer) {
      throw new GatewayAuthenticationError(
        'order does not use evmSignature refund mode'
      )
    }
    ensureCancellationNotRequested(record.cancellationRequestedAt)

    validateDeadline(input.refundSignatureDeadline)

    const verified = await verifyTypedData({
      address: record.refundAuthorizer as Hex,
      domain: refundEip712Domain(this.publicBaseUrl),
      types: REFUND_EIP712_TYPES,
      primaryType: 'CancelOrder',
      message: {
        orderId: input.routerOrderId,
        gateway: this.publicBaseUrl,
        deadline: BigInt(input.refundSignatureDeadline)
      },
      signature: input.refundSignature as Hex
    })

    if (!verified) {
      throw new GatewayAuthenticationError('invalid refund signature')
    }

    const cancellationSecret = this.secretBox.decryptForOrder(
      record.routerOrderId,
      record.encryptedCancellationSecret
    )
    const claimId = await this.claimCancellationAttempt(record)
    return {
      cancellationSecret,
      claimId
    }
  }

  async markCancellationForwarded(
    routerOrderId: string,
    claimId: string
  ): Promise<void> {
    const marked = await this.store.markCancellationRequested(routerOrderId, claimId)
    if (!marked) {
      throw new Error('refund cancellation claim was no longer active')
    }
  }

  async releaseCancellationAttempt(
    routerOrderId: string,
    claimId: string
  ): Promise<void> {
    await this.store.releaseCancellationAttempt(routerOrderId, claimId)
  }

  private async claimCancellationAttempt(
    record: StoredRefundAuthorization
  ): Promise<string> {
    const claimId = randomUUID()
    const claimed = await this.store.claimCancellationAttempt(
      record.routerOrderId,
      claimId,
      this.cancellationClaimTimeoutMs,
      {
        routerOrderId: record.routerOrderId,
        refundMode: record.refundMode,
        refundAuthorizer: record.refundAuthorizer,
        encryptedCancellationSecret: record.encryptedCancellationSecret,
        cancellationSecretHash: record.cancellationSecretHash,
        ...(record.refundTokenHash
          ? { refundTokenHash: record.refundTokenHash }
          : {}),
        ...(record.encryptedRefundToken
          ? { encryptedRefundToken: record.encryptedRefundToken }
          : {})
      }
    )
    if (!claimed) {
      throw new GatewayAuthenticationError(
        'refund authorization already used or in progress'
      )
    }
    return claimId
  }

  private async saveRefundAuthorizationOrThrow(
    input: Parameters<RefundAuthorizationStore['saveRefundAuthorization']>[0]
  ): Promise<StoredRefundAuthorization> {
    const saved = await this.store.saveRefundAuthorization(
      input,
      this.cancellationClaimTimeoutMs
    )
    if (!saved) {
      throw new GatewayConflictError(
        'refund authorization already used or in progress'
      )
    }
    return saved
  }
}

function decryptStoredRefundToken(
  secretBox: RouterCancellationSecretBox,
  routerOrderId: string,
  authorization: StoredRefundAuthorization
): string {
  if (authorization.refundMode !== 'token' || !authorization.encryptedRefundToken) {
    throw new Error('stored token refund authorization is missing encrypted token')
  }
  return secretBox.decryptRefundTokenForOrder(
    routerOrderId,
    authorization.encryptedRefundToken
  )
}

function normalizeGatewayBaseUrl(value: string): string {
  const trimmed = value.trim()
  if (!trimmed) {
    throw new GatewayValidationError('refund signature gateway URL is required')
  }
  try {
    const url = new URL(trimmed)
    if (url.protocol !== 'http:' && url.protocol !== 'https:') {
      throw new Error('expected http or https URL')
    }
    if (url.username || url.password) {
      throw new Error('URL credentials are not allowed')
    }
    if (url.search || url.hash) {
      throw new Error('URL query strings and fragments are not allowed')
    }
    return url.toString().replace(/\/+$/, '')
  } catch {
    throw new GatewayValidationError(
      'refund signature gateway URL must be an absolute HTTP(S) URL without credentials, query string, or fragment'
    )
  }
}

function gatewayDomainSalt(gatewayBaseUrl: string): Hex {
  const normalizedGatewayBaseUrl = normalizeGatewayBaseUrl(gatewayBaseUrl)
  const digest = createHash('sha256')
    .update(`rift-router-gateway-cancel:${normalizedGatewayBaseUrl}`)
    .digest('hex')
  return `0x${digest}` as Hex
}

function normalizeCancellationClaimTimeout(value: number | undefined): number {
  if (value === undefined) return DEFAULT_CANCELLATION_CLAIM_TIMEOUT_MS
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new GatewayValidationError(
      'cancellationClaimTimeoutMs must be a positive safe integer'
    )
  }
  return value
}

function normalizeEvmAddress(value: string | null): string {
  if (!value) {
    throw new GatewayValidationError(
      'refundAuthorizer is required when refundMode is evmSignature'
    )
  }

  if (!isAddress(value)) {
    throw new GatewayValidationError(
      'refundAuthorizer must be an EVM address when refundMode is evmSignature'
    )
  }

  return getAddress(value)
}

function validateDeadline(deadline: number): void {
  const now = Math.floor(Date.now() / 1000)
  if (deadline < now) {
    throw new GatewayAuthenticationError('refund signature expired')
  }

  if (deadline > now + MAX_REFUND_SIGNATURE_DEADLINE_SECONDS) {
    throw new GatewayAuthenticationError(
      `refund signature deadline must be within ${MAX_REFUND_SIGNATURE_DEADLINE_SECONDS} seconds`
    )
  }
}

function ensureCancellationNotRequested(cancellationRequestedAt?: string): void {
  if (cancellationRequestedAt) {
    throw new GatewayAuthenticationError('refund authorization already used')
  }
}
