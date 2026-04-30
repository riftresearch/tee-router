import { getAddress, isAddress, verifyTypedData, type Hex } from 'viem'

import { GatewayAuthenticationError, GatewayValidationError } from '../errors'
import {
  generateRefundToken,
  hashRefundToken,
  RouterCancellationSecretBox,
  verifyRefundToken
} from './crypto'
import type { RefundAuthorizationStore, RefundMode } from './store'

export const REFUND_EIP712_DOMAIN = {
  name: 'TEE Router Gateway',
  version: '1'
} as const

export const REFUND_EIP712_TYPES = {
  CancelOrder: [
    { name: 'orderId', type: 'string' },
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

export class RefundAuthorizationService {
  constructor(
    private readonly store: RefundAuthorizationStore,
    private readonly secretBox: RouterCancellationSecretBox
  ) {}

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
      await this.store.saveRefundAuthorization({
        routerOrderId: input.routerOrderId,
        refundMode: 'token',
        refundAuthorizer: null,
        encryptedCancellationSecret: this.secretBox.encrypt(input.cancellationSecret),
        refundTokenHash: hashRefundToken(refundToken)
      })

      return {
        refundMode: 'token',
        refundAuthorizer: null,
        refundToken
      }
    }

    const refundAuthorizer = normalizeEvmAddress(input.refundAuthorizer)
    await this.store.saveRefundAuthorization({
      routerOrderId: input.routerOrderId,
      refundMode: 'evmSignature',
      refundAuthorizer,
      encryptedCancellationSecret: this.secretBox.encrypt(input.cancellationSecret)
    })

    return {
      refundMode: 'evmSignature',
      refundAuthorizer
    }
  }

  async resolveTokenCancellationSecret(
    routerOrderId: string,
    refundToken: string
  ): Promise<string> {
    const record = await this.store.findRefundAuthorization(routerOrderId)
    if (!record) {
      throw new GatewayValidationError(
        'order does not have refund authorization configured'
      )
    }

    if (record.refundMode !== 'token' || !record.refundTokenHash) {
      throw new GatewayAuthenticationError('order does not use token refund mode')
    }

    if (!verifyRefundToken(refundToken, record.refundTokenHash)) {
      throw new GatewayAuthenticationError('invalid refund token')
    }

    await this.store.markCancellationRequested(routerOrderId)
    return this.secretBox.decrypt(record.encryptedCancellationSecret)
  }

  async resolveEvmSignatureCancellationSecret(
    input: ResolveEvmSignatureInput
  ): Promise<string> {
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

    validateDeadline(input.refundSignatureDeadline)

    const verified = await verifyTypedData({
      address: record.refundAuthorizer as Hex,
      domain: REFUND_EIP712_DOMAIN,
      types: REFUND_EIP712_TYPES,
      primaryType: 'CancelOrder',
      message: {
        orderId: input.routerOrderId,
        deadline: BigInt(input.refundSignatureDeadline)
      },
      signature: input.refundSignature as Hex
    })

    if (!verified) {
      throw new GatewayAuthenticationError('invalid refund signature')
    }

    await this.store.markCancellationRequested(input.routerOrderId)
    return this.secretBox.decrypt(record.encryptedCancellationSecret)
  }
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
}
