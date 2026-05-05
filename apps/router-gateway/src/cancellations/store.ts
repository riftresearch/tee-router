export type RefundMode = 'evmSignature' | 'token'

export type StoredRefundAuthorization = {
  routerOrderId: string
  refundMode: RefundMode
  refundAuthorizer: string | null
  encryptedCancellationSecret: string
  cancellationSecretHash: string
  refundTokenHash?: string
  encryptedRefundToken?: string
  cancellationRequestedAt?: string
  createdAt: string
  updatedAt: string
}

export type SaveRefundAuthorizationInput = {
  routerOrderId: string
  refundMode: RefundMode
  refundAuthorizer: string | null
  encryptedCancellationSecret: string
  cancellationSecretHash: string
  refundTokenHash?: string
  encryptedRefundToken?: string
}

export type ClaimRefundAuthorizationExpectation = SaveRefundAuthorizationInput

export interface RefundAuthorizationStore {
  saveRefundAuthorization(
    input: SaveRefundAuthorizationInput,
    claimTimeoutMs: number
  ): Promise<StoredRefundAuthorization | undefined>
  findRefundAuthorization(
    routerOrderId: string
  ): Promise<StoredRefundAuthorization | undefined>
  claimCancellationAttempt(
    routerOrderId: string,
    claimId: string,
    claimTimeoutMs: number,
    expected: ClaimRefundAuthorizationExpectation
  ): Promise<boolean>
  releaseCancellationAttempt(
    routerOrderId: string,
    claimId: string
  ): Promise<void>
  markCancellationRequested(
    routerOrderId: string,
    claimId: string
  ): Promise<boolean>
}
