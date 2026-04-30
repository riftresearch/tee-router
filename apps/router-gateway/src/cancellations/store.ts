export type RefundMode = 'evmSignature' | 'token'

export type StoredRefundAuthorization = {
  routerOrderId: string
  refundMode: RefundMode
  refundAuthorizer: string | null
  encryptedCancellationSecret: string
  refundTokenHash?: string
  cancellationRequestedAt?: string
  createdAt: string
  updatedAt: string
}

export type SaveRefundAuthorizationInput = {
  routerOrderId: string
  refundMode: RefundMode
  refundAuthorizer: string | null
  encryptedCancellationSecret: string
  refundTokenHash?: string
}

export interface RefundAuthorizationStore {
  saveRefundAuthorization(input: SaveRefundAuthorizationInput): Promise<void>
  findRefundAuthorization(
    routerOrderId: string
  ): Promise<StoredRefundAuthorization | undefined>
  markCancellationRequested(routerOrderId: string): Promise<void>
}
