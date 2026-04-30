export type CancellationAuthMode = 'gateway_managed_token'

export type StoredManagedCancellation = {
  routerOrderId: string
  authMode: CancellationAuthMode
  authPolicy: Record<string, unknown>
  encryptedCancellationSecret: string
  gatewayTokenHash: string
  cancellationRequestedAt?: string
  createdAt: string
  updatedAt: string
}

export type SaveManagedCancellationInput = {
  routerOrderId: string
  authMode: CancellationAuthMode
  authPolicy: Record<string, unknown>
  encryptedCancellationSecret: string
  gatewayTokenHash: string
}

export interface CancellationStore {
  saveManagedCancellation(input: SaveManagedCancellationInput): Promise<void>
  findManagedCancellation(
    routerOrderId: string
  ): Promise<StoredManagedCancellation | undefined>
  markCancellationRequested(routerOrderId: string): Promise<void>
}
