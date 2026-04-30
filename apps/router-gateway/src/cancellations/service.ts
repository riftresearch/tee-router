import { GatewayAuthenticationError, GatewayValidationError } from '../errors'
import {
  CancellationSecretBox,
  generateGatewayCancellationToken,
  hashGatewayCancellationToken,
  verifyGatewayCancellationToken
} from './crypto'
import type { CancellationStore } from './store'

export type CreateGatewayManagedCancellationInput = {
  routerOrderId: string
  cancellationSecret: string
  authPolicy?: Record<string, unknown>
}

export class CancellationService {
  constructor(
    private readonly store: CancellationStore,
    private readonly secretBox: CancellationSecretBox
  ) {}

  async createGatewayManagedToken(
    input: CreateGatewayManagedCancellationInput
  ): Promise<string> {
    const token = generateGatewayCancellationToken()

    await this.store.saveManagedCancellation({
      routerOrderId: input.routerOrderId,
      authMode: 'gateway_managed_token',
      authPolicy: input.authPolicy ?? {},
      encryptedCancellationSecret: this.secretBox.encrypt(input.cancellationSecret),
      gatewayTokenHash: hashGatewayCancellationToken(token)
    })

    return token
  }

  async resolveTokenCancellationSecret(
    routerOrderId: string,
    token: string
  ): Promise<string> {
    const record = await this.store.findManagedCancellation(routerOrderId)
    if (!record) {
      throw new GatewayValidationError(
        'order does not have gateway-managed cancellation configured'
      )
    }

    if (!verifyGatewayCancellationToken(token, record.gatewayTokenHash)) {
      throw new GatewayAuthenticationError('invalid cancellation token')
    }

    await this.store.markCancellationRequested(routerOrderId)
    return this.secretBox.decrypt(record.encryptedCancellationSecret)
  }
}
