import { RedisClient } from 'bun'
import {
  createPublicClient,
  erc20Abi,
  getAddress,
  http,
  type PublicClient
} from 'viem'

import { rememberDecimals, type ResolvedAsset } from './assets'
import type { GatewayConfig } from './config'
import { GatewayValidationError } from './errors'

export type DecimalsResolver = {
  // Fills asset.decimals for EVM address-form tokens whose decimals are unknown,
  // checking a forever (no-TTL) Redis cache first and falling back to an on-chain
  // ERC-20 decimals() call. No-op for assets that already have decimals or are not
  // EVM address tokens.
  ensure(asset: ResolvedAsset): Promise<void>
}

const MAX_REASONABLE_DECIMALS = 36

function redisKey(chain: string, address: string): string {
  return `decimals:${chain}:${address.toLowerCase()}`
}

export function createDecimalsResolver(config: GatewayConfig): DecimalsResolver {
  const redis = config.redisUrl ? new RedisClient(config.redisUrl) : undefined
  const clients = new Map<string, PublicClient>()

  function clientFor(chain: string): PublicClient {
    let client = clients.get(chain)
    if (!client) {
      const rpcUrl = config.evmRpcUrls[chain]
      if (!rpcUrl) {
        throw new GatewayValidationError(
          `no RPC configured for ${chain}; cannot resolve on-chain token decimals`,
          { chain }
        )
      }
      client = createPublicClient({ transport: http(rpcUrl) })
      clients.set(chain, client)
    }
    return client
  }

  return {
    async ensure(asset: ResolvedAsset): Promise<void> {
      if (asset.decimals !== undefined) return
      const { chain, asset: address } = asset.internal
      if (!chain.startsWith('evm:') || !address.startsWith('0x')) return

      const key = redisKey(chain, address)

      // 1. Forever Redis cache (no TTL). Redis being unavailable must not fail the
      //    quote — fall through to the on-chain lookup.
      if (redis) {
        try {
          const cached = await redis.get(key)
          if (cached !== null && cached !== undefined && cached !== '') {
            const parsed = Number(cached)
            if (Number.isInteger(parsed)) {
              asset.decimals = parsed
              rememberDecimals(chain, address, parsed)
              return
            }
          }
        } catch {
          // ignore cache read errors
        }
      }

      // 2. On-chain ERC-20 decimals().
      let decimals: number
      try {
        const result = await clientFor(chain).readContract({
          address: getAddress(address),
          abi: erc20Abi,
          functionName: 'decimals'
        })
        decimals = Number(result)
      } catch {
        throw new GatewayValidationError(
          `could not read on-chain decimals() for ${asset.id}; the token may not be a standard ERC-20 (use amountFormat raw)`,
          { asset: asset.id, chain }
        )
      }
      if (
        !Number.isInteger(decimals) ||
        decimals < 0 ||
        decimals > MAX_REASONABLE_DECIMALS
      ) {
        throw new GatewayValidationError(
          `on-chain decimals() for ${asset.id} returned an unexpected value (${decimals})`,
          { asset: asset.id }
        )
      }

      asset.decimals = decimals
      rememberDecimals(chain, address, decimals)

      // 3. Write-through forever (no TTL). Best-effort.
      if (redis) {
        try {
          await redis.set(key, String(decimals))
        } catch {
          // ignore cache write errors
        }
      }
    }
  }
}
