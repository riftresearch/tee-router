import { GatewayValidationError } from './errors'
import type { InternalDepositAsset } from './internal/router-client'

export type AmountFormat = 'readable' | 'raw'

export type ResolvedAsset = {
  id: string
  internal: InternalDepositAsset
  decimals?: number
}

type KnownAsset = {
  chainId: string
  chainDisplay: string
  symbol: string
  asset: string
  decimals: number
}

const CHAIN_ALIASES: Record<string, { chainId: string; display: string }> = {
  bitcoin: { chainId: 'bitcoin', display: 'Bitcoin' },
  btc: { chainId: 'bitcoin', display: 'Bitcoin' },
  ethereum: { chainId: 'evm:1', display: 'Ethereum' },
  eth: { chainId: 'evm:1', display: 'Ethereum' },
  mainnet: { chainId: 'evm:1', display: 'Ethereum' },
  arbitrum: { chainId: 'evm:42161', display: 'Arbitrum' },
  arb: { chainId: 'evm:42161', display: 'Arbitrum' },
  base: { chainId: 'evm:8453', display: 'Base' },
  hyperliquid: { chainId: 'hyperliquid', display: 'Hyperliquid' }
}

const KNOWN_ASSETS: KnownAsset[] = [
  known('bitcoin', 'Bitcoin', 'BTC', 'native', 8),
  known('evm:1', 'Ethereum', 'ETH', 'native', 18),
  known('evm:1', 'Ethereum', 'USDC', '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', 6),
  known('evm:1', 'Ethereum', 'USDT', '0xdac17f958d2ee523a2206206994597c13d831ec7', 6),
  known('evm:1', 'Ethereum', 'CBBTC', '0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf', 8),
  known('evm:42161', 'Arbitrum', 'USDC', '0xaf88d065e77c8cc2239327c5edb3a432268e5831', 6),
  known('evm:42161', 'Arbitrum', 'USDT', '0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9', 6),
  known('evm:42161', 'Arbitrum', 'CBBTC', '0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf', 8),
  known('evm:8453', 'Base', 'ETH', 'native', 18),
  known('evm:8453', 'Base', 'USDC', '0x833589fcd6edb6e08f4c7c32d4f71b54bda02913', 6),
  known('evm:8453', 'Base', 'USDT', '0xfde4c96c8593536e31f229ea8f37b2ada2699bb2', 6),
  known('evm:8453', 'Base', 'CBBTC', '0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf', 8),
  known('hyperliquid', 'Hyperliquid', 'USDC', 'native', 6)
]

const KNOWN_BY_CHAIN_SYMBOL = new Map(
  KNOWN_ASSETS.map((asset) => [
    chainSymbolKey(asset.chainId, asset.symbol),
    asset
  ])
)

const KNOWN_BY_CHAIN_ASSET = new Map(
  KNOWN_ASSETS.map((asset) => [
    chainAssetKey(asset.chainId, asset.asset),
    asset
  ])
)

const DECIMAL_STRING_PATTERN = /^[0-9]+(?:\.[0-9]+)?$/
const INTEGER_STRING_PATTERN = /^[0-9]+$/

export function resolveAssetIdentifier(identifier: string): ResolvedAsset {
  const parts = identifier.trim().split('.')
  if (parts.length !== 2 || !parts[0] || !parts[1]) {
    throw new GatewayValidationError(
      'asset identifiers must use the form Chain.Asset',
      { identifier }
    )
  }

  const chain = CHAIN_ALIASES[parts[0].toLowerCase()]
  if (!chain) {
    throw new GatewayValidationError('unsupported asset chain', {
      identifier,
      chain: parts[0]
    })
  }

  const assetPart = parts[1]
  const known = KNOWN_BY_CHAIN_SYMBOL.get(chainSymbolKey(chain.chainId, assetPart))
  if (known) return resolvedKnownAsset(known)

  if (assetPart.toLowerCase() === 'native') {
    const native = KNOWN_BY_CHAIN_ASSET.get(chainAssetKey(chain.chainId, 'native'))
    if (native) return resolvedKnownAsset(native)
  }

  if (!assetPart.startsWith('0x')) {
    throw new GatewayValidationError('unknown ticker for chain', {
      identifier,
      chain: parts[0],
      asset: assetPart
    })
  }

  if (!chain.chainId.startsWith('evm:')) {
    throw new GatewayValidationError('address-form assets are only supported on EVM chains', {
      identifier
    })
  }

  const normalizedAddress = assetPart.toLowerCase()
  return {
    id: `${chain.display}.${normalizedAddress}`,
    internal: {
      chain: chain.chainId,
      asset: normalizedAddress
    }
  }
}

export function assetIdentifierFromInternal(asset: InternalDepositAsset): ResolvedAsset {
  const known = KNOWN_BY_CHAIN_ASSET.get(chainAssetKey(asset.chain, asset.asset))
  if (known) return resolvedKnownAsset(known)

  const chain = Object.values(CHAIN_ALIASES).find(
    (candidate) => candidate.chainId === asset.chain
  )
  const display = chain?.display ?? asset.chain
  return {
    id: `${display}.${asset.asset}`,
    internal: asset
  }
}

export function parseAmount(
  value: string,
  asset: ResolvedAsset,
  format: AmountFormat,
  field: string
): string {
  if (format === 'raw') {
    assertIntegerString(value, field)
    assertPositiveRaw(value, field)
    return stripLeadingZeros(value)
  }

  if (asset.decimals === undefined) {
    throw new GatewayValidationError(
      `${field} uses readable format, but ${asset.id} does not have known decimals; use amountFormat raw`,
      { field, asset: asset.id }
    )
  }

  return decimalToRaw(value, asset.decimals, field)
}

export function formatAmount(
  value: string,
  asset: ResolvedAsset,
  format: AmountFormat
): string {
  if (format === 'raw' || asset.decimals === undefined) return value
  return rawToDecimal(value, asset.decimals)
}

export function parseSlippageBps(value: string, format: AmountFormat): number {
  if (format === 'raw') {
    assertIntegerString(value, 'maxSlippage')
    const bps = Number(value)
    if (!Number.isSafeInteger(bps) || bps < 0 || bps > 10_000) {
      throw new GatewayValidationError('maxSlippage raw value must be 0 through 10000 bps')
    }
    return bps
  }

  if (!DECIMAL_STRING_PATTERN.test(value)) {
    throw new GatewayValidationError(
      'maxSlippage must be a decimal percentage string without separators or %'
    )
  }

  const [whole, fractional = ''] = value.split('.')
  if (fractional.length > 2) {
    throw new GatewayValidationError(
      'maxSlippage readable values support at most two decimal places'
    )
  }

  const bps = Number(whole) * 100 + Number(fractional.padEnd(2, '0'))
  if (!Number.isSafeInteger(bps) || bps < 0 || bps > 10_000) {
    throw new GatewayValidationError('maxSlippage must be 0 through 100 percent')
  }

  return bps
}

export function formatSlippage(bps: number, format: AmountFormat): string {
  if (format === 'raw') return String(bps)

  const whole = Math.floor(bps / 100)
  const fractional = String(bps % 100).padStart(2, '0').replace(/0+$/, '')
  return fractional ? `${whole}.${fractional}` : String(whole)
}

function decimalToRaw(value: string, decimals: number, field: string): string {
  if (!DECIMAL_STRING_PATTERN.test(value)) {
    throw new GatewayValidationError(
      `${field} must be a decimal string without separators`,
      { field }
    )
  }

  const [whole, fractional = ''] = value.split('.')
  if (fractional.length > decimals) {
    throw new GatewayValidationError(`${field} has too many decimal places`, {
      field,
      decimals
    })
  }

  const raw = `${whole}${fractional.padEnd(decimals, '0')}`
  assertPositiveRaw(raw, field)
  return stripLeadingZeros(raw)
}

function rawToDecimal(value: string, decimals: number): string {
  assertIntegerString(value, 'amount')

  const stripped = stripLeadingZeros(value)
  if (decimals === 0) return stripped

  const padded = stripped.padStart(decimals + 1, '0')
  const whole = padded.slice(0, -decimals)
  const fractional = padded.slice(-decimals).replace(/0+$/, '')
  return fractional ? `${whole}.${fractional}` : whole
}

function assertIntegerString(value: string, field: string) {
  if (!INTEGER_STRING_PATTERN.test(value)) {
    throw new GatewayValidationError(`${field} must be an integer string`, { field })
  }
}

function assertPositiveRaw(value: string, field: string) {
  if (BigInt(value) <= 0n) {
    throw new GatewayValidationError(`${field} must be greater than zero`, { field })
  }
}

function stripLeadingZeros(value: string): string {
  return value.replace(/^0+(?=\d)/, '')
}

function resolvedKnownAsset(asset: KnownAsset): ResolvedAsset {
  return {
    id: `${asset.chainDisplay}.${asset.symbol}`,
    internal: {
      chain: asset.chainId,
      asset: asset.asset
    },
    decimals: asset.decimals
  }
}

function known(
  chainId: string,
  chainDisplay: string,
  symbol: string,
  asset: string,
  decimals: number
): KnownAsset {
  return {
    chainId,
    chainDisplay,
    symbol,
    asset: asset.toLowerCase(),
    decimals
  }
}

function chainSymbolKey(chainId: string, symbol: string): string {
  return `${chainId}:${symbol.toUpperCase()}`
}

function chainAssetKey(chainId: string, asset: string): string {
  return `${chainId}:${asset.toLowerCase()}`
}
