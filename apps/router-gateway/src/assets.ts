import { createHash } from 'node:crypto'

import { GatewayValidationError } from './errors'
import type { InternalDepositAsset } from './internal/router-client'
import { getAddress, isAddress } from 'viem'

export type AmountFormat = 'readable' | 'raw'

export type ResolvedAsset = {
  id: string
  internal: InternalDepositAsset
  decimals?: number
}

export type BitcoinAddressNetwork = 'mainnet' | 'testnet' | 'regtest'

export type AddressValidationOptions = {
  bitcoinAddressNetworks?: readonly BitcoinAddressNetwork[]
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
  base: { chainId: 'evm:8453', display: 'Base' }
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
  known('evm:8453', 'Base', 'CBBTC', '0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf', 8)
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
const MAX_UINT256 =
  115792089237316195423570985008687907853269984665640564039457584007913129639935n
const MAX_UINT256_DECIMAL_DIGITS = MAX_UINT256.toString().length
const DEFAULT_BITCOIN_ADDRESS_NETWORKS: readonly BitcoinAddressNetwork[] = ['mainnet']
const BITCOIN_BECH32_PREFIX_NETWORKS = new Map<string, BitcoinAddressNetwork>([
  ['bc', 'mainnet'],
  ['tb', 'testnet'],
  ['bcrt', 'regtest']
])
const BITCOIN_BASE58_VERSION_NETWORKS = new Map<number, BitcoinAddressNetwork>([
  [0x00, 'mainnet'],
  [0x05, 'mainnet'],
  [0x6f, 'testnet'],
  [0xc4, 'testnet']
])
const BECH32_CHARSET = 'qpzry9x8gf2tvdw0s3jn54khce6mua7l'
const BECH32M_CHECKSUM = 0x2bc830a3
const BASE58_ALPHABET =
  '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
const BASE58_VALUES = new Map(
  [...BASE58_ALPHABET].map((character, index) => [character, index])
)

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

  if (!isAddress(assetPart)) {
    throw new GatewayValidationError('address-form assets must be valid EVM addresses', {
      identifier,
      asset: assetPart
    })
  }

  const normalizedAddress = getAddress(assetPart).toLowerCase()
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
  if (format === 'raw' || asset.decimals === undefined) {
    assertIntegerString(value, 'amount')
    assertUint256Raw(value, 'amount')
    return stripLeadingZeros(value)
  }
  return rawToDecimal(value, asset.decimals)
}

export function formatPositiveAmount(
  value: string,
  asset: ResolvedAsset,
  format: AmountFormat
): string {
  assertIntegerString(value, 'amount')
  assertPositiveRaw(value, 'amount')
  return formatAmount(value, asset, format)
}

export function parseSlippageBps(value: string, format: AmountFormat): number {
  if (format === 'raw') {
    assertIntegerString(value, 'maxSlippage')
    const normalized = stripLeadingZeros(value)
    if (normalized.length > 5) {
      throw new GatewayValidationError('maxSlippage raw value must be 0 through 10000 bps')
    }
    const bps = Number(normalized)
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
  const normalizedWhole = stripLeadingZeros(whole)
  if (normalizedWhole.length > 3) {
    throw new GatewayValidationError('maxSlippage must be 0 through 100 percent')
  }
  if (fractional.length > 2) {
    throw new GatewayValidationError(
      'maxSlippage readable values support at most two decimal places'
    )
  }

  const bps = Number(normalizedWhole) * 100 + Number(fractional.padEnd(2, '0'))
  if (!Number.isSafeInteger(bps) || bps < 0 || bps > 10_000) {
    throw new GatewayValidationError('maxSlippage must be 0 through 100 percent')
  }

  return bps
}

export function assertAddressMatchesChain(
  chainId: string,
  address: string,
  field: string,
  options: AddressValidationOptions = {}
): void {
  if (chainId.startsWith('evm:') && !isAddress(address)) {
    throw new GatewayValidationError(`${field} must be a valid EVM address`, {
      field,
      chain: chainId
    })
  }

  if (
    chainId === 'bitcoin' &&
    !isBitcoinAddress(
      address,
      options.bitcoinAddressNetworks ?? DEFAULT_BITCOIN_ADDRESS_NETWORKS
    )
  ) {
    throw new GatewayValidationError(`${field} must be a valid Bitcoin address`, {
      field,
      chain: chainId,
      allowedNetworks: options.bitcoinAddressNetworks ?? DEFAULT_BITCOIN_ADDRESS_NETWORKS
    })
  }

  if (!chainId.startsWith('evm:') && chainId !== 'bitcoin') {
    throw new GatewayValidationError(
      `${field} cannot be validated for unsupported chain`,
      {
        field,
        chain: chainId
      }
    )
  }
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
  assertDecimalWholeFitsUint256(whole, field)
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
  assertUint256Raw(value, 'amount')

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
  const normalized = stripLeadingZeros(value)
  assertUint256DecimalDigitLength(normalized, field)
  const raw = BigInt(normalized)
  if (raw <= 0n) {
    throw new GatewayValidationError(`${field} must be greater than zero`, { field })
  }
  if (raw > MAX_UINT256) {
    throw new GatewayValidationError(`${field} exceeds uint256 maximum`, { field })
  }
}

function assertUint256Raw(value: string, field: string) {
  const normalized = stripLeadingZeros(value)
  assertUint256DecimalDigitLength(normalized, field)
  if (BigInt(normalized) > MAX_UINT256) {
    throw new GatewayValidationError(`${field} exceeds uint256 maximum`, { field })
  }
}

function assertDecimalWholeFitsUint256(whole: string, field: string) {
  assertUint256DecimalDigitLength(stripLeadingZeros(whole), field)
}

function assertUint256DecimalDigitLength(value: string, field: string) {
  if (value.length > MAX_UINT256_DECIMAL_DIGITS) {
    throw new GatewayValidationError(`${field} exceeds uint256 maximum`, { field })
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

function isBitcoinAddress(
  address: string,
  allowedNetworks: readonly BitcoinAddressNetwork[]
): boolean {
  const network = bitcoinAddressNetwork(address)
  return network !== undefined && allowedNetworks.includes(network)
}

function bitcoinAddressNetwork(address: string): BitcoinAddressNetwork | undefined {
  return (
    bech32SegwitBitcoinAddressNetwork(address) ??
    base58CheckBitcoinAddressNetwork(address)
  )
}

function bech32SegwitBitcoinAddressNetwork(
  address: string
): BitcoinAddressNetwork | undefined {
  if (address.length < 14 || address.length > 128) return undefined
  if (address !== address.toLowerCase() && address !== address.toUpperCase()) {
    return undefined
  }

  const normalized = address.toLowerCase()
  const separator = normalized.lastIndexOf('1')
  if (separator <= 0 || separator + 7 > normalized.length) return undefined

  const prefix = normalized.slice(0, separator)
  const network = BITCOIN_BECH32_PREFIX_NETWORKS.get(prefix)
  if (!network) return undefined

  const data = bech32Data(normalized.slice(separator + 1))
  if (!data) return undefined

  const checksum = bech32Polymod([...bech32HrpExpand(prefix), ...data])
  const checksumEncoding =
    checksum === 1 ? 'bech32' : checksum === BECH32M_CHECKSUM ? 'bech32m' : undefined
  if (!checksumEncoding) return undefined

  const payload = data.slice(0, -6)
  const witnessVersion = payload[0]
  if (witnessVersion === undefined || witnessVersion > 16) return undefined

  const witnessProgram = convertBits(payload.slice(1), 5, 8, false)
  if (!witnessProgram) return undefined
  if (witnessProgram.length < 2 || witnessProgram.length > 40) return undefined

  if (witnessVersion === 0) {
    if (
      checksumEncoding === 'bech32' &&
      (witnessProgram.length === 20 || witnessProgram.length === 32)
    ) {
      return network
    }
    return undefined
  }

  return checksumEncoding === 'bech32m' ? network : undefined
}

function bech32Data(value: string): number[] | undefined {
  const data: number[] = []
  for (const character of value) {
    const word = BECH32_CHAR_VALUES.get(character)
    if (word === undefined) return undefined
    data.push(word)
  }
  return data
}

const BECH32_CHAR_VALUES = new Map(
  [...BECH32_CHARSET].map((character, index) => [character, index])
)

function bech32HrpExpand(prefix: string): number[] {
  const expanded: number[] = []
  for (const character of prefix) {
    expanded.push(character.charCodeAt(0) >> 5)
  }
  expanded.push(0)
  for (const character of prefix) {
    expanded.push(character.charCodeAt(0) & 31)
  }
  return expanded
}

function bech32Polymod(values: number[]): number {
  const generators = [0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3]
  let checksum = 1

  for (const value of values) {
    const top = checksum >> 25
    checksum = ((checksum & 0x1ffffff) << 5) ^ value
    for (let index = 0; index < generators.length; index += 1) {
      if (((top >> index) & 1) !== 0) checksum ^= generators[index]
    }
  }

  return checksum
}

function convertBits(
  values: number[],
  fromBits: number,
  toBits: number,
  pad: boolean
): number[] | undefined {
  let accumulator = 0
  let bits = 0
  const maxValue = (1 << toBits) - 1
  const result: number[] = []

  for (const value of values) {
    if (value < 0 || value >> fromBits !== 0) return undefined
    accumulator = (accumulator << fromBits) | value
    bits += fromBits
    while (bits >= toBits) {
      bits -= toBits
      result.push((accumulator >> bits) & maxValue)
    }
  }

  if (pad) {
    if (bits > 0) result.push((accumulator << (toBits - bits)) & maxValue)
  } else if (bits >= fromBits || ((accumulator << (toBits - bits)) & maxValue) !== 0) {
    return undefined
  }

  return result
}

function base58CheckBitcoinAddressNetwork(
  address: string
): BitcoinAddressNetwork | undefined {
  const decoded = decodeBase58(address)
  if (!decoded || decoded.length !== 25) return undefined

  const payload = decoded.subarray(0, 21)
  const checksum = decoded.subarray(21)
  const network = BITCOIN_BASE58_VERSION_NETWORKS.get(payload[0])
  if (!network) return undefined

  const expected = doubleSha256(payload).subarray(0, 4)
  return bytesEqual(checksum, expected) ? network : undefined
}

function decodeBase58(value: string): Uint8Array | undefined {
  if (!value) return undefined

  let decoded = 0n
  for (const character of value) {
    const digit = BASE58_VALUES.get(character)
    if (digit === undefined) return undefined
    decoded = decoded * 58n + BigInt(digit)
  }

  const bytes: number[] = []
  while (decoded > 0n) {
    bytes.push(Number(decoded & 0xffn))
    decoded >>= 8n
  }

  for (const character of value) {
    if (character !== '1') break
    bytes.push(0)
  }

  return new Uint8Array(bytes.reverse())
}

function doubleSha256(value: Uint8Array): Uint8Array {
  return createHash('sha256')
    .update(createHash('sha256').update(value).digest())
    .digest()
}

function bytesEqual(left: Uint8Array, right: Uint8Array): boolean {
  if (left.length !== right.length) return false
  return left.every((value, index) => value === right[index])
}
