import { describe, expect, test } from 'bun:test'

import {
  assertAddressMatchesChain,
  formatAmount,
  formatSlippage,
  parseAmount,
  parseSlippageBps,
  resolveAssetIdentifier
} from '../assets'

describe('asset and amount helpers', () => {
  test('resolves known public asset identifiers to router assets', () => {
    expect(resolveAssetIdentifier('Bitcoin.BTC')).toEqual({
      id: 'Bitcoin.BTC',
      internal: {
        chain: 'bitcoin',
        asset: 'native'
      },
      decimals: 8
    })

    expect(resolveAssetIdentifier('Base.USDC')).toEqual({
      id: 'Base.USDC',
      internal: {
        chain: 'evm:8453',
        asset: '0x833589fcd6edb6e08f4c7c32d4f71b54bda02913'
      },
      decimals: 6
    })

    expect(
      resolveAssetIdentifier('Ethereum.0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48')
    ).toEqual({
      id: 'Ethereum.0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
      internal: {
        chain: 'evm:1',
        asset: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
      }
    })
  })

  test('rejects invalid and internal-only asset identifiers', () => {
    expect(() => resolveAssetIdentifier('Ethereum.0xdead')).toThrow(
      'address-form assets must be valid EVM addresses'
    )
    expect(() => resolveAssetIdentifier('Hyperliquid.USDC')).toThrow(
      'unsupported asset chain'
    )
  })

  test('converts readable token amounts to raw units', () => {
    const btc = resolveAssetIdentifier('Bitcoin.BTC')
    const usdc = resolveAssetIdentifier('Ethereum.USDC')

    expect(parseAmount('1.25', btc, 'readable', 'fromAmount')).toBe('125000000')
    expect(parseAmount('500.1523', usdc, 'readable', 'toAmount')).toBe('500152300')
    expect(formatAmount('500152300', usdc, 'readable')).toBe('500.1523')
  })

  test('rejects raw amounts above uint256 before routing upstream', () => {
    const btc = resolveAssetIdentifier('Bitcoin.BTC')
    const aboveUint256 =
      '115792089237316195423570985008687907853269984665640564039457584007913129639936'

    expect(() =>
      parseAmount(aboveUint256, btc, 'raw', 'fromAmount')
    ).toThrow('fromAmount exceeds uint256 maximum')
    expect(() =>
      formatAmount(aboveUint256, btc, 'readable')
    ).toThrow('amount exceeds uint256 maximum')
  })

  test('rejects oversized amount digit strings before BigInt parsing', () => {
    const btc = resolveAssetIdentifier('Bitcoin.BTC')
    const oversized = '1'.repeat(10_000)

    expect(() => parseAmount(oversized, btc, 'raw', 'fromAmount')).toThrow(
      'fromAmount exceeds uint256 maximum'
    )
    expect(() => parseAmount(oversized, btc, 'readable', 'fromAmount')).toThrow(
      'fromAmount exceeds uint256 maximum'
    )
    expect(() => formatAmount(oversized, btc, 'readable')).toThrow(
      'amount exceeds uint256 maximum'
    )
  })

  test('rejects readability separators and percent signs', () => {
    const btc = resolveAssetIdentifier('Bitcoin.BTC')

    expect(() => parseAmount('1_000', btc, 'readable', 'fromAmount')).toThrow()
    expect(() => parseAmount('1,000', btc, 'readable', 'fromAmount')).toThrow()
    expect(() => parseSlippageBps('1.5%', 'readable')).toThrow()
  })

  test('converts slippage between readable percent and raw bps', () => {
    expect(parseSlippageBps('1.5', 'readable')).toBe(150)
    expect(parseSlippageBps('150', 'raw')).toBe(150)
    expect(formatSlippage(150, 'readable')).toBe('1.5')
    expect(formatSlippage(150, 'raw')).toBe('150')
  })

  test('rejects oversized slippage digit strings before numeric parsing', () => {
    const oversized = '1'.repeat(10_000)

    expect(() => parseSlippageBps(oversized, 'raw')).toThrow(
      'maxSlippage raw value must be 0 through 10000 bps'
    )
    expect(() => parseSlippageBps(oversized, 'readable')).toThrow(
      'maxSlippage must be 0 through 100 percent'
    )
  })

  test('validates Bitcoin recipient addresses with checksum enforcement', () => {
    expect(() =>
      assertAddressMatchesChain(
        'bitcoin',
        'bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh',
        'toAddress'
      )
    ).not.toThrow()
    expect(() =>
      assertAddressMatchesChain(
        'bitcoin',
        'bcrt1q2pfqp8a574jxyszmk0h5rxf02wwkpaf4hd8009',
        'toAddress',
        { bitcoinAddressNetworks: ['regtest'] }
      )
    ).not.toThrow()
    expect(() =>
      assertAddressMatchesChain(
        'bitcoin',
        '1BoatSLRHtKNngkdXEeobR76b53LETtpyT',
        'toAddress'
      )
    ).not.toThrow()
    expect(() =>
      assertAddressMatchesChain(
        'bitcoin',
        'bcrt1q2pfqp8a574jxyszmk0h5rxf02wwkpaf4hd8009',
        'toAddress',
        { bitcoinAddressNetworks: ['mainnet'] }
      )
    ).toThrow('toAddress must be a valid Bitcoin address')

    expect(() =>
      assertAddressMatchesChain(
        'bitcoin',
        '0x1111111111111111111111111111111111111111',
        'toAddress'
      )
    ).toThrow('toAddress must be a valid Bitcoin address')
    expect(() =>
      assertAddressMatchesChain(
        'bitcoin',
        'bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wla',
        'toAddress'
      )
    ).toThrow('toAddress must be a valid Bitcoin address')
    expect(() =>
      assertAddressMatchesChain(
        'hyperliquid',
        '0x1111111111111111111111111111111111111111',
        'toAddress'
      )
    ).toThrow('toAddress cannot be validated for unsupported chain')
  })
})
