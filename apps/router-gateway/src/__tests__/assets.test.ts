import { describe, expect, test } from 'bun:test'

import {
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
  })

  test('converts readable token amounts to raw units', () => {
    const btc = resolveAssetIdentifier('Bitcoin.BTC')
    const usdc = resolveAssetIdentifier('Ethereum.USDC')

    expect(parseAmount('1.25', btc, 'readable', 'fromAmount')).toBe('125000000')
    expect(parseAmount('500.1523', usdc, 'readable', 'toAmount')).toBe('500152300')
    expect(formatAmount('500152300', usdc, 'readable')).toBe('500.1523')
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
})
