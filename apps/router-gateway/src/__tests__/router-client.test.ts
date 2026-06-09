import { expect, test } from 'bun:test'

import { RouterClient, type FetchLike } from '../internal/router-client'

const QUOTE_ID = '11111111-1111-4111-8111-111111111111'

test('RouterClient rejects non-canonical base URLs', () => {
  expect(() => new RouterClient({ baseUrl: '', timeoutMs: 1_000 })).toThrow(
    'router base URL is required'
  )

  for (const baseUrl of [
    'ftp://router.internal',
    'https://user:pass@router.internal',
    'https://router.internal?token=secret',
    'https://router.internal#fragment'
  ]) {
    expect(
      () => new RouterClient({ baseUrl, timeoutMs: 1_000 })
    ).toThrow(
      'router base URL must be an absolute HTTP(S) URL without credentials, query string, or fragment'
    )
  }
})

test('RouterClient strips trailing slashes before appending API paths', async () => {
  const calls: string[] = []
  const fetcher: FetchLike = async (input) => {
    calls.push(input.toString())
    return Response.json({
      quote: {
        type: 'market_order',
        payload: {
          id: QUOTE_ID,
          source_asset: {
            chain: 'evm:1',
            asset: '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'
          },
          destination_asset: {
            chain: 'evm:8453',
            asset: '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'
          },
          recipient_address: '0x1111111111111111111111111111111111111111',
          provider_id: 'test-provider',
          amount_in: '1000000',
          estimated_amount_out: '999000',
          expires_at: '2026-05-05T00:00:00Z',
          created_at: '2026-05-05T00:00:00Z'
        }
      }
    })
  }

  const client = new RouterClient({
    baseUrl: 'https://router.internal///',
    fetch: fetcher,
    timeoutMs: 1_000
  })
  await client.getQuote(QUOTE_ID)

  expect(calls).toEqual([
    `https://router.internal/api/v1/quotes/${encodeURIComponent(QUOTE_ID)}`
  ])
})

test('RouterClient maps insufficient_liquidity upstream kind to the dedicated error', async () => {
  const fetcher: FetchLike = async () =>
    Response.json(
      {
        error: {
          code: 422,
          kind: 'insufficient_liquidity',
          message: 'insufficient liquidity for the requested size'
        }
      },
      { status: 422 }
    )
  const client = new RouterClient({
    baseUrl: 'https://router.internal',
    fetch: fetcher,
    timeoutMs: 1_000
  })

  expect(client.getQuote(QUOTE_ID)).rejects.toMatchObject({
    code: 'INSUFFICIENT_LIQUIDITY',
    status: 422,
    message: 'insufficient liquidity for the requested size'
  })
})

test('RouterClient keeps plain 422s as generic upstream errors', async () => {
  const fetcher: FetchLike = async () =>
    Response.json(
      { error: { code: 422, message: 'No route found: nothing matched' } },
      { status: 422 }
    )
  const client = new RouterClient({
    baseUrl: 'https://router.internal',
    fetch: fetcher,
    timeoutMs: 1_000
  })

  expect(client.getQuote(QUOTE_ID)).rejects.toMatchObject({
    code: 'UPSTREAM_ERROR',
    upstreamStatus: 422
  })
})
