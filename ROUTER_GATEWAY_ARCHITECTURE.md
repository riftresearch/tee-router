# Router Gateway Architecture Notes

This file captures the high-level shape for the TypeScript Bun/Hono router
gateway that sits in front of the Rust TEE router API.

## Goals

The gateway has three major responsibilities:

1. Proxy public quote and order creation traffic to the internal Rust router API.
2. Provide a gateway-managed abstraction over router cancellation secrets.
3. Serve high-volume status and analytics reads without placing unnecessary load
   on the TEE router API.

The gateway should remain API-oriented. It should avoid owning router protocol
execution logic and should call internal APIs wherever possible.

## Quote And Order Proxy

The gateway exposes the public customer-facing market quote and order API.

Internally it proxies to the Rust router API endpoints:

- `POST /api/v1/quotes`
- `GET /api/v1/quotes/:id`
- `POST /api/v1/orders`
- `GET /api/v1/orders/:id`

Limit order creation is disabled. The gateway must not register a public limit
order endpoint while limit orders are disabled.

## Public Gateway API

Field names use camelCase in the TypeScript/OpenAPI surface.

### Asset Identifiers

`from` and `to` are compact asset identifiers:

- Known assets can use tickers, such as `Bitcoin.BTC`, `Ethereum.USDC`,
  `Base.USDC`, `Ethereum.ETH`, `Ethereum.USDT`, or `Ethereum.WBTC`.
- Assets without a known ticker use an address form, such as `Base.0x6969...`.

The current Rust router quote API still requires `recipient_address` at quote
time because provider quotes can bind the final recipient. The gateway therefore
accepts `toAddress` on `POST /quote` and forwards it as `recipient_address`.

### Numeric Values

Numerical values are strings only. JavaScript numbers are rejected for amounts
because of precision footguns.

Do not allow readability separators or unit suffixes in numeric strings:

- no underscores, e.g. reject `1_000`
- no commas, e.g. reject `1,000`
- no percent signs, e.g. reject `1.5%`

Requests can include `"amountFormat": "readable"` or `"amountFormat": "raw"`.
`amountFormat` defaults to `readable`.

`readable` amounts are decimal formatted token units, e.g. `"500.1523"` USDC.
`raw` amounts are base-unit integer strings, e.g. `"500152300"` for 500.1523
USDC with 6 decimals.

### `POST /quote`

Market quotes are exact-in only:

```json
{
  "from": "Bitcoin.BTC",
  "to": "Ethereum.USDC",
  "toAddress": "0x...",
  "fromAmount": "10",
  "amountFormat": "readable"
}
```

Response:

```json
{
  "quoteId": "uuid",
  "from": "Bitcoin.BTC",
  "to": "Ethereum.USDC",
  "expiry": "2026-04-29T12:00:00Z",
  "estimatedOut": "99950.25"
}
```

### `POST /order/market`

Request:

```json
{
  "quoteId": "uuid",
  "fromAddress": "bc1a...",
  "toAddress": "0x...",
  "refundAddress": "bc1q...",
  "integrator": "partner-name"
}
```

`refundAddress` can be optional and default to `fromAddress` unless explicitly
specified.

Response:

```json
{
  "orderId": "uuid",
  "orderAddress": "bc1...",
  "amountToSend": "10",
  "quoteId": "uuid",
  "from": "Bitcoin.BTC",
  "to": "Ethereum.USDC",
  "expiry": "2026-04-29T12:00:00Z",
  "estimatedOut": "99950.25",
  "cancellationSecret": "0x..."
}
```

The Rust router API generates and returns `cancellationSecret`. The gateway can
either pass it through to the user or store it privately when gateway-managed
cancellation is requested.

## Gateway-Managed Cancellation

The Rust router API supports unilateral cancellation through a cancellation
secret flow:

- Order creation returns a generated `cancellation_secret`.
- Cancellation later calls `POST /api/v1/orders/:id/cancellations` with the
  matching `cancellation_secret`.
- The Rust router validates the secret by computing:
  `keccak256("router-server-cancel-v1" || secret_bytes)`.

The public order creation API should not accept a `cancellationCommitment`.
Commitments are an internal storage/verification detail only.

## Status Mapping

The gateway maps internal router statuses into the public status vocabulary.
`refund_required` and `refunding` are both exposed as `refund_pending`, because
the public user only needs to know that refund handling is underway or queued.
