# Router Gateway Architecture Notes

This file captures the intended high-level shape for the TypeScript Bun/Hono
router gateway that sits in front of the Rust TEE router API.

## Goals

The gateway has three major responsibilities:

1. Proxy public quote and order creation traffic to the internal Rust router API.
2. Provide a gateway-managed abstraction over router cancellation secrets.
3. Serve high-volume status and analytics reads without placing unnecessary load
   on the TEE router API.

The gateway should remain API-oriented. It should avoid owning router protocol
execution logic and should call internal APIs wherever possible.

## 1. Quote And Order Proxy

The gateway should expose the public customer-facing quote and order API.

Internally it will proxy to the Rust router API endpoints, including:

- `POST /api/v1/quotes`
- `GET /api/v1/quotes/:id`
- `POST /api/v1/orders`
- `GET /api/v1/orders/:id`

The gateway can make the public interface more ergonomic than the internal API.
That may include request/response reshaping, friendlier field names, auth,
idempotency behavior, error normalization, or hiding internal-only fields.

The internal Rust router API remains the source of truth for quote creation,
order creation, vault creation, execution workflow, and provider behavior.

## Proposed Public Gateway API

This is the current proposed public gateway shape. Field names should use
camelCase in the TypeScript/OpenAPI surface.

### Asset Identifiers

`from` and `to` should be compact asset identifiers:

- popular assets can use known tickers, such as `Bitcoin.BTC`,
  `Ethereum.USDC`, `Base.USDC`, `Ethereum.ETH`, `Ethereum.USDT`,
  `Ethereum.WBTC`
- assets without a known ticker should use an address form, such as
  `Base.0x6969...`

Quote requests should not require `fromAddress` or `toAddress`; those belong to
order creation.

Implementation note: the current Rust router quote API still requires
`recipient_address` at quote time because provider quotes can bind the final
recipient. The initial gateway implementation therefore accepts a temporary
`toAddress` field on `POST /quote` and forwards it as `recipient_address`.
Removing that field requires a Rust router contract change, not just a gateway
mapping change.

### Numeric Values

Numerical values should be strings only. Do not accept JavaScript numbers for
amounts, prices, or slippage because of precision footguns.

Do not allow readability separators or unit suffixes in numeric strings:

- no underscores, e.g. reject `1_000`
- no commas, e.g. reject `1,000`
- no percent signs, e.g. reject `1.5%`

Requests can include:

```json
{
  "amountFormat": "readable"
}
```

or:

```json
{
  "amountFormat": "raw"
}
```

`amountFormat` defaults to `readable`.

`readable` means:

- amounts are decimal formatted token units, e.g. `"500.1523"` USDC
- slippage is a percentage number string, e.g. `"1.5"` for 1.5%

`raw` means:

- amounts are base-unit integer strings, e.g. `"500152300"` for 500.1523 USDC
  with 6 decimals
- slippage is basis points as an integer string, e.g. `"150"` for 1.5%

### `POST /quote`

Exact-in market quote:

```json
{
  "from": "Bitcoin.BTC",
  "to": "Ethereum.USDC",
  "fromAmount": "10",
  "maxSlippage": "1.5",
  "amountFormat": "readable"
}
```

Exact-out market quote:

```json
{
  "from": "Bitcoin.BTC",
  "to": "Ethereum.USDC",
  "toAmount": "100000",
  "maxSlippage": "150",
  "amountFormat": "raw"
}
```

The request should include exactly one of `fromAmount` or `toAmount`.

Response:

```json
{
  "quoteId": "uuid",
  "from": "Bitcoin.BTC",
  "to": "Ethereum.USDC",
  "expiry": "2026-04-29T12:00:00Z",
  "expectedOut": "99950.25",
  "expectedSlippage": "0.8",
  "minOut": "99000",
  "maxSlippage": "1.5"
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
  "expectedOut": "99950.25",
  "expectedSlippage": "0.8",
  "minOut": "99000",
  "maxSlippage": "1.5",
  "cancellationSecret": "0x..."
}
```

The Rust router API should generate and return `cancellationSecret`; the gateway
can either pass it through to the user or store it privately when
gateway-managed cancellation is requested.

### `POST /order/limit`

Limit orders are not currently supported and this endpoint should not be added
to the router gateway yet. The following shapes are design notes only for a
future API surface.

Proposed future request variants:

From amount plus price:

```json
{
  "from": "Bitcoin.BTC",
  "to": "Ethereum.USDC",
  "fromAddress": "bc1...",
  "toAddress": "0x...",
  "fromAmount": "10",
  "price": "100000",
  "expiration": "2026-04-29T12:00:00Z",
  "integrator": "partner-name",
  "amountFormat": "readable"
}
```

From amount plus to amount:

```json
{
  "from": "Bitcoin.BTC",
  "to": "Ethereum.USDC",
  "fromAddress": "bc1...",
  "toAddress": "0x...",
  "fromAmount": "10",
  "toAmount": "1000000",
  "amountFormat": "raw"
}
```

To amount plus price:

```json
{
  "from": "Bitcoin.BTC",
  "to": "Ethereum.USDC",
  "fromAddress": "bc1...",
  "toAddress": "0x...",
  "toAmount": "1000000",
  "price": "100000",
  "amountFormat": "raw"
}
```

Assume limit orders do not need slippage.

Response:

```json
{
  "orderId": "uuid",
  "orderAddress": "bc1...",
  "amountToSend": "10",
  "from": "Bitcoin.BTC",
  "to": "Ethereum.USDC",
  "expiry": "2026-04-29T12:00:00Z",
  "minOut": "1000000",
  "cancellationSecret": "0x..."
}
```

## 2. Gateway-Managed Cancellation

The Rust router API should support unilateral cancellation through a cancellation
secret flow:

- Order creation returns a generated `cancellation_secret`.
- Cancellation later requires calling `POST /api/v1/orders/:id/cancellations`
  with the matching `cancellation_secret`.
- The Rust router validates the secret by computing:
  `keccak256("router-server-cancel-v1" || secret_bytes)`.

The public order creation API should not accept a `cancellation_commitment`.
Commitments are an internal storage/verification detail only.

### Gateway Cancellation Modes

The gateway should support multiple cancellation ownership/auth modes.

Initial target modes:

- `user_secret`: user provides or receives a cancellation secret and manages it
  directly. The gateway can pass cancellation through after validating the public
  request shape.
- `gateway_managed_token`: gateway generates and stores the cancellation secret,
  and exposes a gateway-level cancellation token or credential to the user.
- `evm_eip712`: gateway stores the cancellation secret, but later cancellation is
  authorized by an EVM wallet signature over a typed cancellation payload.
- `google_oauth`: gateway stores the cancellation secret, but later cancellation
  is authorized by Google identity.

The auth layer should be generic. The gateway should store an auth policy for
each gateway-managed order rather than hard-code one auth mechanism into the
order model.

### Gateway-Managed Order Creation Flow

For a gateway-managed cancellation request:

1. Customer calls the gateway order creation endpoint.
2. Customer specifies the desired cancellation auth policy.
3. Gateway proxies order creation to the Rust router API.
4. Rust router API returns the created order response with a one-time
   `cancellation_secret`.
5. Gateway stores a private mapping in its own Postgres database:
   - router order id
   - gateway customer/account/auth identity
   - auth policy
   - encrypted cancellation secret
   - timestamps and audit metadata
6. Gateway returns an ergonomic order response to the customer.

The cancellation secret should be encrypted at rest because the gateway must be
able to recover it later to call the internal router cancellation endpoint.
Hashing alone is insufficient for gateway-managed cancellation because the raw
secret is required by the Rust router API.

Initial implementation note: `gateway_managed_token` is the first managed mode.
The gateway stores the Rust router cancellation secret encrypted in its own
Postgres table, stores only a hash of the gateway cancellation token, and later
decrypts the router secret only after token verification succeeds.

### Gateway-Managed Cancellation Flow

For a later cancellation request:

1. Customer calls the gateway cancellation endpoint.
2. Gateway loads the stored cancellation policy and encrypted secret for the
   router order id.
3. Gateway verifies the requested auth method:
   - bearer/gateway token
   - Google OAuth identity
   - EIP-712 wallet signature
   - direct cancellation secret
   - future auth providers
4. If auth succeeds, gateway decrypts the cancellation secret.
5. Gateway calls the Rust router API cancellation endpoint with that secret.
6. Gateway returns the Rust router API response, possibly reshaped into the
   public gateway response contract.

## 3. Status And Analytics Reads

The gateway should serve high-volume order status and analytics endpoints
without sending all reads to the TEE router API.

The desired shape is:

1. Router primary database remains owned by the Rust router deployment.
2. Railway-hosted replica continues to mirror router state.
3. A small internal Rust query API can run near the replica on Railway.
4. The TypeScript gateway calls that internal query API for status/analytics.
5. The gateway does not issue SQL directly for router status reads.

This avoids putting high-volume status traffic on the TEE while also avoiding
hidden query divergence in TypeScript.

## Shared Rust Query API

To avoid divergence, the status query and response formatting should live in a
shared Rust module used by both:

- the existing Rust `router-api` inside the TEE
- an optional Railway-hosted internal Rust query service pointed at the replica

The same request should return the same response whether the gateway calls:

- the TEE router API status endpoint, or
- the Railway internal read API backed by the replica

The TypeScript gateway should see both as interchangeable upstream APIs. Its
configuration can choose the preferred status upstream:

- `ROUTER_INTERNAL_BASE_URL` for write/proxy calls to the TEE router API
- `ROUTER_QUERY_API_BASE_URL` for query/status/analytics calls to the Railway query
  service

The query service should own SQL. The gateway should not duplicate SQL that also
exists in Rust.

Initial implementation note: the shared Rust order-flow query logic now lives in
`bin/router-server/src/query_api.rs`. The existing TEE router API delegates its
internal order-flow endpoint to that module, and the `router-query-api` binary
serves the same response shape from a configured database URL so it can point at
the Railway replica.

## Open Questions

- Exact public gateway endpoint names and request/response shapes.
- Exact cancellation auth policy schema.
- Whether direct cancellation-secret mode should return a generated secret to
  the user or require the user to bring one.
- Encryption key management for stored cancellation secrets.
- Whether gateway-managed cancellation storage should be its own database or a
  schema in an existing gateway Postgres instance.
- Whether the Rust query API should be a new binary in `bin/router-server` or a
  separate Rust package.
- Whether the gateway should fall back to the TEE router API if the Railway query
  service is unavailable.
