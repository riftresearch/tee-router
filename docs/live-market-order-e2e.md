# Live Market-Order E2E

This is an ignored, opt-in real-money test for the provider route:

```text
source EVM wallet -> Across -> Unit deposit address -> Hyperliquid spot -> Unit withdrawal
```

It is intentionally not part of normal CI or normal `cargo nextest run`. It will
only spend funds when both guards are set:

```bash
export ROUTER_LIVE_E2E=1
export ROUTER_LIVE_CONFIRM_SPEND=I_UNDERSTAND_THIS_SPENDS_REAL_FUNDS
```

Install the official Hyperliquid Python SDK in the Python environment used by
the test:

```bash
python3 -m pip install hyperliquid-python-sdk
```

Then provide the live route configuration:

```bash
export ROUTER_LIVE_SOURCE_RPC_URL="https://base-mainnet.example"
export ROUTER_LIVE_EVM_PRIVATE_KEY="0x..."
export ROUTER_LIVE_EXPECTED_EVM_ADDRESS="0x..." # optional but recommended

export ROUTER_LIVE_ACROSS_API_URL="https://app.across.to/api"
export ROUTER_LIVE_ACROSS_API_KEY="..."
export ROUTER_LIVE_ACROSS_INTEGRATOR_ID="0x...."
export ROUTER_LIVE_ACROSS_TRADE_TYPE="exactInput"
export ROUTER_LIVE_ACROSS_ORIGIN_CHAIN_ID=8453
export ROUTER_LIVE_ACROSS_DESTINATION_CHAIN_ID=1
export ROUTER_LIVE_ACROSS_INPUT_TOKEN="0x..."
export ROUTER_LIVE_ACROSS_OUTPUT_TOKEN="0x..."
export ROUTER_LIVE_ACROSS_REFUND_ON_ORIGIN=true
export ROUTER_LIVE_ACROSS_SLIPPAGE="0.005" # optional; "auto" or 0-1 decimal
export ROUTER_LIVE_ACROSS_EXTRA_QUERY_JSON='{}' # optional scalar query params
export ROUTER_LIVE_ALLOW_APPROVAL_TXNS=false # set true only after reviewing allowances

export ROUTER_LIVE_UNIT_API_URL="https://api.hyperunit.xyz"
export ROUTER_LIVE_UNIT_DEPOSIT_SRC_CHAIN="ethereum"
export ROUTER_LIVE_UNIT_DEPOSIT_ASSET="usdc"
export ROUTER_LIVE_UNIT_WITHDRAW_DST_CHAIN="bitcoin"
export ROUTER_LIVE_UNIT_WITHDRAW_ASSET="btc"

export ROUTER_LIVE_HYPERLIQUID_API_URL="https://api.hyperliquid.xyz"
export ROUTER_LIVE_HYPERLIQUID_PYTHON="python3"
export ROUTER_LIVE_HYPERLIQUID_PRIVATE_KEY="0x..."
export ROUTER_LIVE_HYPERLIQUID_DESTINATION_ADDRESS="0x..."
# The standalone live differential harness still accepts the optional account
# and vault address overrides below. The router runtime itself no longer accepts
# shared Hyperliquid execution identities; routed execution always uses
# per-order router-derived custody.
export ROUTER_LIVE_HYPERLIQUID_ACCOUNT_ADDRESS="0x..." # optional harness-only master/user address override
export ROUTER_LIVE_HYPERLIQUID_VAULT_ADDRESS="0x..." # optional harness-only subaccount override
export ROUTER_LIVE_HYPERLIQUID_TRADE_PLAN_JSON='[
  {"kind":"market_open","coin":"UBTC/USDC","is_buy":true,"size":"0.00031","slippage":"0.05","wait_secs":"3"}
]'
export ROUTER_LIVE_HYPERLIQUID_WITHDRAW_TOKEN_SYMBOL="UBTC"
export ROUTER_LIVE_HYPERLIQUID_WITHDRAW_AMOUNT="0.0003"

export ROUTER_LIVE_WITHDRAW_RECIPIENT_ADDRESS="bc1..."
export ROUTER_LIVE_REFUND_ADDRESS="0x..."

export ROUTER_LIVE_AMOUNT_IN="50000000"
export ROUTER_LIVE_MAX_AMOUNT_IN="50000000"
export ROUTER_LIVE_MIN_ACROSS_OUTPUT_AMOUNT="45000000"
export ROUTER_LIVE_POLL_INTERVAL_SECS=15
export ROUTER_LIVE_TIMEOUT_SECS=3600
```

Run the env contract first:

```bash
cargo nextest run -p router-server --test live_market_order_e2e \
  --run-ignored only live_market_order_e2e_environment_contract --no-capture
```

Run the real-money route:

```bash
cargo nextest run -p router-server --test live_market_order_e2e \
  --run-ignored only live_across_unit_hyperliquid_real_money_route --no-capture
```

The test refuses to submit transactions when `ROUTER_LIVE_AMOUNT_IN` exceeds
`ROUTER_LIVE_MAX_AMOUNT_IN`, and it also checks the Across quote's
`maxInputAmount` / `inputAmount` against `ROUTER_LIVE_MAX_AMOUNT_IN` before any
transaction is submitted. It checks the RPC chain ID against
`ROUTER_LIVE_ACROSS_ORIGIN_CHAIN_ID`, requires the Across quote min output to be
at least `ROUTER_LIVE_MIN_ACROSS_OUTPUT_AMOUNT`, and requires the Across refund
address to match the source wallet. By default it refuses Across ERC-20 approval
transactions because they may grant broad allowances; use a native-token route,
pre-approve manually, or explicitly set `ROUTER_LIVE_ALLOW_APPROVAL_TXNS=true`
with a throwaway wallet after reviewing the returned approval.

Use an asset/chain pair that Unit actually accepts for the deposit leg. The
Across `outputToken` must be the same asset represented by
`ROUTER_LIVE_UNIT_DEPOSIT_ASSET`; the harness does not try to repair a route
where Across delivers an ERC-20 that Unit will not credit.
