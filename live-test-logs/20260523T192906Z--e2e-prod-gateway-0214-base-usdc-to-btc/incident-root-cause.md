# Incident Root Cause: Order 019e5650-75fd-7c91-afe9-faa07a997697

Date: 2026-05-23  
Workflow: `order:019e5650-75fd-7c91-afe9-faa07a997697:execution`  
Run ID: `019e5650-96a9-76ae-a5fc-7dcaaeace728`  
Route: Base USDC -> CCTP -> Arbitrum USDC -> Hyperliquid -> UBTC -> Bitcoin

## Summary

Two separate problems appeared in the same order.

The original stuck workflow was caused by a production config mismatch: Sauron was trying to query the Arbitrum EVM token indexer on internal port `4001`, but the Railway token-indexer container was actually listening on `8080`. Because of that, Sauron could not validate the Hyperliquid bridge deposit credit and could not emit the workflow hint.

After setting the token-indexer services to listen on `PORT=4001`, the workflow immediately progressed. It then hit a separate execution-accounting bug: the Hyperliquid trade bought `50000` raw UBTC, but Hyperliquid charged the trading fee in UBTC, leaving only `49965` raw UBTC available. The next Unit withdrawal step still required `50000`, so it failed and the order entered refund flow.

## What Failed

### 1. Sauron could not verify the bridge deposit

Expected:
- Sauron reads the Arbitrum token indexer.
- Sauron observes the Hyperliquid bridge deposit credit.
- Sauron sends `HlBridgeDepositCredited` to Temporal.

Actual:
- Sauron logged token-indexer request failures for the Hyperliquid bridge deposit provider operation.
- The token indexer was reachable inside Railway on `8080`, not `4001`.
- Sauron's configured URL targeted `evm-token-indexer-arbitrum-v3.railway.internal:4001`.

Root cause:
- Railway service env did not set `PORT=4001`.
- Existing repo/deploy intent expected token indexers on `4001` via `PONDER_PORT=4001` and Sauron URLs.
- Railway's runtime port selection made the app listen on `8080`, producing internal connection failures.

Fix applied:
- Set `PORT=4001` in production for:
  - `evm-token-indexer-arbitrum-v3`
  - `evm-token-indexer-base-v3`
  - `evm-token-indexer-ethereum-v3`
- Updated token-indexer env examples and Dockerfiles so `PORT` and `PONDER_PORT` stay aligned.

Evidence after fix:
- Temporal event `84` received `HlBridgeDepositCredited`.
- Temporal event `90` accepted the hint.
- Temporal event `94` canceled the bridge-deposit wait timer.

### 2. Hyperliquid fee accounting made the final withdrawal impossible

Expected:
- Trade output and downstream withdrawal amount should use the net spendable UBTC balance.

Actual:
- Temporal event `97` requoted from Hyperliquid USDC to Bitcoin and requested `50000` raw UBTC output.
- Temporal event `110` completed the Hyperliquid trade.
- Hyperliquid fill evidence showed a UBTC-denominated fee:
  - Gross fill size: `0.0005` UBTC
  - Fee token: `UBTC`
  - Fee: `0.00000035` UBTC
  - Net available: `0.00049965` UBTC = `49965` raw
- Temporal event `117` still planned Unit withdrawal with `available_amount: "50000"`.
- Temporal event `130` failed: `available Hyperliquid UBTC 49965 is below minimum 50000`.
- Temporal event `149` classified the failed attempt as `StartRefund`.
- Temporal event `162` completed the parent workflow with `terminal_status: "RefundRequired"`.

Root cause:
- The workflow treated Hyperliquid trade output as gross filled base size.
- It did not subtract output-token trading fees before materializing the next leg.
- Unit withdrawal correctly refused because the live balance was below the requested minimum.

## Impact

- The original observer stall was fixed by the Railway port correction.
- The order did not complete to Bitcoin.
- The order entered `RefundRequired` / gateway `refund_pending`.
- Funds were not lost; the remaining value is held in Hyperliquid custody and refund handling owns recovery.

## Code Fix Needed

The router should not carry gross Hyperliquid fill output into downstream legs.

Preferred fix:
- When a Hyperliquid fill fee is charged in the output token, subtract that fee from `actual_amount_out`.
- When a fee is charged in the input token, account for it in `actual_amount_in`.
- Persist net amounts so boundary requotes and Unit withdrawals consume spendable balances.

Additional hardening:
- At completed-leg boundary requote, cap Hyperliquid source amount by live spot balance when the current asset is on Hyperliquid.
- This protects against any future provider response that reports gross fill amounts without fee detail.

Relevant code areas:
- `crates/router-core/src/services/action_providers.rs`
  - `HyperliquidProvider::post_execute`
  - `hyperliquid_filled_actual_amounts`
- `crates/router-core/src/db/order_repo.rs`
  - leg rollup of `actual_amount_out`
- `bin/temporal-worker/src/order_execution/activities/refresh.rs`
  - completed-leg boundary requote source amount

## Current Edit State

No code fix for the Hyperliquid fee bug has been applied yet.

Applied production/config changes:
- Railway `PORT=4001` set on the three EVM token indexers.
- Repo env examples and token-indexer Dockerfiles updated to preserve that port contract.

