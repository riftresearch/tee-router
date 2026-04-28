# Paymaster Runtime Reimbursement Ledger

## Context

The router currently computes a paymaster reimbursement plan at quote time. That
plan estimates the native gas Rift expects to advance across the selected route,
converts the estimate into a settlement asset, applies a safety multiplier, and
adds retention actions to the quote.

At execution time, however, the paymaster must fund the actual transaction that
is about to be submitted. Runtime funding should always use the live gas need
for that transaction. It should not send less because the quote estimated less,
and it should not send more because the quote estimated more.

This document records the intended future hardening model. It is not yet an
implementation plan for this branch.

## Desired Model

Keep both concepts:

- **Quote-time estimate**: the router predicts expected paymaster cost and uses
  that to reserve or retain enough user funds.
- **Runtime actuals**: every paymaster top-up records the exact native gas token
  amount sent for a specific chain, vault, order, and action.

The runtime path should persist a paymaster spend ledger entry for every native
gas top-up. At minimum, each entry should identify:

- order id
- execution attempt id
- execution step id
- chain id
- custody vault id and address
- provider/action kind
- native token amount sent by the paymaster
- paymaster funding tx hash
- gas estimate and fee estimate used for the top-up
- timestamp

The paymaster actor remains responsible for funding the exact transaction need.
The router accounting layer becomes responsible for comparing actual spend with
retained reimbursement.

## Exact Reimbursement Case

When the chosen reimbursement point happens after the paymaster-funded actions,
the router can reimburse exactly.

Example:

1. User starts with Base USDC.
2. Router uses CCTP Base USDC -> Arbitrum USDC.
3. Router uses the Hyperliquid bridge from Arbitrum USDC -> Hyperliquid USDC.
4. Router plans to take paymaster reimbursement from Hyperliquid USDC before
   the Hyperliquid trade.

By the time funds are on Hyperliquid, the router already knows the actual Base
ETH and Arbitrum ETH that were sent by the paymaster. The router can convert
that exact native-gas notional into USDC and retain the exact amount from the
Hyperliquid USDC balance.

In this case, the quote-time estimate is only a planning bound. The runtime
ledger gives the final amount to retain.

## Estimate-Only Case

Exact reimbursement is not always possible.

If the reimbursement point is on the first chain, or otherwise occurs before all
paymaster-funded actions have happened, the router cannot know the full actual
spend yet. In that case, the router should retain the quote-time estimate and
safety multiplier, then rely on aggregate performance over time.

Example:

- A multi-hop route takes reimbursement on the source chain before a later
  destination-chain receive action.
- The later action's exact gas cost is not known yet.
- The router retains the estimate and accepts that this is a statistical bet.

If historical data shows the router is under-recovering, the safety multiplier
can be adjusted from `1.25x` to another value, such as `1.5x` or `2x`, based on
observed paymaster PnL.

## Accounting Goal

The system should eventually distinguish:

- estimated paymaster reimbursement
- actual native gas funded
- actual reimbursement collected
- surplus or shortfall

This would let the team analyze paymaster profitability and tune the multiplier
with data rather than guesswork.

## Non-Goals For Now

- Do not block the current alpha on exact paymaster reconciliation.
- Do not change execution semantics yet.
- Do not implement a user-facing refund or surcharge model yet.
- Do not require reimbursement to happen on the same chain as the gas spend.

The immediate goal is only to preserve this intended model so it can be built
cleanly later.
