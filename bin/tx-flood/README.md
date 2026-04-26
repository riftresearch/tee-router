# tx-flood

`tx-flood` was a terminal load-testing tool for the old OTC stack.

Functionally, it did four things:

1. Created a configurable stream of OTC swaps against the RFQ + OTC APIs.
2. Managed funded Bitcoin, Ethereum, and Base wallets, with optional dedicated per-swap wallets.
3. Drove the full swap lifecycle from quote request through swap creation, deposit submission, polling, and settlement tracking.
4. Rendered a live TUI showing per-swap stages, logs, batching state, and final pass/fail counts.

Key behaviors from the old implementation:

- Supported directed flows like `eth->btc`, `base->btc`, `btc->eth`, `btc->base`, plus randomized mode.
- Requested quotes on a cadence, with randomized amounts between configured min/max bounds.
- Submitted swap creation requests to the OTC server and polled swap status until completion or timeout.
- Could pre-fund and isolate dedicated wallets so concurrent swaps did not contend on the same funding account.
- Could batch outbound market-maker payments on Bitcoin, Ethereum, and Base using per-chain batch timers.
- Consumed `.env` configuration for RPC endpoints, wallet material, API base URLs, intervals, timeouts, and batching settings.

If we recreate this for `tee-router`, the useful shape to preserve is:

- a scenario runner that can create many vaults/intents on a schedule
- pluggable funding backends per chain family
- optional dedicated funding wallets for isolation
- a live progress UI
- timeout/error accounting and summary metrics

The deleted implementation depended directly on the old `otc-server`, `rfq-server`, and `market-maker` crates, so it was removed instead of migrated in place.
