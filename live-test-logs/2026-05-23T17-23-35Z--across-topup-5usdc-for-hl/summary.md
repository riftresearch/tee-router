# Across top-up: 5 USDC Base → Arbitrum (pre-flight for HL bridge deposit)

Same test as the closing Across tier-2 (`live_across_swap_lifecycle_transcript_spends_funds`),
re-run with `ACROSS_LIVE_AMOUNT=5000000` to top the wallet's Arb USDC
above HL's 5-USDC bridge minimum. **Not a venue closeout in itself** —
purely the funding step for the Hyperliquid bridge-deposit tier-2.

- **Deposit tx (Base)**: ``
- **Wall time**: 7.82s
- **Arb USDC delivered**: ~4.99 (matches `expectedOutputAmount` after Across's bridge fee)

See sibling dir `2026-05-23T17-24-03Z--hyperliquid-bridge-deposit-arb-to-hl/`
for the HL closeout this funding enables.
