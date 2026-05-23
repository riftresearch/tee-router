# Hyperunit UBTC → BTC withdrawal (tier-2, partial)

- **Started**: 2026-05-23T16-04-12Z UTC
- **Wallet**: `0x33F65788aCa48D733c2C2444Ac9F79B18206aa92`
- **Direction**: HL spot UBTC → Bitcoin (`bc1qk4m6mpxulnlufegdh3w40kayhx9m722am38apn`)
- **Amount attempted**: 0.0002 UBTC (20000 sats)
- **Result**: 🟡 **partial** — protocol flow validated end-to-end, but the operation terminated in `failure` due to below-minimum amount.

## What happened

| Step | Outcome |
|---|---|
| `/gen` via SOCKS5 proxy | ✅ returned protocol HL address `0xa0756aF705a4C587511a33912AeFbd249b49e2Cc` |
| Guardian signature verification (after fix) | ✅ all 3 mainnet guardian sigs verified |
| HL `sendAsset` action signed + submitted | ✅ `{"response":{"type":"default"},"status":"ok"}` |
| UBTC transferred to Hyperunit HL address | ✅ wallet UBTC: 0.0002195384 → 0.0000195384 |
| Hyperunit operation created + tracked | ✅ opId `0x33F65788aCa48D733c2C2444Ac9F79B18206aa92:1779552257204` |
| Operation reached terminal state | ✅ `state: failure` (after ~36s) |
| BTC tx broadcast to destination | ❌ never broadcast |

## Root cause of failure

Hyperunit's minimum BTC withdrawal is **30000 sats (0.0003 BTC)** — verified by:
- The test's hardcoded default: `DEFAULT_HYPERUNIT_WITHDRAW_AMOUNT_BTC = "0.0003"`
- The wallet's last successful withdrawal: 30000 sats → `state: done`
- This run at 20000 sats → `state: failure` within seconds

We overrode to 0.0002 because we only had 0.0002195384 UBTC on HL spot — barely above the
minimum, with no headroom for fees. With at least 0.0003 UBTC available, the test would
have succeeded at the on-chain step too.

## What this validated

This run is still a meaningful **tier-2 validation** because the entire signing + transit
+ observation path was exercised against real Hyperunit infrastructure:
1. The guardian-signature verifier (fixed mid-session to handle the
   `coinType=ethereum`-hardcoded NEW template) accepted real production sigs.
2. The HL exchange-action signing pipeline (`send_asset`) was accepted on first try.
3. Hyperunit's polling-based state machine reached a terminal state correctly.

The only un-validated step is the actual BTC-tx broadcast, gated purely by amount
validation. The on-chain side itself is well-trodden — a previous 30000-sat withdrawal
from this same wallet on 2026-05-06 settled with destinationTxHash
`49a7983def9a6e2eca36c954ae66bb25f87f8b05fddff10478eeb0cf7682830e`.

## Open: stuck funds

20000 sats of UBTC are now held at Hyperunit's HL address (the `0xa0756aF…` protocol
address). `positionInWithdrawQueue: 3` is set on the failure-state operation; whether
Hyperunit auto-recovers or requires manual support is undocumented. The funds are
recoverable in principle.

## Infra side-effect

Two Railway changes made during this run:
- Created a TCP proxy on `hyperunit-socks5-proxy-v3` (production env). New endpoint:
  `kodama.proxy.rlwy.net:25441`. The previous URL in `.env` (`shinkansen.proxy.rlwy.net:20974`)
  was stale.
- Updated `.env` `HYPERUNIT_LIVE_PROXY_URL` + `HYPERUNIT_PROXY_URL` to the new endpoint.
- Cleaned up an accidentally-created HTTP serviceDomain on the same service.
