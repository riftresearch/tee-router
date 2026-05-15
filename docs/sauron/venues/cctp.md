# CCTP (Circle's Cross-Chain Transfer Protocol) — Efficient Observation Strategy

CCTP is Circle's burn-and-mint USDC bridge. We use it for `cctp_burn` (source-chain leg) and `cctp_receive` (destination-chain leg). Three observation phases, two of which are pure on-chain and one of which is necessarily off-chain (Circle's attestation API).

## What we observe

| Phase | Where | Event / API | Why |
|---|---|---|---|
| Burn | Source chain | `MessageSent(message)` from MessageTransmitter | USDC was burned on source chain; message is the auth payload for minting on destination |
| Attestation | Circle's REST API | `GET /v2/messages/{srcDomain}?transactionHash=...` returns `attestation` field | Circle's signers have signed off; we need the attestation to relay to the destination |
| Receive | Destination chain | `MessageReceived(message, ...)` from MessageTransmitter | Mint completed on destination |

## Native push mechanisms

### On-chain (source + destination)

Both `MessageSent` (source) and `MessageReceived` (destination) are observable via the EVM chain backend's WebSocket log subscription (see `chains/{ethereum,base,arbitrum}.md`). The relevant contract is Circle's `MessageTransmitter` on each supported chain.

### Circle attestation API

**No WebSocket / push mechanism exists.** Circle exposes a REST API at `https://iris-api.circle.com` (mainnet) / `https://iris-api-sandbox.circle.com` (testnet). The endpoint:

```
GET /v2/messages/{sourceDomain}?transactionHash=<hash>
```

returns an attestation object with `status: "complete"` once the signers have signed. Polling this is *the only way* to learn when the attestation is ready. **This is acceptable — it lives at the Sauron level, never in T-router's hotpath.**

Typical attestation latency:
- L1 → L1: 13–20 minutes (hard-finality wait on Ethereum)
- L2 → L2 / L2 → L1: 8–20 minutes
- L1 → L2 fast: 5–10 minutes (CCTP V2's faster finality option, where supported)

This is dominated by source-chain finality, not by Circle's signing latency. Polling once every 30s after burn is sufficient — anything faster is wasted requests.

## Pull / fallback mechanisms

| Mechanism | Use case |
|---|---|
| `eth_getLogs` on `MessageSent` / `MessageReceived` | Backfill on chain backend reconnect |
| `eth_getTransactionReceipt(burn_tx_hash)` | Definitive burn confirmation when we know our submission tx hash |
| Circle attestation REST polling | The only way to observe attestation availability — no alternative |

## Recommended efficient design

### Architecture

A new `bin/sauron/src/discovery/cctp.rs` backend that handles only the Circle attestation polling, plus chain-level enhancements for the on-chain legs:

1. **Source/destination on-chain observation** is delegated to the EVM chain backend, with two enhancements:
   - Add `MessageTransmitter` contract addresses (per chain) to the EVM backend's watch set.
   - Emit chain hints with venue context: `CctpBurnObserved` (from `MessageSent`), `CctpReceiveObserved` (from `MessageReceived`).
2. **CCTP backend** (`discovery/cctp.rs`) polls Circle's API for attestations:
   - On `CctpBurnObserved` hint, register the `messageHash` for attestation polling.
   - Poll `GET /v2/messages/{srcDomain}?transactionHash=<burn_tx_hash>` every 30s (configurable; consider longer windows after the first 5 minutes since Ethereum-source attestations take 13–20 min).
   - On `status: "complete"`, emit `DetectorHint { hint_kind: CctpAttestationReady, attestation }`.
   - Stop polling once attestation is delivered or after a hard timeout (e.g., 1 hour — anything beyond this is a real failure).

### Observation flow

1. T-router submits `depositForBurn` on source chain. Persists `provider_ref = burn_tx_hash`.
2. **Burn observed** (chain backend WS): `MessageSent` log appears in the burn tx → emit `DetectorHint { hint_kind: CctpBurnObserved, message_hash, burn_tx_hash, source_domain }`. T-router persists the `message_hash` and waits for attestation.
3. **Attestation polled** (CCTP backend): polling Circle's API every 30s. Once `status = complete`, emit `DetectorHint { hint_kind: CctpAttestationReady, message_hash, attestation }`.
4. **Mint submitted** (T-router action): with the attestation in hand, T-router calls `receiveMessage(message, attestation)` on the destination chain.
5. **Receive observed** (chain backend WS): `MessageReceived` log on destination → emit `DetectorHint { hint_kind: CctpReceiveObserved, mint_tx_hash, destination_domain, recipient, amount }`. Step complete.

### Polling cadence specifics

- 0–5 min after burn: skip polling (no chance of attestation yet for L1 sources).
- 5–10 min: poll every 60s.
- 10–25 min: poll every 30s (peak window for attestation).
- 25–60 min: poll every 60s (degraded window).
- > 60 min: alert + escalate to MIR.

For L2-source bridges (faster finality), shrink each window proportionally (e.g., start polling at 1 min for Arbitrum-source).

## Step-ID disambiguation

CCTP `messageHash` (computed deterministically from message contents) is globally unique per CCTP message. Use it as the strongest disambiguator. The watch entry must carry `(message_hash, execution_step_id)` — when the attestation arrives or the destination mint observes, the hint is tagged with the step ID directly via the message-hash → step-ID mapping in the watch store.

For chain-side events (`MessageSent` and `MessageReceived`), match on `messageHash` topic in the event, not on token addresses or recipients. This is more reliable than amount matching and never confuses parallel CCTP operations.

## Failure modes and backstops

| Failure | Mitigation |
|---|---|
| Burn tx reverts | `eth_getTransactionReceipt(burn_tx_hash)` returns status=0; verifier emits failure; refund (no funds were burned) |
| Circle attestation API down | Sauron polling fails; mark backend degraded; alert. Polling resumes when API comes back. T-router orders waiting on attestation stall visibly |
| Circle attestation never arrives (hard failure, e.g., reorg of source tx) | Hard timeout at 1h triggers MIR. Operator manually inspects: if source tx reorged, route to refund; if Circle confirmed but our polling missed, manually fetch and retry mint |
| Destination mint reverts | Possible if attestation is replayed or `receiveMessage` rejected — should not happen in normal operation. If it does, refund flow on the source side is already complete (USDC was burned), so we have a stuck operation requiring manual intervention |
| Reorg on source chain after attestation issued | This is the worst case: Circle attested to a message that no longer exists. CCTP V2's fast-finality option mitigates this for L2 sources. For L1 sources, the 13–20 min wait is precisely Circle's solution. Reorg deeper than 32 blocks is essentially never |
| Reorg on destination chain after `MessageReceived` | Chain reorg path retracts hint; T-router re-evaluates. The attestation is reusable, so we can re-call `receiveMessage` — but in practice destination reorgs deep enough to retract are extraordinarily rare on L2s |

## What's missing today

1. **No CCTP backend in Sauron.** Attestation observation flows entirely through the generic `ProviderOperationObserverLoop`, which is a poke based on DB `updated_at` and has zero visibility into Circle's API. This is one of the three "missing venue backend" gaps from `audit.md` §E (alongside Hyperliquid and explicit Across wiring).
2. **`MessageTransmitter` contract addresses are not explicitly in the EVM backend's watch set.** Today, T-router observes the destination via the recipient `Transfer` event (which works) but the source `MessageSent` is invisible to Sauron — T-router has to know the burn happened from its own submission receipt rather than from a Sauron hint.
3. **No `Cctp*` hint kinds** (`CctpBurnObserved`, `CctpAttestationReady`, `CctpReceiveObserved`). Generic `PossibleProgress` only.
4. **No attestation cache.** Once an attestation is fetched, it's reusable for the lifetime of the message. We should cache attestations per `messageHash` so retries don't re-poll.

## Out of scope

- CCTP V1 vs V2 dispatch logic (both are supported by the same architecture; T-router decides which to use per-route based on chain support).
- Circle's other products (Web3 Services, Programmable Wallets) — we use only CCTP.
- Direct attestation relay (we don't run an attestation relayer; we just consume Circle's signed attestations).
