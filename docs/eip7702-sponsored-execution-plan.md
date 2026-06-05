# EVM Paymaster: Sponsored-Execution Plan (EIP-7702 / ERC-7710)

**Status:** Design / deferred. Not started. This captures a design discussion so the
direction isn't lost; it is an *endgame* improvement to the EVM gas model, intentionally
deprioritized in favor of shipping + codebase clarity.

**Author context:** discussion between operator and Claude, 2026-06.

---

## 1. The problem with today's model

Today's EVM gas model is **fund-then-act**: the paymaster sends ETH to a custody vault
(EOA), waits for confirmation, and only then can the vault sign + send its own action tx.
Current code already batches the *funding* side via EIP-7702 (the paymaster is the
delegated batching account — see `crates/eip7702-paymaster/`), but the fundamental shape
is still fund → confirm → act.

Downsides:

1. **Latency.** Two confirmation rounds on the critical path (fund, then act). Often more,
   because we wait several confirmations on the funding before acting for safety. Worst on
   Ethereum mainnet (~12s/block); milder but still 2× the confirmations on Base/Arbitrum.
2. **Gas waste.** Every plain ETH transfer costs the 21,000-gas intrinsic. A multi-step
   vault (approve → swap → transfer-out) pays that repeatedly, plus repeated funding
   top-ups if estimates are off.
3. **Multi-action multiplication.** A vault that must do several actions back-to-back means
   several funding txns and several action txns.

## 2. The hard constraint (do not violate)

> **Every action routes through a distinct blockchain address. Do NOT collapse multiple
> actions onto a single shared address.**

This is the binding requirement. It exists for isolation/auditability of custody. It
**rules out** the "easy" efficiency wins:

- ❌ One shared hot wallet for everything.
- ❌ A shared executor contract that pulls funds via Permit2/permit and swaps — because then
  the action's `msg.sender` is the *executor* (one shared address), not the vault.

So whatever we do, **the action must execute in the vault's own address context**
(`msg.sender == the vault`). That single requirement is what selects EIP-7702 as the answer.

**Granularity to confirm:** "distinct address per *action-step*" vs per *order*. Working
interpretation: don't collapse distinct steps onto a shared hot wallet; a single vault
batching its *own* sub-ops (approve + swap + transfer-out) on its *own* address is fine.

## 3. The design space + tradeoffs

Two regimes, then (within the sponsored regime) a choice of authorization mechanism.

### Regime I — keep funding, optimize it
- **I-a. Current (7702 batched funding).** Already built. 2 confs; every action still pays
  21k intrinsic; multi-step multiplies.
- **I-b. Pre-fund vaults at derivation.** Removes fund-wait from the critical path, but
  speculative (stranded ETH to recover) and still per-action funding.

### Regime II — sponsored execution (no ETH ever touches the vault)
The vault delegates (EIP-7702), signs its action batch off-chain, the paymaster submits +
pays gas. **One tx, one confirmation, batched.** All three sub-options preserve the
distinct-address rule (actions run as the vault). They differ only in *how the sponsor is
authorized*:

- **II-a. Minimal custom sig-gated delegator.** ~40-line `execute(calls, sig)`: vault signs
  the batch, paymaster lands it directly. Cheapest (~12.5–25k auth + action gas), 1 conf,
  **zero infra dependencies**, full control. Cost: new contract to audit.
- **II-b. Existing MetaMask DeleGator via ERC-4337.** Vault signs a UserOp → EntryPoint
  validates → a Paymaster contract sponsors → EntryPoint calls `execute`. Audited + standard
  tooling, but heaviest gas (EntryPoint + validation overhead, ~+80k/op) and needs an
  EntryPoint singleton + Paymaster contract + a bundler.
- **II-c. Existing MetaMask DeleGator via ERC-7710 delegations.** Vault signs a Delegation
  (with caveats) → paymaster calls `redeemDelegations(...)` directly. Audited, **caveats**
  give fine-grained on-chain permissioning, no bundler, lower gas than 4337. Needs the
  DelegationManager singleton + CaveatEnforcers + DeleGator init under 7702.

### Regime III — full 4337 without 7702
- **III. A deployed smart-contract account per vault.** A contract deployment per distinct
  address → strictly worse for one-shot vaults. Only relevant pre-Pectra. Skip (Eth/Base/Arb
  all support 7702 now).

### Comparison

| Direction | Latency | Gas / action | New code | Extra infra | Multi-action |
|---|---|---|---|---|---|
| I-a current | **2 confs** | fund(~11k) + 21k + action | none | none | separate txs |
| I-b pre-fund | ~1 conf | 21k + 21k + action | small | none | separate txs |
| **II-a custom 7702** | **1 conf** | **~25k + action** | ~40-line contract | **none** | **batched, 1 tx** |
| II-b DeleGator/4337 | 1 conf (+bundler) | ~25k + **80k+** + action | none (audited) | EntryPoint + Paymaster + bundler | batched UserOp |
| II-c DeleGator/7710 | 1 conf | ~25k + ~30k + action | none (audited) | DelegationManager + enforcers | batched, 1 tx |
| III 4337-per-vault | 1 conf | **huge (deploy)** | none | EntryPoint + factory | batched |

### Orthogonal refinements (layer onto any sponsored path)
- **Token reimbursement.** Include a leg that repays the paymaster in the *source token*
  (USDC) from the order value → paymaster ETH float is self-sustaining. Adds pricing logic.
- **Cross-vault batching.** The paymaster (itself 7702-delegated to a multicall) lands
  `vault1.execute + vault2.execute + …` in one tx, amortizing the 21k intrinsic across a
  settlement wave.

### II-b vs II-c — the practical difference
- **4337** = a sponsorship *protocol* with off-chain infra (alt-mempool → bundler →
  EntryPoint → Paymaster-contract-with-staked-deposit). Heaviest, most standardized, best for
  third-party ecosystem interop.
- **7710** = the paymaster sends a *plain tx* that redeems a vault-signed permission. No
  bundler, lower gas, immediate submission. **Caveats** are its superpower: a delegation can
  be scoped on-chain ("only call the Velora router, only up to amount X, only this token,
  only once"), enforced at redemption. Even a compromised paymaster key could only do the
  caveated action.
- For a self-operated router (we run both paymaster and vaults, want immediate submission +
  tight safety), **II-c is the better fit.**

## 4. Recommended path

1. **Prove II-a on Anvil first** (minimal custom delegator). It isolates the whole concept —
   "does sponsored batched execution from a distinct vault actually work?" — without dragging
   in EntryPoint/DelegationManager. Cheapest, simplest, dependency-free.
2. **Productionize as II-c** (7710 + caveats) if/when audited contracts + caveat safety are
   wanted. The signing/redemption *shape* from II-a maps almost 1:1 onto `redeemDelegations`.

II-b (4337) only if specifically targeting bundler/ecosystem compatibility — a self-operated
router doesn't need it.

## 5. How ERC-7710 actually works (the II-c flow)

Grounded in our actual delegator: `crates/eip7702-delegator-contract/` is the **MetaMask
Delegation Framework DeleGator** — a full smart account: ERC-4337 (`entryPoint`,
`validateUserOp`), ERC-7579 (`execute`, `executeFromExecutor`, `supportsExecutionMode`),
ERC-7710 (`redeemDelegations`, `enableDelegation`), ERC-1271 (`isValidSignature`).

### The cast
- **Vault** = custody EOA whose code is set (via EIP-7702) to the DeleGator. The *delegator*.
- **Paymaster** = the *delegate* and gas payer.
- **DelegationManager** = singleton the DeleGator trusts (`delegationManager() → address`).
  Processes redemptions and calls back into the vault.
- **CaveatEnforcers** = small contracts each enforcing one restriction.

### The delegation struct (from the ABI)
```
Delegation = (
  address delegate,     // the paymaster
  address delegator,    // the vault
  bytes32 authority,    // ROOT_AUTHORITY for a top-level grant
  Caveat[] caveats,     // the restrictions
  uint256 salt,
  bytes   signature     // the vault's signature over the above
)
Caveat = (address enforcer, bytes terms, bytes args)
```

### How the delegation is "given" — the key insight
It is **NOT a transaction** in the normal path. The vault **signs the Delegation struct
off-chain** (EIP-712, free). The paymaster **carries that signed blob and passes it as a
function argument** at redemption:

```
DelegationManager.redeemDelegations(
  bytes[]   _permissionContexts,  // = abi.encode([ signed Delegation ])  ← "given" here
  bytes32[] _modes,               // = [ BATCH ]
  bytes[]   _executionCallDatas   // = [ abi-encoded (target,value,calldata)[] ]
)
```

So "give the delegation to the contract" = vault signs off-chain → paymaster presents the
signed bytes at execution time. No prior on-chain step, no gas to create it.
*(Alternative: `enableDelegation(Delegation)` writes it to storage on-chain — costs gas; only
for delegators that can't ECDSA-sign, or when you want explicit on-chain revocation via
`disableDelegation`.)*

### End-to-end
```
OFF-CHAIN (router, holds vault key)            ON-CHAIN (paymaster submits, pays gas)
1. build Delegation{delegate=paymaster,
   delegator=vault, ROOT_AUTHORITY,
   caveats=[AllowedTarget(Velora),
            ValueLte(x), ...], salt}
2. vault key signs it (EIP-712)  ─────────▶  3. paymaster sends ONE tx:
                                                redeemDelegations([sigDelegation],[BATCH],[actions])
                                                 │ inside DelegationManager:
                                                 a. verify vault's signature (isValidSignature/ECDSA)
                                                 b. require(caller == delegate)
                                                 c. run each Caveat enforcer.beforeHook(actions)
                                                 d. vault.executeFromExecutor(BATCH, actions)
                                                       └─ approve+swap+transfer run AS the vault
                                                 e. caveat afterHooks
```

## 6. Why no prior ERC-20 approval is needed (the conceptual crux)

The common confusion: "how can we authorize the DelegationManager to move the vault's assets
without a prior `approve`?" Answer: **it doesn't move them — it triggers the vault to move
its own.**

- The `approve → transferFrom` model has a *spender* pull your tokens (`msg.sender == spender`),
  which requires prior approval.
- Here, the batched actions execute **as the vault** (`msg.sender == vault`), so it's just the
  vault calling `transfer` on its *own* balance. The owner moving its own money needs no
  approval. The DelegationManager/paymaster never touch the assets — they're a *trigger*.

**Why a third party can trigger an EOA to act as itself — that's exactly what EIP-7702 added:**
- Pre-7702: an EOA can only do what its key signs, one tx at a time; no code; no way for
  anyone else to make it run logic. *That's precisely why `approve`/`transferFrom` had to exist.*
- With 7702: the EOA sets its code to the DeleGator (signed authorization) → it's now a smart
  account → a trusted caller (the DelegationManager) can invoke its logic → it executes as itself.

The authorization is therefore **two off-chain signatures, not an on-chain grant**:
1. The **7702 authorization** — "my code is the DeleGator" (implicitly trusts the DeleGator's
   hard-coded DelegationManager).
2. The **delegation** — "paymaster may trigger me to run this caveated batch."

If a swap needs an allowance, that `approve(router)` is just the **first action inside the
batch** (the vault approving as itself, atomically with the swap) — never a separate prior tx,
and it never lingers.

## 7. Where this plugs into the current codebase

Current paymaster surface (~2,400 LOC total, ~1,900 in one crate):

| Layer | File | Role |
|---|---|---|
| Engine | `crates/eip7702-paymaster/src/actor.rs` (733) | actor loop `run_paymaster_actor` + serial fallback |
| | `…/batched_tx.rs` (605) | build + submit batched delegator `Execution` txs |
| | `…/delegation.rs` (287) | 7702 delegation setup (sign `Authorization`) |
| | `…/{metrics,error,config,lib}.rs` (238) | metrics, errors, config, exports |
| Contract | `crates/eip7702-delegator-contract/` (31 + abi/bytecode) | MetaMask DeleGator bindings (contract is external) |
| Driver | `crates/chains/src/evm.rs` (~300-LOC slice) | `EvmChain` owns `gas_sponsor`, spawns the actor, routes funded txs |
| Integration | `crates/router-core/.../custody_action_executor.rs` (~150) | `PaymasterRegistry` + release-sweep back to paymaster |
| Wiring | `bin/router-server/src/app.rs`, `bin/temporal-worker/src/production.rs` (~100) | per-chain key plumbing, `gas_sponsor_config()` |

**Integration seam:** the engine sits behind `EvmChain.gas_sponsor` (`crates/chains/src/evm.rs`,
fund call sites ~:652 / :701, actor spawn ~:1289). A move from fund-then-act to
sponsored-execution swaps in **behind that seam** — the router/worker/order logic above it is
untouched. That isolation is what makes this a contained, later-stage change.

## 8. Open questions to resolve before building

1. **DeleGator init under 7702.** The DeleGator keeps owner/config in storage; a fresh 7702
   EOA needs that set (vault's own key as ECDSA owner) — either auto-derived by the impl or a
   one-time init folded into the first tx. Verify against this specific DeleGator version.
2. **Exact `redeemDelegations` entry point** — DeleGator passthrough vs the DelegationManager
   singleton; and which DelegationManager/enforcers are deployed on Eth/Base/Arb.
3. **Constraint granularity** (per action-step vs per order) — see §2.
4. **Sequential cross-vault steps** — when action B needs action A's on-chain result, it can't
   be one tx; each becomes a single sponsored tx (still no separate fund step). Batch what's
   independent; make the delegator compute dynamic values (e.g. "transfer whatever balance")
   where possible.
5. **Anvil 7702 support** — spawn with the Prague hardfork for the POC.

## 9. POC sketch (when we pick this back up — II-a)

A self-contained scratch crate (deleted for now) using the devnet's `node_bindings::Anvil`:
1. Spawn Anvil (Prague). Accounts: paymaster (has ETH), fresh vault (holds a mock ERC-20, no ETH).
2. Deploy a ~40-line sig-gated delegator + a mock ERC-20.
3. Vault signs a batch `[transfer(A, 600), transfer(B, 400)]` off-chain.
4. Paymaster sends ONE type-4 tx: vault's 7702 authorization + `execute(batch, sig)`.
5. Assert: A=600, B=400, vault token balance 0, **vault ETH still 0**, paymaster paid the gas.

That single assertion — balances moved, no allowance ever set, paymaster paid — is the proof
of the whole model.
