# `live-test-logs/` — funded test run records

Every test in this repo that **spends real money** (i.e. submits a real
transaction on mainnet/L2/HL/Bitcoin/etc.) writes a self-contained record
of what happened into a timestamped subdirectory here. These records are
committed to the repo so a future investigator can reconstruct what ran,
what state was touched, and what the result was — without re-running
anything.

## Folder layout

```
live-test-logs/
└── 2026-05-23T14-30-00Z--cctp-arb-to-base-1usdc/
    ├── summary.md           # high-level: scenario, result, gas, tx hashes, durations
    ├── run.log              # full stdout/stderr from the test process, timestamped lines
    ├── pre-balances.json    # wallet ETH + token balances on every chain touched, before
    ├── post-balances.json   # … after
    ├── burn-tx.json         # full on-chain receipt for the source-side tx
    ├── iris-poll-000.json   # first Iris attestation poll
    ├── iris-poll-001.json   # second poll
    ├── …                    # one file per poll until status=complete
    ├── attestation-complete.json  # final Iris response carrying the attestation bytes
    └── receive-tx.json      # full on-chain receipt for the destination-side tx
```

Folder name convention: `<utc-timestamp>--<short-scenario-name>` where the
timestamp is `YYYY-MM-DDTHH-MM-SSZ` (colons replaced with dashes to play
nice on case-insensitive filesystems).

## What must be captured

At minimum:
- Wallet balances on every chain touched, before and after.
- Every on-chain receipt as raw JSON, one file per tx.
- Every off-chain API response (Iris, REST APIs, etc.) during the run,
  one file per response, in chronological order.
- A `summary.md` written last with: scenario, result, total gas spent
  per chain, total wall time, every tx hash with explorer link, anything
  surprising.
- A `run.log` with timestamped lines from the test process.

## Why

Real-money tests are the only fully-honest validation of venue code. They
also need to be auditable years later — what burned, on what tx, against
what Iris response, with what attestation. Memory and intuition decay;
files don't.

## Running funded tests

Funded tests are gated by `FUNDED_SUBFLOW_TESTS=1` so they don't run by
default. To run one:

```bash
FUNDED_SUBFLOW_TESTS=1 cargo test -p sauron --test funded_cctp_arb_to_base \
    -- --ignored --nocapture
```

The test writes its own subfolder under `live-test-logs/` and prints the
path on completion.
