# TODO

## Devnet Anvil temp directory cleanup validation

The runner now scopes Anvil dump state and fork RPC cache data to devnet-owned temporary directories under `~/.cache/rift-devnet/tmp/`, while preserving explicit cached devnet state under `~/.cache/rift-devnet/`.

Remaining validation:

- Run `devnet-cli server` with a fork URL and watch `~/.foundry` before and after clean shutdown.
- Run `devnet-cli cache` to confirm explicitly cached devnet mode still works end to end.
