#!/usr/bin/env python3
"""Vendor a verified-contract ABI JSON from Etherscan v2 into vendor/<venue>/.

Resolves proxies to their implementation automatically. Pulls the
ETHERSCAN_API_KEY from the worktree's `.env` (single key works across all
EVM chains via the v2 multi-chain endpoint).

Usage:
    scripts/vendor-abi.py --chain 1 --address 0x... --out vendor/cctp/Foo.abi.json

Re-run any time to refresh; the file is overwritten with a freshly-pulled
ABI plus a small provenance header (chain, address, impl, fetched_at).
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

API_BASE = "https://api.etherscan.io/v2/api"
TIMEOUT_SECS = 20
# Etherscan free tier is 3 req/sec; stay well under it.
INTER_REQUEST_SLEEP_SECS = 0.4


def _load_env_key() -> str:
    key = os.environ.get("ETHERSCAN_API_KEY")
    if key:
        return key.strip()
    env_path = pathlib.Path(__file__).resolve().parent.parent / ".env"
    if env_path.is_file():
        for line in env_path.read_text().splitlines():
            if line.startswith("ETHERSCAN_API_KEY="):
                return line.split("=", 1)[1].strip().strip("\"'")
    sys.exit("ETHERSCAN_API_KEY not set in env or .env")


def _get(params: dict) -> dict:
    url = f"{API_BASE}?{urllib.parse.urlencode(params)}"
    last_err = None
    for attempt in range(4):
        with urllib.request.urlopen(url, timeout=TIMEOUT_SECS) as r:
            d = json.load(r)
        # Etherscan reports rate-limit hits with status=0 and a textual result.
        result = d.get("result")
        if d.get("status") == "0" and isinstance(result, str) and "rate limit" in result.lower():
            time.sleep(1.0 + attempt * 0.5)
            last_err = result
            continue
        time.sleep(INTER_REQUEST_SLEEP_SECS)
        return d
    sys.exit(f"persistent rate-limit from etherscan: {last_err}")


def _resolve_impl(chain: int, address: str, key: str) -> str:
    """If `address` is a proxy, return the implementation address; else the same address."""
    d = _get({
        "chainid": chain,
        "module": "contract",
        "action": "getsourcecode",
        "address": address,
        "apikey": key,
    })
    items = d.get("result", [])
    if not items or not isinstance(items[0], dict):
        sys.exit(f"getsourcecode failed for {address}: {d}")
    it = items[0]
    if it.get("Proxy") == "1":
        impl = it.get("Implementation")
        if not impl:
            sys.exit(f"{address} reports Proxy=1 but has no Implementation field")
        return impl
    return address


def _fetch_abi(chain: int, address: str, key: str) -> list:
    d = _get({
        "chainid": chain,
        "module": "contract",
        "action": "getabi",
        "address": address,
        "apikey": key,
    })
    if d.get("status") != "1":
        sys.exit(f"getabi failed for {address}: status={d.get('status')} msg={d.get('message')} result={d.get('result')}")
    return json.loads(d["result"])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--chain", type=int, required=True, help="Etherscan v2 chain id (1=Eth, 8453=Base, 42161=Arb, ...)")
    ap.add_argument("--address", required=True, help="contract address (proxy or impl)")
    ap.add_argument("--out", required=True, help="output path under vendor/")
    ap.add_argument("--no-resolve-proxy", action="store_true", help="skip proxy → impl resolution")
    args = ap.parse_args()

    key = _load_env_key()
    address = args.address.lower()
    impl = address if args.no_resolve_proxy else _resolve_impl(args.chain, address, key).lower()
    abi = _fetch_abi(args.chain, impl, key)

    # Provenance header keyed under a synthetic ABI entry so the file remains
    # a valid ABI JSON for tools that don't read the header.
    payload = {
        "_vendor_provenance": {
            "etherscan_chain_id": args.chain,
            "queried_address": address,
            "implementation_address": impl,
            "is_proxy": impl != address,
            "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "source": "https://api.etherscan.io/v2/api?module=contract&action=getabi",
        },
        "abi": abi,
    }

    out = pathlib.Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    # Write raw ABI array so `alloy::sol!(Name, "path.abi.json")` consumes it
    # directly. Provenance lives in a sibling PROVENANCE.md per venue dir.
    out.write_text(json.dumps(abi, indent=2) + "\n")
    _update_provenance(out, payload["_vendor_provenance"])

    events = sum(1 for x in abi if x.get("type") == "event")
    funcs = sum(1 for x in abi if x.get("type") == "function")
    print(f"vendored {out}")
    print(f"  chain={args.chain} address={address} impl={impl} (proxy={impl != address})")
    print(f"  total={len(abi)} events={events} functions={funcs}")


def _update_provenance(abi_path: pathlib.Path, provenance: dict) -> None:
    """Append/replace a row in `<venue-dir>/PROVENANCE.md` for this ABI file."""
    venue_dir = abi_path.parent
    pmd = venue_dir / "PROVENANCE.md"

    # Build the row first; we'll either add it or replace any existing row keyed by file name.
    row = (
        f"| `{abi_path.name}` "
        f"| {provenance['etherscan_chain_id']} "
        f"| `{provenance['queried_address']}` "
        f"| `{provenance['implementation_address']}` "
        f"| {'yes' if provenance['is_proxy'] else 'no'} "
        f"| {provenance['fetched_at']} |"
    )
    header = (
        "# Provenance — vendored ABIs in this directory\n\n"
        "Source: Etherscan v2 verified-contract ABIs (proxy auto-resolved to impl).\n"
        "Refresh: `./scripts/vendor-abi.py --chain <id> --address <0x..> --out <path>`.\n\n"
        "| ABI file | chain id | queried address | impl address | is proxy | fetched (UTC) |\n"
        "|---|---|---|---|---|---|\n"
    )
    existing_rows = []
    if pmd.exists():
        for line in pmd.read_text().splitlines():
            if line.startswith(f"| `{abi_path.name}` "):
                continue  # drop previous row for this file
            if line.startswith("| `") and "|" in line:
                existing_rows.append(line)
    existing_rows.append(row)
    existing_rows.sort()
    pmd.write_text(header + "\n".join(existing_rows) + "\n")


if __name__ == "__main__":
    main()
