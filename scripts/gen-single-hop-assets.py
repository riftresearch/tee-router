#!/usr/bin/env python3
"""Generate single-hop venue asset map Rust specs from venue APIs.

The generated files are intentionally checked in. Runtime quote code uses static
Rust specs, while this script refreshes those specs from venue catalogs / chain
metadata when we deliberately choose to update them.

Usage:
    scripts/gen-single-hop-assets.py
    scripts/gen-single-hop-assets.py --venue garden --garden-api-key "$GARDEN_API_KEY"
    scripts/gen-single-hop-assets.py --check
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
import tempfile
from dataclasses import dataclass
from typing import Any, Callable, Iterable

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
OUT_ROOT = REPO_ROOT / "crates/router-core/src/services/single_hop_assets"
TIMEOUT_SECS = 30
USER_AGENT = "tee-router-single-hop-asset-generator/1.0"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

ROUTER_CHAINS = {
    "ethereum": "evm:1",
    "arbitrum": "evm:42161",
    "base": "evm:8453",
    "bitcoin": "bitcoin",
}

EVM_ROUTER_CHAIN_TO_ID = {
    "evm:1": 1,
    "evm:42161": 42161,
    "evm:8453": 8453,
}

EVM_ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")


@dataclass(frozen=True)
class AssetEntry:
    router_asset: str
    venue_asset: str


@dataclass(frozen=True)
class ChainSpec:
    const_name: str
    router_chain: str
    venue_chain: str
    policy: str
    assets: tuple[AssetEntry, ...]


@dataclass(frozen=True)
class VenueSpec:
    provider_variant: str
    const_name: str
    chains_const: str
    chains: tuple[ChainSpec, ...]
    imports: tuple[str, ...]
    policy_consts: tuple[str, ...] = ()
    header_comments: tuple[str, ...] = ()


def rust_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def normalize_evm_address(value: str) -> str:
    raw = value.strip()
    if not EVM_ADDRESS_RE.match(raw):
        raise ValueError(f"expected 0x-prefixed EVM address, got {value!r}")
    return raw.lower()


def http_json(url: str, *, method: str = "GET", headers: dict[str, str] | None = None, body: dict[str, Any] | None = None) -> Any:
    merged_headers = {
        "accept": "application/json",
        "user-agent": USER_AGENT,
    }
    if headers:
        merged_headers.update(headers)
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        merged_headers.setdefault("content-type", "application/json")
    request = urllib.request.Request(url, data=data, headers=merged_headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECS) as response:
            payload = response.read()
    except urllib.error.HTTPError as err:
        preview = err.read(4096).decode("utf-8", "replace")
        raise SystemExit(f"{method} {url} failed: HTTP {err.code}: {preview}") from err
    except urllib.error.URLError as err:
        raise SystemExit(f"{method} {url} failed: {err}") from err
    try:
        return json.loads(payload)
    except json.JSONDecodeError as err:
        preview = payload[:4096].decode("utf-8", "replace")
        raise SystemExit(f"{method} {url} returned non-JSON: {preview}") from err


def sort_asset_entries(entries: Iterable[AssetEntry]) -> tuple[AssetEntry, ...]:
    def key(entry: AssetEntry) -> tuple[int, str]:
        return (0 if entry.router_asset == "Native" else 1, entry.router_asset)

    return tuple(sorted(entries, key=key))


def dedupe_entries(entries: Iterable[AssetEntry], venue: str, chain: str) -> tuple[AssetEntry, ...]:
    by_router: dict[str, AssetEntry] = {}
    by_venue: dict[str, AssetEntry] = {}
    for entry in entries:
        prior = by_router.get(entry.router_asset)
        if prior and prior.venue_asset != entry.venue_asset:
            raise SystemExit(
                f"{venue} {chain}: duplicate router asset {entry.router_asset}: "
                f"{prior.venue_asset} vs {entry.venue_asset}"
            )
        prior = by_venue.get(entry.venue_asset)
        if prior and prior.router_asset != entry.router_asset:
            raise SystemExit(
                f"{venue} {chain}: duplicate venue asset {entry.venue_asset}: "
                f"{prior.router_asset} vs {entry.router_asset}"
            )
        by_router[entry.router_asset] = entry
        by_venue[entry.venue_asset] = entry
    return sort_asset_entries(by_router.values())


def chain_const_name(venue: str, chain: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", chain).strip("_").upper()
    return f"{venue.upper()}_{normalized}_ASSETS"


def render_asset_entry(entry: AssetEntry) -> str:
    if entry.router_asset == "Native":
        router = "RouterAssetKind::Native"
    else:
        router = f"RouterAssetKind::Reference({rust_string(entry.router_asset)})"
    return (
        "    VenueAssetEntry {\n"
        f"        router_asset: {router},\n"
        f"        venue_asset: {rust_string(entry.venue_asset)},\n"
        "    },\n"
    )


def render_spec(spec: VenueSpec) -> str:
    lines: list[str] = [
        "// @generated by scripts/gen-single-hop-assets.py; do not edit by hand.",
    ]
    lines.extend(f"// {comment}" for comment in spec.header_comments)
    lines.extend(
        [
            "",
            "use crate::services::{",
            "    asset_registry::ProviderId,",
            "    single_hop_assets::{",
        ]
    )
    for item in spec.imports:
        lines.append(f"        {item},")
    lines.extend(["    },", "};", ""])
    for policy in spec.policy_consts:
        lines.extend(policy.rstrip().splitlines())
        lines.append("")
    for chain in spec.chains:
        lines.append(f"const {chain.const_name}: &[VenueAssetEntry] = &[")
        for entry in chain.assets:
            lines.extend(render_asset_entry(entry).rstrip().splitlines())
        lines.append("];")
        lines.append("")
    lines.append(f"const {spec.chains_const}: &[VenueChainAssetsSpec] = &[")
    for chain in spec.chains:
        lines.extend(
            [
                "    VenueChainAssetsSpec {",
                f"        router_chain: {rust_string(chain.router_chain)},",
                f"        venue_chain: {rust_string(chain.venue_chain)},",
                f"        asset_policy: {chain.policy},",
                f"        assets: {chain.const_name},",
                "    },",
            ]
        )
    lines.append("];")
    lines.append("")
    lines.extend(
        [
            f"pub const {spec.const_name}: VenueAssetMapSpec = VenueAssetMapSpec {{",
            f"    provider: ProviderId::{spec.provider_variant},",
            f"    chains: {spec.chains_const},",
            "};",
            "",
        ]
    )
    return "\n".join(lines)

def rustfmt_content(content: str) -> str:
    with tempfile.TemporaryDirectory(prefix="single-hop-assets-rustfmt-") as temp_dir:
        path = pathlib.Path(temp_dir) / "generated.rs"
        path.write_text(content)
        try:
            subprocess.run(
                ["rustfmt", "--edition", "2021", str(path)],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        except FileNotFoundError as err:
            raise SystemExit("rustfmt is required to generate single-hop asset specs") from err
        except subprocess.CalledProcessError as err:
            raise SystemExit(
                f"rustfmt failed for generated asset spec:\n{err.stderr}"
            ) from err
        return path.read_text()



def write_file(path: pathlib.Path, content: str, *, check: bool) -> bool:
    current = path.read_text() if path.exists() else None
    if current == content:
        return False
    if check:
        print(f"would update {path.relative_to(REPO_ROOT)}", file=sys.stderr)
        return True
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    print(f"wrote {path.relative_to(REPO_ROOT)}")
    return True


def module_file(const_name: str) -> str:
    return (
        "mod generated;\n\n"
        f"pub use generated::{const_name};\n"
    )


def generate_relay() -> VenueSpec:
    data = http_json("https://api.relay.link/chains")
    chains = data.get("chains")
    if not isinstance(chains, list):
        raise SystemExit("Relay /chains response missing chains array")
    relay_by_id = {str(chain.get("id")): chain for chain in chains if isinstance(chain, dict)}
    wanted = [
        ("evm:1", "1"),
        ("evm:42161", "42161"),
        ("evm:8453", "8453"),
        ("bitcoin", "8253038"),
    ]
    chain_specs: list[ChainSpec] = []
    for router_chain, relay_id in wanted:
        chain = relay_by_id.get(relay_id)
        if not chain:
            raise SystemExit(f"Relay chain id {relay_id} missing from /chains")
        currency = chain.get("currency") or {}
        native_address = str(currency.get("address") or "").strip()
        if not native_address:
            raise SystemExit(f"Relay chain id {relay_id} missing native currency address")
        if router_chain.startswith("evm:"):
            native_address = normalize_evm_address(native_address)
            token_support = chain.get("tokenSupport")
            policy = (
                "VenueAssetPolicy::ListedPlusOpenContracts(RELAY_EVM_CONTRACT_PASSTHROUGH)"
                if token_support == "All"
                else "VenueAssetPolicy::ListedOnly"
            )
        else:
            policy = "VenueAssetPolicy::ListedOnly"
        entries = [AssetEntry("Native", native_address)]
        if policy == "VenueAssetPolicy::ListedOnly" and router_chain.startswith("evm:"):
            for token in chain.get("erc20Currencies") or []:
                address = token.get("address")
                if isinstance(address, str) and EVM_ADDRESS_RE.match(address):
                    entries.append(AssetEntry(normalize_evm_address(address), normalize_evm_address(address)))
        chain_specs.append(
            ChainSpec(
                const_name=chain_const_name("relay", router_chain),
                router_chain=router_chain,
                venue_chain=relay_id,
                policy=policy,
                assets=dedupe_entries(entries, "relay", router_chain),
            )
        )
    return VenueSpec(
        provider_variant="Relay",
        const_name="RELAY_ASSET_MAP_SPEC",
        chains_const="RELAY_CHAINS",
        chains=tuple(chain_specs),
        imports=(
            "ContractEncoding",
            "ContractPassthrough",
            "ContractStandard",
            "RouterAssetKind",
            "VenueAssetEntry",
            "VenueAssetMapSpec",
            "VenueAssetPolicy",
            "VenueChainAssetsSpec",
        ),
        policy_consts=(
            "const RELAY_EVM_CONTRACT_PASSTHROUGH: ContractPassthrough = ContractPassthrough {\n"
            "    standard: ContractStandard::Erc20,\n"
            "    encoding: ContractEncoding::LowercaseHex,\n"
            "};",
        ),
        header_comments=("Source: https://api.relay.link/chains",),
    )


def near_router_entry(token: dict[str, Any]) -> tuple[str, str] | None:
    blockchain = token.get("blockchain")
    asset_id = token.get("assetId")
    if not isinstance(blockchain, str) or not isinstance(asset_id, str):
        return None
    contract = token.get("contractAddress")
    if blockchain == "btc":
        if asset_id == "nep141:btc.omft.near" and not contract:
            return ("Native", asset_id)
        return None
    native_ids = {
        "eth": "nep141:eth.omft.near",
        "arb": "nep141:arb.omft.near",
        "base": "nep141:base.omft.near",
    }
    if blockchain in native_ids and asset_id == native_ids[blockchain] and not contract:
        return ("Native", asset_id)
    if isinstance(contract, str) and EVM_ADDRESS_RE.match(contract):
        return (normalize_evm_address(contract), asset_id)
    match = re.search(r"(?:^|[:-])0x([0-9a-fA-F]{40})(?:\.|$)", asset_id)
    if match:
        return (f"0x{match.group(1).lower()}", asset_id)
    return None


def generate_near_intents() -> VenueSpec:
    data = http_json("https://1click.chaindefuser.com/v0/tokens")
    if not isinstance(data, list):
        raise SystemExit("NEAR Intents /v0/tokens response is not an array")
    venue_to_router = {
        "btc": "bitcoin",
        "eth": "evm:1",
        "arb": "evm:42161",
        "base": "evm:8453",
    }
    entries_by_chain: dict[str, list[AssetEntry]] = {chain: [] for chain in venue_to_router}
    for token in data:
        if not isinstance(token, dict):
            continue
        blockchain = token.get("blockchain")
        if blockchain not in entries_by_chain:
            continue
        mapped = near_router_entry(token)
        if mapped:
            entries_by_chain[blockchain].append(AssetEntry(mapped[0], mapped[1]))
    chain_specs = []
    for venue_chain, router_chain in venue_to_router.items():
        entries = dedupe_entries(entries_by_chain[venue_chain], "near_intents", router_chain)
        if not entries:
            raise SystemExit(f"NEAR Intents generated no assets for {router_chain}")
        chain_specs.append(
            ChainSpec(
                const_name=chain_const_name("near_intents", router_chain),
                router_chain=router_chain,
                venue_chain=venue_chain,
                policy="VenueAssetPolicy::ListedOnly",
                assets=entries,
            )
        )
    return VenueSpec(
        provider_variant="NearIntents",
        const_name="NEAR_INTENTS_ASSET_MAP_SPEC",
        chains_const="NEAR_INTENTS_CHAINS",
        chains=tuple(chain_specs),
        imports=(
            "RouterAssetKind",
            "VenueAssetEntry",
            "VenueAssetMapSpec",
            "VenueAssetPolicy",
            "VenueChainAssetsSpec",
        ),
        header_comments=("Source: https://1click.chaindefuser.com/v0/tokens",),
    )


def generate_mayan() -> VenueSpec:
    data = http_json("https://sia.mayan.finance/v10/init")
    chains = data.get("chains") if isinstance(data, dict) else None
    if not isinstance(chains, list):
        raise SystemExit("Mayan /v10/init response missing chains array")
    by_slug = {chain.get("nameId"): chain for chain in chains if isinstance(chain, dict)}
    expected = [("evm:1", "ethereum"), ("evm:42161", "arbitrum"), ("evm:8453", "base")]
    chain_specs = []
    for router_chain, slug in expected:
        chain = by_slug.get(slug)
        if not chain:
            raise SystemExit(f"Mayan chain {slug} missing from /v10/init")
        expected_chain_id = EVM_ROUTER_CHAIN_TO_ID[router_chain]
        if int(chain.get("chainId", -1)) != expected_chain_id:
            raise SystemExit(f"Mayan chain {slug} chainId mismatch: {chain.get('chainId')}")
        if chain.get("mode") != "EVM":
            raise SystemExit(f"Mayan chain {slug} is not EVM mode")
        if chain.get("originActive") is False or chain.get("destinationActive") is False:
            raise SystemExit(f"Mayan chain {slug} is not active for origin/destination")
        chain_specs.append(
            ChainSpec(
                const_name=chain_const_name("mayan", router_chain),
                router_chain=router_chain,
                venue_chain=slug,
                policy="VenueAssetPolicy::ListedPlusOpenContracts(MAYAN_EVM_CONTRACT_PASSTHROUGH)",
                assets=(AssetEntry("Native", ZERO_ADDRESS),),
            )
        )
    return VenueSpec(
        provider_variant="Mayan",
        const_name="MAYAN_ASSET_MAP_SPEC",
        chains_const="MAYAN_CHAINS",
        chains=tuple(chain_specs),
        imports=(
            "ContractEncoding",
            "ContractPassthrough",
            "ContractStandard",
            "RouterAssetKind",
            "VenueAssetEntry",
            "VenueAssetMapSpec",
            "VenueAssetPolicy",
            "VenueChainAssetsSpec",
        ),
        policy_consts=(
            "const MAYAN_EVM_CONTRACT_PASSTHROUGH: ContractPassthrough = ContractPassthrough {\n"
            "    standard: ContractStandard::Erc20,\n"
            "    encoding: ContractEncoding::LowercaseHex,\n"
            "};",
        ),
        header_comments=("Source: https://sia.mayan.finance/v10/init",),
    )


CHAINFLIP_STATIC = {
    "Btc": ("bitcoin", "Bitcoin", "Native", "BTC"),
    "Eth": ("evm:1", "Ethereum", "Native", "ETH"),
    "Usdc": ("evm:1", "Ethereum", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", "USDC"),
    "Usdt": ("evm:1", "Ethereum", "0xdac17f958d2ee523a2206206994597c13d831ec7", "USDT"),
    "Wbtc": ("evm:1", "Ethereum", "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599", "WBTC"),
    "Flip": ("evm:1", "Ethereum", "0x826180541412d574cf1336d22c0c0a287822678a", "FLIP"),
    "ArbEth": ("evm:42161", "Arbitrum", "Native", "ETH"),
    "ArbUsdc": ("evm:42161", "Arbitrum", "0xaf88d065e77c8cc2239327c5edb3a432268e5831", "USDC"),
    "ArbUsdt": ("evm:42161", "Arbitrum", "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9", "USDT"),
}


def chainflip_enabled(asset: dict[str, Any]) -> bool:
    return all(
        asset.get(field) is not False
        for field in (
            "egressEnabled",
            "vaultSwapDepositsEnabled",
            "depositChannelDepositsEnabled",
            "depositChannelCreationEnabled",
        )
    )


def generate_chainflip() -> VenueSpec:
    data = http_json("https://chainflip-swap.chainflip.io/api/networkInfo")
    assets = data.get("assets") if isinstance(data, dict) else None
    if not isinstance(assets, list):
        raise SystemExit("Chainflip /api/networkInfo response missing assets array")
    enabled_ids = {
        asset.get("asset")
        for asset in assets
        if isinstance(asset, dict) and isinstance(asset.get("asset"), str) and chainflip_enabled(asset)
    }
    entries_by_chain: dict[tuple[str, str], list[AssetEntry]] = {}
    for asset_id, (router_chain, venue_chain, router_asset, venue_asset) in CHAINFLIP_STATIC.items():
        if asset_id not in enabled_ids:
            continue
        if router_asset != "Native":
            router_asset = normalize_evm_address(router_asset)
        entries_by_chain.setdefault((router_chain, venue_chain), []).append(
            AssetEntry(router_asset, venue_asset)
        )
    ordered = [("bitcoin", "Bitcoin"), ("evm:1", "Ethereum"), ("evm:42161", "Arbitrum")]
    chain_specs = []
    for router_chain, venue_chain in ordered:
        entries = dedupe_entries(entries_by_chain.get((router_chain, venue_chain), []), "chainflip", router_chain)
        if entries:
            chain_specs.append(
                ChainSpec(
                    const_name=chain_const_name("chainflip", router_chain),
                    router_chain=router_chain,
                    venue_chain=venue_chain,
                    policy="VenueAssetPolicy::ListedOnly",
                    assets=entries,
                )
            )
    return VenueSpec(
        provider_variant="Chainflip",
        const_name="CHAINFLIP_ASSET_MAP_SPEC",
        chains_const="CHAINFLIP_CHAINS",
        chains=tuple(chain_specs),
        imports=(
            "RouterAssetKind",
            "VenueAssetEntry",
            "VenueAssetMapSpec",
            "VenueAssetPolicy",
            "VenueChainAssetsSpec",
        ),
        header_comments=("Source: https://chainflip-swap.chainflip.io/api/networkInfo plus official Chainflip asset constants",),
    )


def generate_garden(api_key: str, base_url: str) -> VenueSpec:
    if not api_key.strip():
        raise SystemExit("GARDEN_API_KEY is required to generate Garden asset specs")
    base = base_url.rstrip("/")
    data = http_json(f"{base}/chains", headers={"garden-app-id": api_key.strip()})
    if isinstance(data, dict) and data.get("status") not in (None, "Ok"):
        raise SystemExit(f"Garden /chains returned status {data.get('status')}: {data}")
    chains = data.get("result") if isinstance(data, dict) else None
    if not isinstance(chains, list):
        raise SystemExit("Garden /chains response missing result array")
    entries: dict[tuple[str, str], list[AssetEntry]] = {}
    for chain in chains:
        if not isinstance(chain, dict):
            continue
        router_chain = chain.get("id")
        venue_chain = chain.get("chain")
        if router_chain not in {"bitcoin", "evm:1", "evm:42161", "evm:8453"}:
            continue
        if not isinstance(venue_chain, str) or not venue_chain:
            continue
        if chain.get("is_active") is False:
            continue
        for asset in chain.get("assets") or []:
            if not isinstance(asset, dict) or asset.get("is_active") is False:
                continue
            asset_id = asset.get("id")
            if not isinstance(asset_id, str) or ":" not in asset_id:
                continue
            prefix, venue_asset = asset_id.split(":", 1)
            if prefix != venue_chain or not venue_asset:
                continue
            token = asset.get("token")
            if router_chain == "bitcoin" and venue_asset == "btc" and token in (None, {}):
                router_asset = "Native"
            elif router_chain.startswith("evm:") and isinstance(token, dict):
                if token.get("schema") != "evm:erc20":
                    continue
                address = token.get("address")
                if not isinstance(address, str) or not EVM_ADDRESS_RE.match(address):
                    continue
                router_asset = normalize_evm_address(address)
            else:
                continue
            entries.setdefault((router_chain, venue_chain), []).append(AssetEntry(router_asset, venue_asset))
    ordered = [("bitcoin", "bitcoin"), ("evm:1", "ethereum"), ("evm:42161", "arbitrum"), ("evm:8453", "base")]
    chain_specs = []
    for router_chain, venue_chain in ordered:
        chain_entries = dedupe_entries(entries.get((router_chain, venue_chain), []), "garden", router_chain)
        if chain_entries:
            chain_specs.append(
                ChainSpec(
                    const_name=chain_const_name("garden", router_chain),
                    router_chain=router_chain,
                    venue_chain=venue_chain,
                    policy="VenueAssetPolicy::ListedOnly",
                    assets=chain_entries,
                )
            )
    if not chain_specs:
        raise SystemExit("Garden generated no chain specs")
    return VenueSpec(
        provider_variant="Garden",
        const_name="GARDEN_ASSET_MAP_SPEC",
        chains_const="GARDEN_CHAINS",
        chains=tuple(chain_specs),
        imports=(
            "RouterAssetKind",
            "VenueAssetEntry",
            "VenueAssetMapSpec",
            "VenueAssetPolicy",
            "VenueChainAssetsSpec",
        ),
        header_comments=(f"Source: {base}/chains",),
    )

def generate_changenow() -> VenueSpec:
    data = http_json("https://api.changenow.io/v2/exchange/currencies?active=true")
    if not isinstance(data, list):
        raise SystemExit("ChangeNOW /currencies response is not an array")

    wanted = {
        "evm:1": ("eth", "eth"),
        "evm:42161": ("arbitrum", "arbitrum"),
        "evm:8453": ("base", "base"),
        "bitcoin": ("btc", "btc"),
    }

    entries_by_chain = {}
    for item in data:
        if not isinstance(item, dict):
            continue
        ticker = item.get("ticker")
        network = item.get("network")
        contract = item.get("tokenContract")

        if not isinstance(ticker, str) or not isinstance(network, str):
            continue

        for router_chain, (venue_chain, cn_network) in wanted.items():
            if network == cn_network:
                if not contract:
                    if ticker.lower() in ["eth", "btc"]:
                        router_asset = "Native"
                    else:
                        continue
                else:
                    if router_chain.startswith("evm:"):
                        try:
                            router_asset = normalize_evm_address(contract)
                        except ValueError:
                            continue
                    else:
                        continue

                key = (router_chain, venue_chain)
                entries_by_chain.setdefault(key, []).append(AssetEntry(router_asset, ticker))

    chain_specs = []
    ordered = sorted(wanted.keys())
    for router_chain in ordered:
        venue_chain, _ = wanted[router_chain]
        entries = dedupe_entries(
            entries_by_chain.get((router_chain, venue_chain), []),
            "changenow",
            router_chain,
        )
        if entries:
            chain_specs.append(
                ChainSpec(
                    const_name=chain_const_name("changenow", router_chain),
                    router_chain=router_chain,
                    venue_chain=venue_chain,
                    policy="VenueAssetPolicy::ListedOnly",
                    assets=entries,
                )
            )

    return VenueSpec(
        provider_variant="Changenow",
        const_name="CHANGENOW_ASSET_MAP_SPEC",
        chains_const="CHANGENOW_CHAINS",
        chains=tuple(chain_specs),
        imports=(
            "RouterAssetKind",
            "VenueAssetEntry",
            "VenueAssetMapSpec",
            "VenueAssetPolicy",
            "VenueChainAssetsSpec",
        ),
        header_comments=("Source: https://api.changenow.io/v2/exchange/currencies?active=true",),
    )


GENERATORS: dict[str, Callable[[argparse.Namespace], VenueSpec]] = {
    "relay": lambda args: generate_relay(),
    "near_intents": lambda args: generate_near_intents(),
    "mayan": lambda args: generate_mayan(),
    "chainflip": lambda args: generate_chainflip(),
    "garden": lambda args: generate_garden(args.garden_api_key or os.environ.get("GARDEN_API_KEY", ""), args.garden_api_url),
    "changenow": lambda args: generate_changenow(),
}


CONST_NAMES = {
    "relay": "RELAY_ASSET_MAP_SPEC",
    "near_intents": "NEAR_INTENTS_ASSET_MAP_SPEC",
    "mayan": "MAYAN_ASSET_MAP_SPEC",
    "chainflip": "CHAINFLIP_ASSET_MAP_SPEC",
    "garden": "GARDEN_ASSET_MAP_SPEC",
    "changenow": "CHANGENOW_ASSET_MAP_SPEC",
}



def selected_venues(value: str) -> list[str]:
    if value == "all":
        return list(GENERATORS)
    venues = [part.strip() for part in value.split(",") if part.strip()]
    unknown = [venue for venue in venues if venue not in GENERATORS]
    if unknown:
        raise SystemExit(f"unknown venue(s): {', '.join(unknown)}")
    return venues


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--venue",
        default="all",
        help="Venue to generate: all, or comma-separated relay,near_intents,mayan,chainflip,garden,changenow",
    )
    parser.add_argument("--check", action="store_true", help="Fail if generated files are not up to date")
    parser.add_argument(
        "--garden-api-key",
        default=None,
        help="Garden API key sent as garden-app-id. Defaults to GARDEN_API_KEY env.",
    )
    parser.add_argument(
        "--garden-api-url",
        default=os.environ.get("GARDEN_API_URL", "https://api.garden.finance/v2"),
        help="Garden API base URL.",
    )
    args = parser.parse_args()

    changed = False
    for venue in selected_venues(args.venue):
        spec = GENERATORS[venue](args)
        changed |= write_file(
            OUT_ROOT / venue / "mod.rs",
            rustfmt_content(module_file(CONST_NAMES[venue])),
            check=args.check,
        )
        changed |= write_file(
            OUT_ROOT / venue / "generated.rs",
            rustfmt_content(render_spec(spec)),
            check=args.check,
        )
    if args.check and changed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
