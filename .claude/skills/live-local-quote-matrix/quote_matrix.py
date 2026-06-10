#!/usr/bin/env python3
"""Read-only quote matrix against the live-local gateway.

Covers the distinct route families x an amount ladder from below-minimum to
past-liquidity-boundary. Records every response (or error) to a JSONL + a
summary table. No orders are created; nothing spends.
"""
import json
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

GATEWAY = "http://localhost:3001/quote"
OUT_DIR = sys.argv[1] if len(sys.argv) > 1 else "/tmp/quote-matrix"  # pass live-test-logs/<ts>--<scenario>

# (name, from, to, [amounts in readable units of `from`])
# Ladder philosophy: dust (expect below-floor reject), small, medium, large,
# huge (expect INSUFFICIENT_LIQUIDITY on book-bound routes).
CASES = [
    # --- BTC <-> EVM, the unit+HL multi-hop family (full ladder both ways)
    ("btc_to_eth_usdc",      "Bitcoin.BTC",      "Ethereum.USDC",   ["0.0001", "0.001", "0.1", "10", "900"]),
    ("eth_to_btc",           "Ethereum.ETH",     "Bitcoin.BTC",     ["0.001", "0.02", "1", "100", "20000"]),
    ("btc_to_base_eth",      "Bitcoin.BTC",      "Base.ETH",        ["0.001", "0.5", "50"]),
    ("base_usdc_to_btc",     "Base.USDC",        "Bitcoin.BTC",     ["1", "50", "5000", "500000", "30000000"]),
    # --- stable <-> stable cross-chain (CCTP / Across; huge should mostly work via CCTP)
    ("base_usdc_to_arb_usdc","Base.USDC",        "Arbitrum.USDC",   ["50", "5000", "1000000", "50000000"]),
    ("eth_usdc_to_base_usdc","Ethereum.USDC",    "Base.USDC",       ["50", "100000", "20000000"]),
    ("eth_usdt_to_arb_usdt", "Ethereum.USDT",    "Arbitrum.USDT",   ["50", "100000", "5000000"]),
    ("base_usdc_to_eth_usdt","Base.USDC",        "Ethereum.USDT",   ["50", "250000"]),
    # --- native <-> native / native <-> stable
    ("eth_to_base_eth",      "Ethereum.ETH",     "Base.ETH",        ["0.02", "5", "500"]),
    ("eth_to_eth_usdc",      "Ethereum.ETH",     "Ethereum.USDC",   ["0.02", "5", "2000"]),
    ("base_eth_to_arb_usdc", "Base.ETH",         "Arbitrum.USDC",   ["0.02", "10"]),
    # --- wrapped BTC family (exercises the new WBTC alias end-to-end)
    ("eth_wbtc_to_btc",      "Ethereum.WBTC",    "Bitcoin.BTC",     ["0.001", "0.05", "5", "300"]),
    ("eth_cbbtc_to_btc",     "Ethereum.CBBTC",   "Bitcoin.BTC",     ["0.05", "5"]),
    ("btc_to_arb_wbtc",      "Bitcoin.BTC",      "Arbitrum.WBTC",   ["0.05", "5"]),
    ("eth_wbtc_to_eth_usdc", "Ethereum.WBTC",    "Ethereum.USDC",   ["0.05", "20"]),
    ("arb_wbtc_to_base_eth", "Arbitrum.WBTC",    "Base.ETH",        ["0.05", "2"]),
    # --- Hyperliquid-chain endpoints
    ("hl_usdc_to_btc",       "Hyperliquid.USDC", "Bitcoin.BTC",     ["50", "5000", "2000000"]),
    ("base_usdc_to_hl_ubtc", "Base.USDC",        "Hyperliquid.UBTC",["50", "5000", "10000000"]),
    ("hl_ubtc_to_eth_usdc",  "Hyperliquid.UBTC", "Ethereum.USDC",   ["0.001", "0.5", "100", "900"]),
    ("hl_ueth_to_base_usdc", "Hyperliquid.UETH", "Base.USDC",       ["0.02", "10"]),
    ("eth_usdc_to_hl_hype",  "Ethereum.USDC",    "Hyperliquid.HYPE",["100"]),
    ("arb_wbtc_to_btc",      "Arbitrum.WBTC",    "Bitcoin.BTC",     ["0.05", "5"]),
    ("eth_wbtc_to_btc_huge", "Ethereum.WBTC",    "Bitcoin.BTC",     ["300"]),
    ("base_cbbtc_to_btc",    "Base.CBBTC",       "Bitcoin.BTC",     ["0.05", "5"]),
    ("btc_to_eth_cbbtc",     "Bitcoin.BTC",      "Ethereum.CBBTC",  ["0.5"]),
    ("arb_usdt_to_base_usdc","Arbitrum.USDT",    "Base.USDC",       ["50", "250000"]),
    ("hl_hype_to_eth_usdc",  "Hyperliquid.HYPE", "Ethereum.USDC",   ["10"]),
]


def quote(case, frm, to, amount):
    time.sleep(0.75)  # pacing: keep venue request rate low
    body = json.dumps({
        "from": frm, "to": to,
        "fromAmount": amount, "amountFormat": "readable",
    }).encode()
    req = urllib.request.Request(
        GATEWAY, data=body, method="POST",
        headers={"content-type": "application/json"})
    started = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read())
        return {
            "case": case, "from": frm, "to": to, "amount": amount,
            "http": 200, "latency_s": round(time.monotonic() - started, 2),
            "estimatedOut": data.get("estimatedOut"),
            "venues": data.get("venues"),
        }
    except urllib.error.HTTPError as e:
        raw = e.read().decode(errors="replace")
        try:
            err = json.loads(raw)["error"]
            code, msg = err.get("code"), err.get("message", "")[:160]
        except Exception:
            code, msg = "UNPARSEABLE", raw[:160]
        return {
            "case": case, "from": frm, "to": to, "amount": amount,
            "http": e.code, "latency_s": round(time.monotonic() - started, 2),
            "error_code": code, "error_message": msg,
        }
    except Exception as e:
        return {
            "case": case, "from": frm, "to": to, "amount": amount,
            "http": 0, "latency_s": round(time.monotonic() - started, 2),
            "error_code": "TRANSPORT", "error_message": str(e)[:160],
        }


def main():
    import os
    os.makedirs(OUT_DIR, exist_ok=True)
    jobs = [(c, f, t, a) for (c, f, t, amts) in CASES for a in amts]
    print(f"{len(jobs)} quotes across {len(CASES)} routes -> {OUT_DIR}", flush=True)
    results = []
    # Low concurrency + jitter: quotes fan out to live venue APIs (Velora's
    # free tier 429s easily); don't hammer them.
    with ThreadPoolExecutor(max_workers=2) as pool:
        for r in pool.map(lambda j: quote(*j), jobs):
            results.append(r)
            tag = (f"OK   out={r['estimatedOut']}" if r["http"] == 200
                   else f"{r['http']} {r.get('error_code')}: {r.get('error_message','')[:80]}")
            print(f"  {r['case']:24s} {r['amount']:>12} {r['from']:>17} -> {r['to']:<17} {tag}", flush=True)
    with open(f"{OUT_DIR}/results.jsonl", "w") as fh:
        for r in results:
            fh.write(json.dumps(r) + "\n")
    ok = sum(1 for r in results if r["http"] == 200)
    insliq = sum(1 for r in results if r.get("error_code") == "INSUFFICIENT_LIQUIDITY")
    other = len(results) - ok - insliq
    print(f"\nSUMMARY: {ok} quoted, {insliq} INSUFFICIENT_LIQUIDITY, {other} other-error of {len(results)}")


if __name__ == "__main__":
    main()
