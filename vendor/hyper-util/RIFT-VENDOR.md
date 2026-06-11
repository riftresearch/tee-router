# Vendored hyper-util 0.1.20 (patched)

Pristine crates.io source of `hyper-util 0.1.20` with **one local change**, applied
via `[patch.crates-io]` in the workspace `Cargo.toml`.

## The patch

`src/client/legacy/connect/proxy/socks/v5/mod.rs`, `SocksConfig::execute` (marked
`RIFT PATCH`): strip square brackets from the destination host before the
`parse::<IpAddr>()` classification.

## Why

reqwest's SOCKS5 local-DNS path (`socks5://` scheme, used by `proxy-transport`'s
`ProxyDnsMode::LocalIpv6Only`) resolves the destination locally and re-embeds the
resolved IP in a `Uri`. IPv6 addresses must be bracketed to form a valid `Uri`
(reqwest PR #2753), and `http::Uri::host()` returns them *still bracketed*.
Unpatched hyper-util fails to parse `"[2606:...]"` as an `IpAddr`, misclassifies
it as a domain name, and asks the SOCKS proxy to DNS-resolve the bracketed
literal — the proxy replies host-unreachable. Net effect upstream: every IPv6
destination over `socks5://` is broken (reqwest issue #637, open since 2019;
reqwest PR #2877 fixes only the resolve side, not this handoff).

Verified end-to-end by `crates/proxy-transport/tests/live_proxy_profiles.rs`
(venues on the `ipv6-us-west-1` profile fail without this patch, pass with it).

## Upgrading

When bumping hyper-util: re-vendor the new version, re-apply the `RIFT PATCH`
hunk (or drop the vendor entirely if upstream has fixed the bracket handling in
`SocksConfig::execute`), and re-run the live test above through the real
proxies.
