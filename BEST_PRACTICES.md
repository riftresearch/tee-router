# Best Practices

## Railway Deployments

Railway deployments that are inherently server services must be linked to the
GitHub repository so CI/CD deploys work automatically from committed changes.

This applies to application services, workers, API gateways, indexers,
collectors, proxies, and any other long-running service built from this repo.

Database services, volumes, and one-shot operational jobs do not need GitHub
source linkage unless they are built from repo-owned code.

## Proxy DNS Semantics

When using SOCKS proxies, prefer `socks5://`, not `socks5h://`.

`socks5h://` delegates DNS resolution to the proxy, which increases trust in the
proxy by letting it choose the destination IP for a hostname. TEE Router should
resolve destination hostnames itself and use `socks5://` unless there is a
documented, reviewed exception.
