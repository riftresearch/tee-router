//! Live transport check: every production upstream through its exact proxy
//! profile (URL + DNS mode), exercising the same `apply_reqwest_proxy` path the
//! binaries use. Pass = an HTTP response came back (any status); fail = a
//! transport error (DNS, SOCKS5 connect, TLS).
//!
//! Requires the proxy profile URLs in the environment (and optionally the
//! direct-upstream URLs):
//!
//! ```bash
//! set -a; source .env.live-local; set +a
//! cargo test -p proxy-transport --test live_proxy_profiles -- --ignored --nocapture
//! ```

use std::time::{Duration, Instant};

use proxy_transport::{apply_reqwest_proxy, ProxyDnsMode, ProxyUrl, UpstreamProxy};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Clone, Copy)]
enum Profile {
    Direct,
    Ipv4UsWest1,
    Ipv6UsWest1,
    Ipv4Eu,
}

impl Profile {
    fn name(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Ipv4UsWest1 => "ipv4-us-west-1",
            Self::Ipv6UsWest1 => "ipv6-us-west-1",
            Self::Ipv4Eu => "ipv4-eu",
        }
    }

    fn upstream_proxy(self) -> Option<UpstreamProxy> {
        let (url_env, dns_mode) = match self {
            Self::Direct => return None,
            Self::Ipv4UsWest1 => (
                "PROXY_PROFILE_IPV4_US_WEST_1_URL",
                ProxyDnsMode::SystemDefault,
            ),
            Self::Ipv6UsWest1 => (
                "PROXY_PROFILE_IPV6_US_WEST_1_URL",
                ProxyDnsMode::LocalIpv6Only,
            ),
            Self::Ipv4Eu => ("PROXY_PROFILE_IPV4_EU_URL", ProxyDnsMode::SystemDefault),
        };
        let raw = std::env::var(url_env)
            .unwrap_or_else(|_| panic!("{url_env} must be set (source .env.live-local)"));
        let url = ProxyUrl::parse(&raw, url_env).expect("profile proxy URL must be valid");
        Some(UpstreamProxy::new(url, dns_mode))
    }
}

/// The production venue → profile mapping pinned in etc/compose.phala.yml.
const VENUES: &[(&str, &str, Profile)] = &[
    ("across", "https://app.across.to/api", Profile::Ipv6UsWest1),
    ("cctp", "https://iris-api.circle.com", Profile::Ipv4UsWest1),
    ("hyperunit", "https://api.hyperunit.xyz", Profile::Ipv4Eu),
    (
        "hyperliquid",
        "https://api.hyperliquid.xyz/info",
        Profile::Ipv4UsWest1,
    ),
    (
        "kyberswap",
        "https://aggregator-api.kyberswap.com",
        Profile::Ipv6UsWest1,
    ),
    ("relay", "https://api.relay.link", Profile::Ipv4UsWest1),
    (
        "near_intents",
        "https://1click.chaindefuser.com",
        Profile::Ipv6UsWest1,
    ),
    (
        "mayan",
        "https://price-api.mayan.finance/v3",
        Profile::Ipv6UsWest1,
    ),
    (
        "chainflip",
        "https://chainflip-swap.chainflip.io",
        Profile::Ipv6UsWest1,
    ),
    (
        "garden",
        "https://api.garden.finance/v2",
        Profile::Ipv6UsWest1,
    ),
    (
        "chainalysis",
        "https://rift-address-screener.up.railway.app",
        Profile::Direct,
    ),
    ("coinbase", "https://api.coinbase.com", Profile::Ipv6UsWest1),
    // No venue maps to ipv4-us-west-1 today, but the profile is configured in
    // the catalog — keep its transport path covered.
    (
        "profile-coverage",
        "https://api.ipify.org",
        Profile::Ipv4UsWest1,
    ),
];

/// Direct upstreams, included when their URL env vars are present.
const DIRECT_UPSTREAMS: &[(&str, &str)] = &[
    ("ethereum_rpc", "ETH_RPC_URL"),
    ("base_rpc", "BASE_RPC_URL"),
    ("arbitrum_rpc", "ARBITRUM_RPC_URL"),
    ("bitcoin_rpc", "BITCOIN_RPC_URL"),
    ("esplora", "ESPLORA_HTTP_SERVER_URL"),
];

struct Outcome {
    venue: &'static str,
    profile: &'static str,
    result: Result<(reqwest::StatusCode, Duration), String>,
}

async fn probe(venue: &'static str, url: String, profile: Profile) -> Outcome {
    let result = async {
        let builder = reqwest::Client::builder()
            .use_rustls_tls()
            // Transport check only: a redirect status is a pass, and following it
            // can hop to a different host with different IPv6 support (coinbase's
            // root redirects to a v4-only docs site).
            .redirect(reqwest::redirect::Policy::none())
            .timeout(REQUEST_TIMEOUT);
        let client = apply_reqwest_proxy(builder, profile.upstream_proxy().as_ref())
            .map_err(|err| format!("proxy setup: {err}"))?
            .build()
            .map_err(|err| format!("client build: {err}"))?;
        let started = Instant::now();
        let response = client.get(&url).send().await.map_err(|err| {
            let mut rendered = format!("transport: {err}");
            let mut source = std::error::Error::source(&err);
            while let Some(cause) = source {
                rendered.push_str(&format!(" -> {cause}"));
                source = cause.source();
            }
            rendered
        })?;
        Ok((response.status(), started.elapsed()))
    }
    .await;
    Outcome {
        venue,
        profile: profile.name(),
        result,
    }
}

#[tokio::test]
#[ignore = "live network test: hits real venues through the real proxy profiles"]
async fn every_production_upstream_is_reachable_through_its_exact_profile() {
    let mut tasks = Vec::new();
    for &(venue, url, profile) in VENUES {
        tasks.push(tokio::spawn(probe(venue, url.to_string(), profile)));
    }
    for &(venue, url_env) in DIRECT_UPSTREAMS {
        let Ok(url) = std::env::var(url_env) else {
            eprintln!("skipping {venue}: {url_env} not set");
            continue;
        };
        tasks.push(tokio::spawn(probe(venue, url, Profile::Direct)));
    }

    let mut failures = Vec::new();
    for task in tasks {
        let outcome = task.await.expect("probe task must not panic");
        match &outcome.result {
            Ok((status, elapsed)) => eprintln!(
                "PASS {:<14} via {:<16} -> HTTP {} in {:?}",
                outcome.venue, outcome.profile, status, elapsed
            ),
            Err(err) => {
                eprintln!(
                    "FAIL {:<14} via {:<16} -> {}",
                    outcome.venue, outcome.profile, err
                );
                failures.push(format!("{} ({}): {}", outcome.venue, outcome.profile, err));
            }
        }
    }

    assert!(
        failures.is_empty(),
        "transport failures:\n- {}",
        failures.join("\n- ")
    );
}
