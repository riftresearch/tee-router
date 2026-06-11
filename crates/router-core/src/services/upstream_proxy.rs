use std::{collections::BTreeMap, fmt};

pub use proxy_transport::{
    sanitize_proxy_url_for_error, ProxyDnsMode, ProxyDnsModeError, ProxyUrl, ProxyUrlError,
    UpstreamProxy,
};

pub const PRODUCTION_ENV: &str = "PRODUCTION";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProxyProfile {
    Direct,
    Proxied(UpstreamProxy),
}

impl ProxyProfile {
    #[must_use]
    pub const fn as_upstream_proxy(&self) -> Option<&UpstreamProxy> {
        match self {
            Self::Direct => None,
            Self::Proxied(proxy) => Some(proxy),
        }
    }

    #[must_use]
    pub fn into_upstream_proxy(self) -> Option<UpstreamProxy> {
        match self {
            Self::Direct => None,
            Self::Proxied(proxy) => Some(proxy),
        }
    }
}

/// Every upstream the router reaches over HTTP, paired with the env vars that
/// configure it. The single source of truth for proxy-profile env names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ProxyTarget {
    Across,
    Cctp,
    Hyperunit,
    Hyperliquid,
    Velora,
    Kyberswap,
    Relay,
    NearIntents,
    Mayan,
    Chainflip,
    Garden,
    Chainalysis,
    Coinbase,
    EthereumRpc,
    BaseRpc,
    ArbitrumRpc,
    BitcoinRpc,
    Esplora,
}

impl ProxyTarget {
    #[must_use]
    pub const fn profile_env(self) -> &'static str {
        match self {
            Self::Across => "ACROSS_PROXY_PROFILE",
            Self::Cctp => "CCTP_PROXY_PROFILE",
            Self::Hyperunit => "HYPERUNIT_PROXY_PROFILE",
            Self::Hyperliquid => "HYPERLIQUID_PROXY_PROFILE",
            Self::Velora => "VELORA_PROXY_PROFILE",
            Self::Kyberswap => "KYBERSWAP_PROXY_PROFILE",
            Self::Relay => "RELAY_PROXY_PROFILE",
            Self::NearIntents => "NEAR_INTENTS_PROXY_PROFILE",
            Self::Mayan => "MAYAN_PROXY_PROFILE",
            Self::Chainflip => "CHAINFLIP_PROXY_PROFILE",
            Self::Garden => "GARDEN_PROXY_PROFILE",
            Self::Chainalysis => "CHAINALYSIS_PROXY_PROFILE",
            Self::Coinbase => "COINBASE_PROXY_PROFILE",
            Self::EthereumRpc => "ETH_RPC_PROXY_PROFILE",
            Self::BaseRpc => "BASE_RPC_PROXY_PROFILE",
            Self::ArbitrumRpc => "ARBITRUM_RPC_PROXY_PROFILE",
            Self::BitcoinRpc => "BITCOIN_RPC_PROXY_PROFILE",
            Self::Esplora => "ESPLORA_PROXY_PROFILE",
        }
    }

    #[must_use]
    pub const fn upstream_env(self) -> &'static str {
        match self {
            Self::Across => "ACROSS_API_URL",
            Self::Cctp => "CCTP_API_URL",
            Self::Hyperunit => "HYPERUNIT_API_URL",
            Self::Hyperliquid => "HYPERLIQUID_API_URL",
            Self::Velora => "VELORA_API_URL",
            Self::Kyberswap => "KYBERSWAP_API_URL",
            Self::Relay => "RELAY_API_URL",
            Self::NearIntents => "NEAR_INTENTS_API_URL",
            Self::Mayan => "MAYAN_API_URL",
            Self::Chainflip => "CHAINFLIP_API_URL",
            Self::Garden => "GARDEN_API_URL",
            Self::Chainalysis => "CHAINALYSIS_HOST",
            Self::Coinbase => "COINBASE_PRICE_API_BASE_URL",
            Self::EthereumRpc => "ETH_RPC_URL",
            Self::BaseRpc => "BASE_RPC_URL",
            Self::ArbitrumRpc => "ARBITRUM_RPC_URL",
            Self::BitcoinRpc => "BITCOIN_RPC_URL",
            Self::Esplora => "ESPLORA_HTTP_SERVER_URL",
        }
    }
}

/// Proxy profiles resolved against the catalog, keyed by upstream target.
/// A missing entry means no profile env was set for that target.
#[derive(Debug, Clone, Default)]
pub struct ResolvedProxies(BTreeMap<ProxyTarget, ProxyProfile>);

impl ResolvedProxies {
    pub fn resolve<'a>(
        catalog: &ProxyProfileCatalog,
        entries: impl IntoIterator<Item = (ProxyTarget, Option<&'a str>)>,
    ) -> Result<Self, ProxyProfileError> {
        let mut profiles = BTreeMap::new();
        for (target, value) in entries {
            if let Some(profile) = catalog.resolve(value, target.profile_env())? {
                profiles.insert(target, profile);
            }
        }
        Ok(Self(profiles))
    }

    #[must_use]
    pub fn profile(&self, target: ProxyTarget) -> Option<&ProxyProfile> {
        self.0.get(&target)
    }

    #[must_use]
    pub fn proxy_ref(&self, target: ProxyTarget) -> Option<&UpstreamProxy> {
        self.profile(target)
            .and_then(ProxyProfile::as_upstream_proxy)
    }

    #[must_use]
    pub fn proxy_owned(&self, target: ProxyTarget) -> Option<UpstreamProxy> {
        self.proxy_ref(target).cloned()
    }

    /// Append a `{PROFILE_ENV} is required for {UPSTREAM_ENV}` error for every
    /// target without a configured profile.
    pub fn require(
        &self,
        errors: &mut Vec<String>,
        targets: impl IntoIterator<Item = ProxyTarget>,
    ) {
        for target in targets {
            if self.profile(target).is_none() {
                errors.push(format!(
                    "{} is required for {}",
                    target.profile_env(),
                    target.upstream_env()
                ));
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProxyProfileCatalog {
    ipv4_us_west_1: Option<UpstreamProxy>,
    ipv6_us_west_1: Option<UpstreamProxy>,
    ipv4_eu: Option<UpstreamProxy>,
}

impl ProxyProfileCatalog {
    pub fn new(
        ipv4_us_west_1_url: Option<&str>,
        ipv4_us_west_1_dns_mode: &str,
        ipv6_us_west_1_url: Option<&str>,
        ipv6_us_west_1_dns_mode: &str,
        ipv4_eu_url: Option<&str>,
        ipv4_eu_dns_mode: &str,
    ) -> Result<Self, ProxyProfileError> {
        Ok(Self {
            ipv4_us_west_1: configured_profile(
                ipv4_us_west_1_url,
                "PROXY_PROFILE_IPV4_US_WEST_1_URL",
                ipv4_us_west_1_dns_mode,
                "PROXY_PROFILE_IPV4_US_WEST_1_DNS_MODE",
            )?,
            ipv6_us_west_1: configured_profile(
                ipv6_us_west_1_url,
                "PROXY_PROFILE_IPV6_US_WEST_1_URL",
                ipv6_us_west_1_dns_mode,
                "PROXY_PROFILE_IPV6_US_WEST_1_DNS_MODE",
            )?,
            ipv4_eu: configured_profile(
                ipv4_eu_url,
                "PROXY_PROFILE_IPV4_EU_URL",
                ipv4_eu_dns_mode,
                "PROXY_PROFILE_IPV4_EU_DNS_MODE",
            )?,
        })
    }

    pub fn resolve(
        &self,
        profile_value: Option<&str>,
        profile_env_name: &'static str,
    ) -> Result<Option<ProxyProfile>, ProxyProfileError> {
        let Some(profile) = normalize_optional_string(profile_value) else {
            return Ok(None);
        };
        match profile.trim().to_ascii_lowercase().as_str() {
            "direct" => Ok(Some(ProxyProfile::Direct)),
            "ipv4-us-west-1" => {
                self.profile_value(&self.ipv4_us_west_1, &profile, profile_env_name)
            }
            "ipv6-us-west-1" => {
                self.profile_value(&self.ipv6_us_west_1, &profile, profile_env_name)
            }
            "ipv4-eu" => self.profile_value(&self.ipv4_eu, &profile, profile_env_name),
            other => Err(ProxyProfileError::UnknownProfile {
                env_name: profile_env_name,
                profile: other.to_string(),
            }),
        }
    }

    fn profile_value(
        &self,
        value: &Option<UpstreamProxy>,
        profile: &str,
        env_name: &'static str,
    ) -> Result<Option<ProxyProfile>, ProxyProfileError> {
        value
            .clone()
            .map(ProxyProfile::Proxied)
            .map(Some)
            .ok_or_else(|| ProxyProfileError::MissingProfileUrl {
                env_name,
                profile: profile.to_string(),
            })
    }
}

fn configured_profile(
    url: Option<&str>,
    url_env_name: &'static str,
    dns_mode: &str,
    dns_mode_env_name: &'static str,
) -> Result<Option<UpstreamProxy>, ProxyProfileError> {
    let Some(url) = normalize_optional_string(url) else {
        return Ok(None);
    };
    let url = ProxyUrl::parse(&url, url_env_name).map_err(ProxyProfileError::ProxyUrl)?;
    let dns_mode =
        ProxyDnsMode::parse(dns_mode, dns_mode_env_name).map_err(ProxyProfileError::DnsMode)?;
    Ok(Some(UpstreamProxy::new(url, dns_mode)))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProxyProfileError {
    ProxyUrl(ProxyUrlError),
    DnsMode(ProxyDnsModeError),
    UnknownProfile {
        env_name: &'static str,
        profile: String,
    },
    MissingProfileUrl {
        env_name: &'static str,
        profile: String,
    },
}

impl fmt::Display for ProxyProfileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ProxyUrl(error) => write!(f, "{error}"),
            Self::DnsMode(error) => write!(f, "{error}"),
            Self::UnknownProfile { env_name, profile } => write!(
                f,
                "{env_name} references unknown proxy profile {profile:?}; expected direct, ipv4-us-west-1, ipv6-us-west-1, or ipv4-eu"
            ),
            Self::MissingProfileUrl { env_name, profile } => write!(
                f,
                "{env_name} references proxy profile {profile:?}, but that profile has no configured URL"
            ),
        }
    }
}

impl std::error::Error for ProxyProfileError {}

#[must_use]
pub fn normalize_optional_string(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_socks5_host_port_with_optional_credentials() {
        let bare = ProxyUrl::parse("socks5://proxy.example:1080", "TEST_PROXY_URL").unwrap();
        assert_eq!(bare.as_str(), "socks5://proxy.example:1080");

        let authed = ProxyUrl::parse(
            "socks5://user:password@proxy.example:1080",
            "TEST_PROXY_URL",
        )
        .unwrap();
        assert_eq!(authed.redacted(), "socks5://<redacted>@proxy.example:1080");
    }

    #[test]
    fn rejects_proxy_shapes_that_change_or_weaken_contract() {
        for value in [
            "socks5h://proxy.example:1080",
            "http://proxy.example:1080",
            "socks5://proxy.example",
            "socks5://proxy.example:1080/path",
            "socks5://proxy.example:1080?token=secret",
            "socks5://proxy.example:1080#fragment",
        ] {
            let err = ProxyUrl::parse(value, "TEST_PROXY_URL").unwrap_err();
            assert_eq!(err.env_name, "TEST_PROXY_URL");
        }
    }

    #[test]
    fn profiles_resolve_direct_and_configured_routes() {
        let catalog = ProxyProfileCatalog::new(
            Some("socks5://proxy.example:1080"),
            "system-default",
            Some("socks5://ipv6.example:1080"),
            "local-ipv6-only",
            None,
            "local-ipv6-only",
        )
        .unwrap();

        assert_eq!(
            catalog
                .resolve(Some("direct"), "TEST_PROXY_PROFILE")
                .unwrap(),
            Some(ProxyProfile::Direct)
        );
        let resolved = catalog
            .resolve(Some("ipv6-us-west-1"), "TEST_PROXY_PROFILE")
            .unwrap()
            .unwrap();
        let proxy = resolved.as_upstream_proxy().unwrap();

        assert_eq!(proxy.as_str(), "socks5://ipv6.example:1080");
        assert_eq!(proxy.dns_mode(), ProxyDnsMode::LocalIpv6Only);
    }

    #[test]
    fn resolved_proxies_resolve_lookup_and_require() {
        let catalog = ProxyProfileCatalog::new(
            Some("socks5://proxy.example:1080"),
            "system-default",
            None,
            "local-ipv6-only",
            None,
            "system-default",
        )
        .unwrap();

        let proxies = ResolvedProxies::resolve(
            &catalog,
            [
                (ProxyTarget::Across, Some("ipv4-us-west-1")),
                (ProxyTarget::Velora, Some("direct")),
                (ProxyTarget::Cctp, None),
            ],
        )
        .unwrap();

        assert_eq!(
            proxies.proxy_ref(ProxyTarget::Across).unwrap().as_str(),
            "socks5://proxy.example:1080"
        );
        assert_eq!(
            proxies.profile(ProxyTarget::Velora),
            Some(&ProxyProfile::Direct)
        );
        assert!(proxies.proxy_ref(ProxyTarget::Velora).is_none());
        assert!(proxies.profile(ProxyTarget::Cctp).is_none());

        let mut errors = Vec::new();
        proxies.require(
            &mut errors,
            [ProxyTarget::Across, ProxyTarget::Cctp, ProxyTarget::Esplora],
        );
        assert_eq!(
            errors,
            vec![
                "CCTP_PROXY_PROFILE is required for CCTP_API_URL".to_string(),
                "ESPLORA_PROXY_PROFILE is required for ESPLORA_HTTP_SERVER_URL".to_string(),
            ]
        );
    }

    #[test]
    fn redaction_does_not_include_credentials_or_query_values() {
        let rendered =
            sanitize_proxy_url_for_error("socks5://user:secret@proxy.example:1080/path?token=abc");
        assert!(!rendered.contains("user"));
        assert!(!rendered.contains("secret"));
        assert!(!rendered.contains("abc"));
        assert_eq!(
            rendered,
            "socks5://<redacted>@proxy.example:1080/<redacted-path>?<redacted-query>"
        );
    }
}
