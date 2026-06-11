use std::{fmt, net::SocketAddr, sync::Arc};

use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use url::Url;

/// A validated upstream SOCKS5 proxy URL.
///
/// Only `socks5://` is accepted. `socks5h://` delegates DNS resolution to the
/// proxy and changes the endpoint-security contract.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ProxyUrl(String);

impl ProxyUrl {
    pub fn parse(value: impl AsRef<str>, env_name: &'static str) -> Result<Self, ProxyUrlError> {
        let value = value.as_ref().trim();
        let parsed = Url::parse(value).map_err(|source| ProxyUrlError {
            env_name,
            value: sanitize_proxy_url_for_error(value),
            reason: format!("invalid URL: {source}"),
        })?;
        if parsed.scheme() != "socks5" {
            return Err(ProxyUrlError {
                env_name,
                value: sanitize_proxy_url_for_error(value),
                reason: format!("unsupported scheme {:?}", parsed.scheme()),
            });
        }
        if parsed.host_str().is_none() {
            return Err(ProxyUrlError {
                env_name,
                value: sanitize_proxy_url_for_error(value),
                reason: "missing host".to_string(),
            });
        }
        if parsed.port().is_none() {
            return Err(ProxyUrlError {
                env_name,
                value: sanitize_proxy_url_for_error(value),
                reason: "missing port".to_string(),
            });
        }
        if !parsed.path().is_empty() && parsed.path() != "/" {
            return Err(ProxyUrlError {
                env_name,
                value: sanitize_proxy_url_for_error(value),
                reason: "paths are not allowed".to_string(),
            });
        }
        if parsed.query().is_some() {
            return Err(ProxyUrlError {
                env_name,
                value: sanitize_proxy_url_for_error(value),
                reason: "query strings are not allowed".to_string(),
            });
        }
        if parsed.fragment().is_some() {
            return Err(ProxyUrlError {
                env_name,
                value: sanitize_proxy_url_for_error(value),
                reason: "fragments are not allowed".to_string(),
            });
        }
        Ok(Self(parsed.as_str().to_string()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }

    #[must_use]
    pub fn redacted(&self) -> String {
        sanitize_proxy_url_for_error(&self.0)
    }
}

impl fmt::Debug for ProxyUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ProxyUrl").field(&self.redacted()).finish()
    }
}

impl fmt::Display for ProxyUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.redacted())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyUrlError {
    pub env_name: &'static str,
    pub value: String,
    pub reason: String,
}

impl fmt::Display for ProxyUrlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} has invalid proxy URL {}: {}",
            self.env_name, self.value, self.reason
        )
    }
}

impl std::error::Error for ProxyUrlError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProxyDnsMode {
    SystemDefault,
    LocalIpv6Only,
}

impl ProxyDnsMode {
    pub fn parse(
        value: impl AsRef<str>,
        env_name: &'static str,
    ) -> Result<Self, ProxyDnsModeError> {
        match value.as_ref().trim().to_ascii_lowercase().as_str() {
            "system-default" | "system" | "default" => Ok(Self::SystemDefault),
            "local-ipv6-only" | "ipv6-only" => Ok(Self::LocalIpv6Only),
            other => Err(ProxyDnsModeError {
                env_name,
                value: other.to_string(),
            }),
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SystemDefault => "system-default",
            Self::LocalIpv6Only => "local-ipv6-only",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyDnsModeError {
    pub env_name: &'static str,
    pub value: String,
}

impl fmt::Display for ProxyDnsModeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} has unsupported proxy DNS mode {:?}; expected system-default or local-ipv6-only",
            self.env_name, self.value
        )
    }
}

impl std::error::Error for ProxyDnsModeError {}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct UpstreamProxy {
    url: ProxyUrl,
    dns_mode: ProxyDnsMode,
}

impl UpstreamProxy {
    #[must_use]
    pub const fn new(url: ProxyUrl, dns_mode: ProxyDnsMode) -> Self {
        Self { url, dns_mode }
    }

    #[must_use]
    pub fn url(&self) -> &ProxyUrl {
        &self.url
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.url.as_str()
    }

    #[must_use]
    pub const fn dns_mode(&self) -> ProxyDnsMode {
        self.dns_mode
    }

    #[must_use]
    pub fn redacted(&self) -> String {
        format!("{} dns={}", self.url.redacted(), self.dns_mode.as_str())
    }
}

impl fmt::Debug for UpstreamProxy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UpstreamProxy")
            .field("url", &self.url.redacted())
            .field("dns_mode", &self.dns_mode)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct Ipv6OnlyResolver;

impl Resolve for Ipv6OnlyResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().to_string();
        Box::pin(async move {
            let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), 0))
                .await?
                .filter(SocketAddr::is_ipv6)
                .collect();
            if addrs.is_empty() {
                return Err(format!("no AAAA records for {host}").into());
            }
            Ok(Box::new(addrs.into_iter()) as Addrs)
        })
    }
}

pub fn apply_reqwest_proxy(
    builder: reqwest::ClientBuilder,
    proxy: Option<&UpstreamProxy>,
) -> Result<reqwest::ClientBuilder, reqwest::Error> {
    let Some(proxy) = proxy else {
        return Ok(builder);
    };
    let mut builder = builder.proxy(reqwest::Proxy::all(proxy.as_str())?);
    if proxy.dns_mode() == ProxyDnsMode::LocalIpv6Only {
        builder = builder.dns_resolver(Arc::new(Ipv6OnlyResolver));
    }
    Ok(builder)
}

#[must_use]
pub fn sanitize_proxy_url_for_error(value: &str) -> String {
    let Ok(parsed) = Url::parse(value.trim()) else {
        return "<invalid url>".to_string();
    };

    let host = parsed.host_str().unwrap_or("<missing-host>");
    let mut redacted = format!("{}://", parsed.scheme());
    if !parsed.username().is_empty() || parsed.password().is_some() {
        redacted.push_str("<redacted>@");
    }
    redacted.push_str(host);
    if let Some(port) = parsed.port() {
        redacted.push(':');
        redacted.push_str(&port.to_string());
    }
    if !parsed.path().is_empty() && parsed.path() != "/" {
        redacted.push_str("/<redacted-path>");
    }
    if parsed.query().is_some() {
        redacted.push_str("?<redacted-query>");
    }
    if parsed.fragment().is_some() {
        redacted.push_str("#<redacted-fragment>");
    }
    redacted
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
    fn parses_dns_modes() {
        assert_eq!(
            ProxyDnsMode::parse("system-default", "TEST_DNS_MODE").unwrap(),
            ProxyDnsMode::SystemDefault
        );
        assert_eq!(
            ProxyDnsMode::parse("local-ipv6-only", "TEST_DNS_MODE").unwrap(),
            ProxyDnsMode::LocalIpv6Only
        );
        assert!(ProxyDnsMode::parse("socks5h", "TEST_DNS_MODE").is_err());
    }
}
