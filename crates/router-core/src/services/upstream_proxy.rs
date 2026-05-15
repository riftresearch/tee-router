use std::fmt;

use url::Url;

pub const PRODUCTION_ENV: &str = "PRODUCTION";
pub const UPSTREAM_PROXY_URL_ENV: &str = "UPSTREAM_PROXY_URL";

/// A validated upstream proxy URL.
///
/// The router intentionally supports only `socks5://` proxies. `socks5h://`
/// moves DNS resolution to the proxy, which changes the endpoint-security
/// assumptions for upstream providers.
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

pub fn optional_proxy(
    value: Option<&str>,
    env_name: &'static str,
) -> Result<Option<ProxyUrl>, ProxyUrlError> {
    let Some(value) = normalize_optional_string(value) else {
        return Ok(None);
    };
    ProxyUrl::parse(value, env_name).map(Some)
}

pub fn effective_proxy(
    specific_value: Option<&str>,
    specific_env_name: &'static str,
    upstream_value: Option<&str>,
) -> Result<Option<ProxyUrl>, ProxyUrlError> {
    match optional_proxy(specific_value, specific_env_name)? {
        Some(proxy_url) => Ok(Some(proxy_url)),
        None => optional_proxy(upstream_value, UPSTREAM_PROXY_URL_ENV),
    }
}

#[must_use]
pub fn normalize_optional_string(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
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
    fn empty_specific_proxy_inherits_upstream_proxy() {
        let resolved = effective_proxy(
            Some("  "),
            "CHAIN_PROXY_URL",
            Some("socks5://proxy.example:1080"),
        )
        .unwrap()
        .unwrap();

        assert_eq!(resolved.as_str(), "socks5://proxy.example:1080");
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
