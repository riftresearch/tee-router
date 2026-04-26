use secrecy::{ExposeSecret, SecretString};
use serde::{Serialize, Serializer};
use std::fmt;
use zeroize::Zeroize;

pub struct Wallet {
    pub address: String,
    private_key: SecretString,
}

impl Wallet {
    #[must_use]
    pub fn new(address: String, private_key: String) -> Self {
        Self {
            address,
            private_key: SecretString::from(private_key),
        }
    }

    #[must_use]
    pub fn private_key(&self) -> &str {
        self.private_key.expose_secret()
    }
}

impl fmt::Debug for Wallet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Wallet")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

impl Serialize for Wallet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.address.serialize(serializer)
    }
}

impl Drop for Wallet {
    fn drop(&mut self) {
        self.address.zeroize();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wallet_debug_redacts_private_key() {
        let wallet = Wallet::new(
            "0x1234567890123456789012345678901234567890".to_string(),
            "private_key_12345".to_string(),
        );

        let debug_str = format!("{wallet:?}");
        assert!(debug_str.contains("Wallet"));
        assert!(debug_str.contains("0x1234567890123456789012345678901234567890"));
        assert!(debug_str.contains(".."));
        assert!(!debug_str.contains("private_key"));
    }

    #[test]
    fn test_wallet_serialization_excludes_private_key() {
        let wallet = Wallet::new(
            "0x1234567890123456789012345678901234567890".to_string(),
            "private_key_12345".to_string(),
        );

        let json = serde_json::to_string(&wallet).unwrap();
        assert_eq!(json, "\"0x1234567890123456789012345678901234567890\"");
    }
}
