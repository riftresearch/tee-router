use argon2::{Argon2, PasswordHash, PasswordVerifier};
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize)]
pub struct PublicApiKeyRecord {
    pub id: Uuid,
    pub tag: String,
    pub hash: String,
}

impl fmt::Debug for PublicApiKeyRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PublicApiKeyRecord")
            .field("id", &self.id)
            .field("tag", &self.tag)
            .field("hash", &"<redacted>")
            .finish()
    }
}

impl PublicApiKeyRecord {
    #[must_use]
    pub fn verify(&self, api_key: &str) -> bool {
        if let Ok(parsed_hash) = PasswordHash::new(&self.hash) {
            Argon2::default()
                .verify_password(api_key.as_bytes(), &parsed_hash)
                .is_ok()
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_api_key_record_debug_redacts_hash() {
        let hash = "$argon2id$v=19$m=19456,t=2,p=1$secret-salt$secret-hash".to_string();
        let record = PublicApiKeyRecord {
            id: Uuid::nil(),
            tag: "admin".to_string(),
            hash: hash.clone(),
        };

        let debug = format!("{record:?}");

        assert!(debug.contains("PublicApiKeyRecord"));
        assert!(debug.contains("tag: \"admin\""));
        assert!(debug.contains("hash: \"<redacted>\""));
        assert!(!debug.contains(&hash));
        assert!(!debug.contains("secret-salt"));
        assert!(!debug.contains("secret-hash"));
    }
}
