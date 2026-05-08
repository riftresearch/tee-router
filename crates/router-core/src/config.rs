use secrecy::{ExposeSecret, SecretBox};
use snafu::{ResultExt, Snafu};
use std::{fs, path::Path};
use zeroize::Zeroize;

#[derive(Debug, Snafu)]
pub enum SettingsError {
    #[snafu(display("Failed to load master key file: {}", source))]
    Load { source: std::io::Error },

    #[snafu(display("Failed to decode hex: {}", source))]
    HexDecode { source: alloy::hex::FromHexError },

    #[snafu(display("Invalid master key length"))]
    KeyLength,
}

type Result<T> = std::result::Result<T, SettingsError>;

#[derive(Clone)]
pub struct MasterKey([u8; 64]);

pub struct Settings {
    pub master_key: SecretBox<MasterKey>,
}

impl Zeroize for MasterKey {
    fn zeroize(&mut self) {
        self.0.zeroize();
    }
}

impl Settings {
    pub fn load(master_key_path: impl AsRef<Path>) -> Result<Self> {
        let master_key_hex = fs::read_to_string(master_key_path.as_ref())
            .context(LoadSnafu)?
            .trim()
            .to_string();
        let master_key = alloy::hex::decode(master_key_hex)
            .context(HexDecodeSnafu)?
            .try_into()
            .map_err(|_| SettingsError::KeyLength)?;

        Ok(Self {
            master_key: SecretBox::new(Box::new(MasterKey(master_key))),
        })
    }

    #[must_use]
    pub fn master_key_bytes(&self) -> [u8; 64] {
        self.master_key.expose_secret().0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn load_rejects_missing_master_key_file() {
        let dir = tempdir().expect("tempdir");
        let err = match Settings::load(dir.path().join("missing-master-key.hex")) {
            Ok(_) => panic!("expected missing master key file to fail"),
            Err(err) => err,
        };
        assert!(matches!(err, SettingsError::Load { .. }));
    }

    #[test]
    fn load_accepts_valid_master_key_file() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("router-master-key.hex");
        std::fs::write(&path, alloy::hex::encode([0x42_u8; 64])).expect("write master key");

        let settings = Settings::load(&path).expect("load settings");
        assert_eq!(settings.master_key_bytes(), [0x42_u8; 64]);
    }
}
