//! Configuration management for OpenSky credentials.
//!
//! Reads credentials from `~/.config/opensky/settings.conf` (Linux/macOS)
//! or `%LOCALAPPDATA%\opensky\settings.conf` (Windows).

use crate::types::{OpenSkyError, Result};
use configparser::ini::Ini;
use std::path::PathBuf;

/// OpenSky configuration containing Trino credentials.
#[derive(Debug, Clone, Default)]
pub struct Config {
    /// Trino username
    pub username: Option<String>,
    /// Trino password
    pub password: Option<String>,
    /// Live API client ID (optional)
    pub client_id: Option<String>,
    /// Live API client secret (optional)
    pub client_secret: Option<String>,
    /// Cache purge duration (e.g., "90 days")
    pub cache_purge: Option<String>,
}

impl Config {
    /// Load configuration from the default config file.
    pub fn load() -> Result<Self> {
        let config_path = Self::config_path()?;
        Self::load_from_path(&config_path)
    }

    /// Load configuration from a specific path.
    pub fn load_from_path(path: &PathBuf) -> Result<Self> {
        if !path.exists() {
            return Err(OpenSkyError::Config(format!(
                "Config file not found: {}. Run `ostk pyopensky config set` to create it.",
                path.display()
            )));
        }

        let mut ini = Ini::new();
        ini.load(path).map_err(|e| OpenSkyError::Config(e))?;

        let config = Config {
            username: ini.get("default", "username").filter(|s| !s.is_empty()),
            password: ini.get("default", "password").filter(|s| !s.is_empty()),
            client_id: ini.get("default", "client_id").filter(|s| !s.is_empty()),
            client_secret: ini.get("default", "client_secret").filter(|s| !s.is_empty()),
            cache_purge: ini.get("cache", "purge").filter(|s| !s.is_empty()),
        };

        Ok(config)
    }

    /// Get the platform-specific config directory for OpenSky.
    pub fn config_dir() -> Result<PathBuf> {
        #[cfg(target_os = "linux")]
        {
            dirs::config_dir()
                .map(|p| p.join("opensky"))
                .ok_or_else(|| OpenSkyError::Config("Could not determine config directory".into()))
        }

        #[cfg(target_os = "macos")]
        {
            dirs::config_dir()
                .map(|p| p.join("opensky"))
                .ok_or_else(|| OpenSkyError::Config("Could not determine config directory".into()))
        }

        #[cfg(target_os = "windows")]
        {
            dirs::data_local_dir()
                .map(|p| p.join("opensky"))
                .ok_or_else(|| OpenSkyError::Config("Could not determine config directory".into()))
        }

        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        {
            dirs::home_dir()
                .map(|p| p.join(".opensky"))
                .ok_or_else(|| OpenSkyError::Config("Could not determine home directory".into()))
        }
    }

    /// Get the config file path.
    pub fn config_path() -> Result<PathBuf> {
        Ok(Self::config_dir()?.join("settings.conf"))
    }

    /// Check if credentials are configured.
    pub fn has_credentials(&self) -> bool {
        self.username.is_some() && self.password.is_some()
    }

    /// Get username or return error.
    pub fn require_username(&self) -> Result<&str> {
        self.username
            .as_deref()
            .ok_or_else(|| OpenSkyError::Config("Username not configured".into()))
    }

    /// Get password or return error.
    pub fn require_password(&self) -> Result<&str> {
        self.password
            .as_deref()
            .ok_or_else(|| OpenSkyError::Config("Password not configured".into()))
    }
}

/// Default config file content template.
pub const DEFAULT_CONFIG: &str = r#"[default]
username =
password =
client_id =
client_secret =

[cache]
purge = 90 days
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_load_config() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(
            temp_file,
            r#"[default]
username = testuser
password = testpass

[cache]
purge = 30 days
"#
        )
        .unwrap();

        let config = Config::load_from_path(&temp_file.path().to_path_buf()).unwrap();
        assert_eq!(config.username, Some("testuser".to_string()));
        assert_eq!(config.password, Some("testpass".to_string()));
        assert_eq!(config.cache_purge, Some("30 days".to_string()));
        assert!(config.has_credentials());
    }

    #[test]
    fn test_empty_values_treated_as_none() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(
            temp_file,
            r#"[default]
username =
password =
"#
        )
        .unwrap();

        let config = Config::load_from_path(&temp_file.path().to_path_buf()).unwrap();
        assert_eq!(config.username, None);
        assert_eq!(config.password, None);
        assert!(!config.has_credentials());
    }
}
