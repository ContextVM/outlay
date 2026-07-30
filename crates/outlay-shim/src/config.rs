//! Runtime configuration for the outlay shim, parsed from env / `.env`.
//!
//! `.env` then `.env.local` load first-write-wins (via `dotenvy`); missing
//! files ignored. The shim does not depend on `outlay` — the two config structs
//! differ (design/shim.md §7).

use std::collections::HashMap;

use contextvm_sdk::{EncryptionMode, GiftWrapMode};

#[derive(Clone)]
pub struct ShimConfig {
    /// Local bind address, e.g. `127.0.0.1:8088`. Localhost-only by design
    /// (no authz/TLS in v1 — design/shim.md §9.7).
    pub listen_addr: String,
    /// CVM relays used to reach outlay servers. Overridden per-connection when
    /// the `/<pubkey>` path is an nprofile carrying its own relay hints.
    pub relay_urls: Vec<String>,
    /// Hex/nsec client key. `None` → ephemeral per run.
    pub private_key: Option<String>,
    pub encryption_mode: EncryptionMode,
    /// CVM transport connect timeout.
    pub connect_timeout: std::time::Duration,
    /// Outbound gift-wrap kind policy. Default `Ephemeral` (kind 21059): CVM
    /// open-stream control frames are not stored by the relay, so they cannot be
    /// replayed/backfilled into a later stream — removes the stored-frame replay
    /// root cause outright. Requires the CVM relay + the server to handle 21059;
    /// both sides must agree (`Ephemeral` rejects incoming 1059).
    pub gift_wrap_mode: GiftWrapMode,
    /// Max distinct outlay identities kept cached (each = one CVM transport +
    /// relay subscription). Hot identities stay cached for the process lifetime;
    /// least-recently-used identities evict at the cap. Bounds memory/connections
    /// against drive-by traffic to random `/<pubkey>` paths.
    pub max_cached_outlays: usize,
    /// WS message size cap (inbound + outbound).
    pub max_ws_message_bytes: usize,
    /// Whether to run the colocated memoryless relay endpoint at `/`. On by
    /// default: outlays that want to collapse their CVM transport relay into the
    /// shim (one fewer hop) advertise the shim's public URL as their relay hint;
    /// outlays that don't are unaffected. Zero idle cost — the relay is
    /// memoryless and event-driven.
    pub enable_relay: bool,
    /// Test-only: an injected mock CVM relay pool (replaces the real transport,
    /// giving a network-free shim↔outlay hop). Mirrors outlay's `test-utils`.
    #[cfg(feature = "test-utils")]
    pub test_relay_pool: Option<std::sync::Arc<dyn contextvm_sdk::RelayPoolTrait>>,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("invalid OUTLAY_SHIM_ENCRYPTION_MODE: {0} (expected optional|disabled|required)")]
    InvalidEncryption(String),
    #[error("invalid OUTLAY_SHIM_GIFT_WRAP_MODE: {0} (expected persistent|ephemeral|optional)")]
    InvalidGiftWrapMode(String),
    #[error("Invalid boolean environment variable: {0}")]
    InvalidBoolean(String),
}

pub fn default_relay_urls() -> Vec<String> {
    vec!["wss://nostr.wtf".into()]
}

fn opt_string(env: &HashMap<String, String>, name: &str) -> Option<String> {
    env.get(name)
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

/// Parse a boolean env var accepting `true`/`1`/`false`/`0` (case-insensitive).
fn opt_bool_env(env: &HashMap<String, String>, name: &str) -> Result<Option<bool>, ConfigError> {
    match opt_string(env, name).map(|s| s.to_ascii_lowercase()) {
        None => Ok(None),
        Some(s) if s == "true" || s == "1" => Ok(Some(true)),
        Some(s) if s == "false" || s == "0" => Ok(Some(false)),
        Some(other) => Err(ConfigError::InvalidBoolean(other)),
    }
}

/// Read the shim config from the given environment map (defaults applied for
/// missing vars). Pass `std::env::vars().collect()` for the live environment.
pub fn read_shim_config(env: &HashMap<String, String>) -> Result<ShimConfig, ConfigError> {
    let listen_addr =
        opt_string(env, "OUTLAY_SHIM_LISTEN_ADDR").unwrap_or_else(|| "127.0.0.1:8088".into());

    let relay_urls = match opt_string(env, "OUTLAY_SHIM_RELAY_URLS") {
        Some(raw) => {
            let urls: Vec<String> = raw
                .split(',')
                .map(|s| s.trim().to_owned())
                .filter(|s| !s.is_empty())
                .collect();
            if urls.is_empty() {
                default_relay_urls()
            } else {
                urls
            }
        }
        None => default_relay_urls(),
    };

    let encryption_mode = match opt_string(env, "OUTLAY_SHIM_ENCRYPTION_MODE").as_deref() {
        None => EncryptionMode::Optional,
        Some("optional") => EncryptionMode::Optional,
        Some("disabled") => EncryptionMode::Disabled,
        Some("required") => EncryptionMode::Required,
        Some(other) => return Err(ConfigError::InvalidEncryption(other.into())),
    };

    // Default `Ephemeral` (21059): stream control frames are transient, so the
    // relay cannot replay stale ones into a fresh stream. Both sides must agree
    // on the kind; `Ephemeral` rejects incoming persistent (1059) wraps.
    let gift_wrap_mode = match opt_string(env, "OUTLAY_SHIM_GIFT_WRAP_MODE").as_deref() {
        None => GiftWrapMode::Ephemeral,
        Some("ephemeral") => GiftWrapMode::Ephemeral,
        Some("persistent") => GiftWrapMode::Persistent,
        Some("optional") => GiftWrapMode::Optional,
        Some(other) => return Err(ConfigError::InvalidGiftWrapMode(other.into())),
    };

    let connect_timeout = std::time::Duration::from_secs(
        opt_string(env, "OUTLAY_SHIM_CONNECT_TIMEOUT")
            .and_then(|s| s.parse().ok())
            .unwrap_or(15),
    );

    let max_ws_message_bytes = opt_string(env, "OUTLAY_SHIM_MAX_WS_MESSAGE_BYTES")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_048_576);

    let max_cached_outlays = opt_string(env, "OUTLAY_SHIM_MAX_CACHED_OUTLAYS")
        .and_then(|s| s.parse().ok())
        .unwrap_or(64)
        .max(1);

    let enable_relay = opt_bool_env(env, "OUTLAY_SHIM_RELAY")?.unwrap_or(true);

    Ok(ShimConfig {
        listen_addr,
        relay_urls,
        private_key: opt_string(env, "OUTLAY_SHIM_PRIVATE_KEY"),
        encryption_mode,
        connect_timeout,
        gift_wrap_mode,
        max_cached_outlays,
        max_ws_message_bytes,
        enable_relay,
        #[cfg(feature = "test-utils")]
        test_relay_pool: None,
    })
}

/// Convenience: load `.env` then `.env.local` (first-write-wins) and read from
/// the live environment.
pub fn load() -> Result<ShimConfig, ConfigError> {
    let _ = dotenvy::dotenv();
    let _ = dotenvy::from_filename(".env.local");
    read_shim_config(&std::env::vars().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn defaults() {
        let c = read_shim_config(&HashMap::new()).unwrap();
        assert_eq!(c.listen_addr, "127.0.0.1:8088");
        assert_eq!(c.relay_urls, vec!["wss://nostr.wtf"]);
        assert_eq!(c.encryption_mode, EncryptionMode::Optional);
        assert_eq!(c.connect_timeout, std::time::Duration::from_secs(15));
        assert_eq!(c.max_ws_message_bytes, 1_048_576);
        assert!(c.private_key.is_none());
        assert_eq!(c.gift_wrap_mode, GiftWrapMode::Ephemeral);
        assert_eq!(c.max_cached_outlays, 64);
        assert!(c.enable_relay, "relay endpoint defaults to on");
    }

    #[test]
    fn overrides() {
        let c = read_shim_config(&env(&[
            ("OUTLAY_SHIM_LISTEN_ADDR", "127.0.0.1:9100"),
            ("OUTLAY_SHIM_RELAY_URLS", "wss://a.test, wss://b.test"),
            ("OUTLAY_SHIM_ENCRYPTION_MODE", "disabled"),
            ("OUTLAY_SHIM_CONNECT_TIMEOUT", "30"),
            ("OUTLAY_SHIM_PRIVATE_KEY", "nsec1..."),
            ("OUTLAY_SHIM_GIFT_WRAP_MODE", "optional"),
            ("OUTLAY_SHIM_MAX_CACHED_OUTLAYS", "8"),
            ("OUTLAY_SHIM_RELAY", "false"),
        ]))
        .unwrap();
        assert_eq!(c.listen_addr, "127.0.0.1:9100");
        assert_eq!(c.relay_urls, vec!["wss://a.test", "wss://b.test"]);
        assert_eq!(c.encryption_mode, EncryptionMode::Disabled);
        assert_eq!(c.connect_timeout, std::time::Duration::from_secs(30));
        assert_eq!(c.private_key.as_deref(), Some("nsec1..."));
        assert_eq!(c.gift_wrap_mode, GiftWrapMode::Optional);
        assert_eq!(c.max_cached_outlays, 8);
        assert!(
            !c.enable_relay,
            "OUTLAY_SHIM_RELAY=false disables the relay"
        );
    }

    #[test]
    fn invalid_encryption_rejected() {
        assert!(matches!(
            read_shim_config(&env(&[("OUTLAY_SHIM_ENCRYPTION_MODE", "encrypted")])),
            Err(ConfigError::InvalidEncryption(_))
        ));
    }

    #[test]
    fn invalid_gift_wrap_mode_rejected() {
        assert!(matches!(
            read_shim_config(&env(&[("OUTLAY_SHIM_GIFT_WRAP_MODE", "sealed")])),
            Err(ConfigError::InvalidGiftWrapMode(_))
        ));
    }

    #[test]
    fn max_cached_outlays_floors_to_one() {
        let c = read_shim_config(&env(&[("OUTLAY_SHIM_MAX_CACHED_OUTLAYS", "0")])).unwrap();
        assert_eq!(c.max_cached_outlays, 1);
    }

    #[test]
    fn invalid_relay_flag_rejected() {
        assert!(matches!(
            read_shim_config(&env(&[("OUTLAY_SHIM_RELAY", "yes")])),
            Err(ConfigError::InvalidBoolean(_))
        ));
    }
}
