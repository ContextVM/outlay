//! Runtime configuration parsed from environment variables / `.env` files.
//!
//! Two kinds of relay URLs, kept distinct:
//! - `proxy_relay_url` — the single upstream Nostr relay outlay proxies (v1:
//!   one relay only, per design §6).
//! - `cvm_relay_urls`   — the ContextVM relays the server listens on.
//!
//! Env-var conventions follow `cordn-server` (`OUTLAY_` prefix). `.env` then
//! `.env.local` load first-write-wins (via `dotenvy`); missing files ignored.

use contextvm_sdk::GiftWrapMode;

#[derive(Debug, Clone, PartialEq)]
pub struct ServerConfig {
    /// External upstream relay to proxy (e.g. `ws://localhost:8080`,
    /// `wss://relay.nostr.net`). `None` → run the bundled in-process relay as
    /// the upstream (the default under the `bundled-relay` feature; a hard error
    /// otherwise). `Some(url)` → proxy that external relay instead (advanced).
    pub proxy_relay_url: Option<String>,
    /// ContextVM relays the server publishes to / listens on.
    pub cvm_relay_urls: Vec<String>,
    /// Hex/nsec private key. `None` → generate an ephemeral key each start.
    pub private_key_hex: Option<String>,
    pub server_name: String,
    pub server_about: Option<String>,
    pub is_announced: bool,
    /// Outbound gift-wrap kind policy. Default `Ephemeral` (kind 21059): CVM
    /// open-stream control frames are transient, so the relay cannot replay
    /// stale ones into a later stream. Requires the CVM relay + clients to
    /// handle 21059; both sides must agree (`Ephemeral` rejects incoming 1059).
    pub gift_wrap_mode: GiftWrapMode,
    /// Bundled in-process relay config (Shape A: loopback-only internal upstream).
    /// Only present under the `bundled-relay` feature; absent otherwise.
    #[cfg(feature = "bundled-relay")]
    pub bundled: BundledConfig,
}

/// Configuration for the bundled in-process relay (`outlay-relay` crate). The
/// relay binds loopback and acts as outlay's upstream when `enabled`, making
/// outlay fully self-contained (no external relay needed).
#[cfg(feature = "bundled-relay")]
#[derive(Debug, Clone, PartialEq)]
pub struct BundledConfig {
    /// `"sqlite"` (default; persistent, bundled SQLite) or `"memory"` (volatile).
    pub backend: String,
    /// SQLite file path (ignored for `memory`).
    pub db_path: String,
    /// Bind port (`0` = OS-assigned ephemeral — recommended).
    pub port: u16,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("OUTLAY_PROXY_RELAY_URL is required (no bundled relay compiled in; rebuild with the `bundled-relay` feature, or set an upstream URL)")]
    MissingProxyRelayUrl,
    #[error("Invalid boolean environment variable: {0}")]
    InvalidBoolean(String),
    #[error("invalid OUTLAY_GIFT_WRAP_MODE: {0} (expected persistent|ephemeral|optional)")]
    InvalidGiftWrapMode(String),
}

pub fn default_cvm_relay_urls() -> Vec<String> {
    vec!["wss://nostr.wtf".into()]
}

fn opt_string(env: &std::collections::HashMap<String, String>, name: &str) -> Option<String> {
    env.get(name)
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

fn opt_bool(
    env: &std::collections::HashMap<String, String>,
    name: &str,
) -> Result<Option<bool>, ConfigError> {
    match opt_string(env, name).as_deref() {
        None => Ok(None),
        Some("true") | Some("1") => Ok(Some(true)),
        Some("false") | Some("0") => Ok(Some(false)),
        Some(_) => Err(ConfigError::InvalidBoolean(name.into())),
    }
}

/// Read the server config from the given environment map (defaults applied for
/// missing vars). Pass `std::env::vars().collect()` for the live environment.
pub fn read_server_config(
    env: &std::collections::HashMap<String, String>,
) -> Result<ServerConfig, ConfigError> {
    #[cfg(feature = "bundled-relay")]
    let bundled = BundledConfig {
        backend: opt_string(env, "OUTLAY_BUNDLED_BACKEND").unwrap_or_else(|| "sqlite".into()),
        db_path: opt_string(env, "OUTLAY_BUNDLED_DB_PATH")
            .unwrap_or_else(|| "outlay-relay.db".into()),
        port: opt_string(env, "OUTLAY_BUNDLED_PORT")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0),
    };

    // Upstream mode is chosen by the presence of `OUTLAY_PROXY_RELAY_URL`:
    //   set   → proxy that external relay (advanced; skip the bundled relay)
    //   unset → run the bundled in-process relay as the upstream (default)
    // Without the `bundled-relay` feature compiled in, an unset URL is a hard
    // error — there's no bundled relay to fall back to.
    let proxy_relay_url = opt_string(env, "OUTLAY_PROXY_RELAY_URL");
    if proxy_relay_url.is_none() && !cfg!(feature = "bundled-relay") {
        return Err(ConfigError::MissingProxyRelayUrl);
    }

    let cvm_relay_urls = match opt_string(env, "OUTLAY_RELAY_URLS") {
        Some(raw) => {
            let urls: Vec<String> = raw
                .split(',')
                .map(|s| s.trim().to_owned())
                .filter(|s| !s.is_empty())
                .collect();
            if urls.is_empty() {
                default_cvm_relay_urls()
            } else {
                urls
            }
        }
        None => default_cvm_relay_urls(),
    };

    // Default `Ephemeral` (21059): stream control frames are transient, so the
    // relay cannot replay stale ones into a fresh stream. Both sides must agree
    // on the kind; `Ephemeral` rejects incoming persistent (1059) wraps.
    let gift_wrap_mode = match opt_string(env, "OUTLAY_GIFT_WRAP_MODE").as_deref() {
        None => GiftWrapMode::Ephemeral,
        Some("ephemeral") => GiftWrapMode::Ephemeral,
        Some("persistent") => GiftWrapMode::Persistent,
        Some("optional") => GiftWrapMode::Optional,
        Some(other) => return Err(ConfigError::InvalidGiftWrapMode(other.into())),
    };

    Ok(ServerConfig {
        proxy_relay_url,
        cvm_relay_urls,
        private_key_hex: opt_string(env, "OUTLAY_SERVER_PRIVATE_KEY"),
        server_name: opt_string(env, "OUTLAY_SERVER_NAME").unwrap_or_else(|| "outlay".into()),
        server_about: opt_string(env, "OUTLAY_SERVER_ABOUT"),
        is_announced: opt_bool(env, "OUTLAY_ANNOUNCED")?.unwrap_or(false),
        gift_wrap_mode,
        #[cfg(feature = "bundled-relay")]
        bundled,
    })
}

/// Convenience: load `.env` then `.env.local` (first-write-wins) and read from
/// the live environment.
pub fn load() -> Result<ServerConfig, ConfigError> {
    let _ = dotenvy::dotenv();
    let _ = dotenvy::from_filename(".env.local");
    read_server_config(&std::env::vars().collect())
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
        let c =
            read_server_config(&env(&[("OUTLAY_PROXY_RELAY_URL", "ws://localhost:8080")])).unwrap();
        assert_eq!(c.proxy_relay_url.as_deref(), Some("ws://localhost:8080"));
        assert_eq!(c.cvm_relay_urls, vec!["wss://nostr.wtf"]);
        assert_eq!(c.server_name, "outlay");
        assert!(!c.is_announced);
        assert!(c.private_key_hex.is_none());
        assert_eq!(c.gift_wrap_mode, GiftWrapMode::Ephemeral);
    }

    #[test]
    fn overrides() {
        let c = read_server_config(&env(&[
            ("OUTLAY_PROXY_RELAY_URL", "wss://relay.nostr.net"),
            ("OUTLAY_RELAY_URLS", "wss://a.test, wss://b.test"),
            ("OUTLAY_SERVER_NAME", "my-outlay"),
            ("OUTLAY_ANNOUNCED", "1"),
            ("OUTLAY_GIFT_WRAP_MODE", "persistent"),
        ]))
        .unwrap();
        assert_eq!(c.proxy_relay_url.as_deref(), Some("wss://relay.nostr.net"));
        assert_eq!(c.cvm_relay_urls, vec!["wss://a.test", "wss://b.test"]);
        assert_eq!(c.server_name, "my-outlay");
        assert!(c.is_announced);
        assert_eq!(c.gift_wrap_mode, GiftWrapMode::Persistent);
    }

    // Without the `bundled-relay` feature, an unset upstream is a hard error
    // (there's no bundled relay to fall back to).
    #[cfg(not(feature = "bundled-relay"))]
    #[test]
    fn missing_upstream_is_required_without_bundled() {
        assert!(matches!(
            read_server_config(&HashMap::new()),
            Err(ConfigError::MissingProxyRelayUrl)
        ));
    }

    // Bundled-relay config is only compiled under the `bundled-relay` feature.
    #[cfg(feature = "bundled-relay")]
    #[test]
    fn no_upstream_defaults_to_bundled() {
        let c = read_server_config(&env(&[("OUTLAY_BUNDLED_BACKEND", "memory")])).unwrap();
        assert_eq!(c.proxy_relay_url, None, "no proxy URL → bundled mode");
        assert_eq!(c.bundled.backend, "memory");
        assert_eq!(c.bundled.port, 0);
    }

    #[cfg(feature = "bundled-relay")]
    #[test]
    fn bundled_defaults_to_sqlite() {
        let c = read_server_config(&HashMap::new()).unwrap();
        assert_eq!(c.bundled.backend, "sqlite");
        assert_eq!(c.bundled.db_path, "outlay-relay.db");
    }

    #[cfg(feature = "bundled-relay")]
    #[test]
    fn proxy_url_selects_proxy_mode() {
        let c = read_server_config(&env(&[(
            "OUTLAY_PROXY_RELAY_URL",
            "wss://relay.primal.net",
        )]))
        .unwrap();
        assert_eq!(c.proxy_relay_url.as_deref(), Some("wss://relay.primal.net"));
    }

    #[test]
    fn invalid_gift_wrap_mode_rejected() {
        assert!(matches!(
            read_server_config(&env(&[
                ("OUTLAY_PROXY_RELAY_URL", "ws://localhost:8080"),
                ("OUTLAY_GIFT_WRAP_MODE", "sealed"),
            ])),
            Err(ConfigError::InvalidGiftWrapMode(_))
        ));
    }
}
