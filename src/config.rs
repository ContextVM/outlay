//! Runtime configuration parsed from environment variables / `.env` files.
//!
//! Two kinds of relay URLs, kept distinct:
//! - `proxy_relay_url` — the single upstream Nostr relay outlay proxies (v1:
//!   one relay only, per design §6).
//! - `cvm_relay_urls`   — the ContextVM relays the server listens on.
//!
//! Env-var conventions follow `cordn-server` (`OUTLAY_` prefix). The `.env`
//! loader is a direct port: first-write-wins per key, missing files ignored.

#[derive(Debug, Clone, PartialEq)]
pub struct ServerConfig {
    /// Upstream relay to proxy (e.g. `ws://localhost:8080`, `wss://relay.nostr.net`).
    /// Required — a proxy with no upstream is meaningless.
    pub proxy_relay_url: String,
    /// ContextVM relays the server publishes to / listens on.
    pub cvm_relay_urls: Vec<String>,
    /// Hex/nsec private key. `None` → generate an ephemeral key each start.
    pub private_key_hex: Option<String>,
    pub server_name: String,
    pub server_about: Option<String>,
    pub is_announced: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("OUTLAY_PROXY_RELAY_URL is required (the upstream relay to proxy)")]
    MissingProxyRelayUrl,
    #[error("Invalid boolean environment variable: {0}")]
    InvalidBoolean(String),
}

pub fn default_cvm_relay_urls() -> Vec<String> {
    vec!["wss://relay.contextvm.org".into()]
}

/// Load `.env` then `.env.local` into the process environment, without
/// overwriting variables already set. Missing files are ignored.
/// Ported from `cordn-server` (loadRuntimeEnv / loadEnvFile).
pub fn load_env_files() {
    load_env_file(".env");
    load_env_file(".env.local");
}

fn load_env_file(path: &str) {
    let Ok(content) = std::fs::read_to_string(path) else {
        return;
    };
    for line in content.lines() {
        let Some((key, value)) = parse_env_assignment(line) else {
            continue;
        };
        if std::env::var_os(&key).is_none() {
            std::env::set_var(&key, &value);
        }
    }
}

fn parse_env_assignment(line: &str) -> Option<(String, String)> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return None;
    }
    let normalized = trimmed
        .strip_prefix("export ")
        .map(str::trim)
        .unwrap_or(trimmed);
    let sep = normalized.find('=')?;
    if sep == 0 {
        return None;
    }
    let key = normalized[..sep].trim();
    if !key
        .chars()
        .next()
        .map(|c| c.is_ascii_alphabetic() || c == '_')
        .unwrap_or(false)
        || !key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return None;
    }
    let mut value = normalized[sep + 1..].trim().to_owned();
    if (value.starts_with('"') && value.ends_with('"') && value.len() >= 2)
        || (value.starts_with('\'') && value.ends_with('\'') && value.len() >= 2)
    {
        value = value[1..value.len() - 1].to_owned();
    }
    Some((key.to_owned(), value))
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
    let proxy_relay_url =
        opt_string(env, "OUTLAY_PROXY_RELAY_URL").ok_or(ConfigError::MissingProxyRelayUrl)?;

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

    Ok(ServerConfig {
        proxy_relay_url,
        cvm_relay_urls,
        private_key_hex: opt_string(env, "OUTLAY_SERVER_PRIVATE_KEY"),
        server_name: opt_string(env, "OUTLAY_SERVER_NAME").unwrap_or_else(|| "outlay".into()),
        server_about: opt_string(env, "OUTLAY_SERVER_ABOUT"),
        is_announced: opt_bool(env, "OUTLAY_ANNOUNCED")?.unwrap_or(false),
    })
}

/// Convenience: load `.env` files then read from the live environment.
pub fn load() -> Result<ServerConfig, ConfigError> {
    load_env_files();
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
        assert_eq!(c.proxy_relay_url, "ws://localhost:8080");
        assert_eq!(c.cvm_relay_urls, vec!["wss://relay.contextvm.org"]);
        assert_eq!(c.server_name, "outlay");
        assert!(!c.is_announced);
        assert!(c.private_key_hex.is_none());
    }

    #[test]
    fn overrides() {
        let c = read_server_config(&env(&[
            ("OUTLAY_PROXY_RELAY_URL", "wss://relay.nostr.net"),
            ("OUTLAY_RELAY_URLS", "wss://a.test, wss://b.test"),
            ("OUTLAY_SERVER_NAME", "my-outlay"),
            ("OUTLAY_ANNOUNCED", "1"),
        ]))
        .unwrap();
        assert_eq!(c.proxy_relay_url, "wss://relay.nostr.net");
        assert_eq!(c.cvm_relay_urls, vec!["wss://a.test", "wss://b.test"]);
        assert_eq!(c.server_name, "my-outlay");
        assert!(c.is_announced);
    }

    #[test]
    fn missing_upstream_is_required() {
        assert!(matches!(
            read_server_config(&HashMap::new()),
            Err(ConfigError::MissingProxyRelayUrl)
        ));
    }

    #[test]
    fn env_assignment_parser() {
        assert_eq!(
            parse_env_assignment("FOO=bar"),
            Some(("FOO".into(), "bar".into()))
        );
        assert_eq!(
            parse_env_assignment("export BAZ = \"hi\""),
            Some(("BAZ".into(), "hi".into()))
        );
        assert_eq!(parse_env_assignment("# comment"), None);
        assert_eq!(parse_env_assignment(""), None);
        assert_eq!(parse_env_assignment("1BAD=x"), None);
    }
}
