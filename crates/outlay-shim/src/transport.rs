//! CVM client transport construction and the per-outlay transport cache.
//!
//! `build_client` builds one stateless CVM client transport. `TransportCache`
//! shares ONE long-lived transport per outlay identity across every WS
//! connection and NIP-11 request, keyed by hex pubkey (so hex / npub / nprofile
//! of the same identity collapse to one entry). Sharing is the fix for the
//! per-connection churn that caused CVM open-stream cross-delivery, progress-
//! token collisions, and stored-frame replay (design/shim.md §5). Stateless mode
//! skips the `initialize` handshake (design/shim.md §6).

use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use contextvm_sdk::transport::client::{NostrClientTransport, NostrClientTransportConfig};
use contextvm_sdk::transport::open_stream::OpenStreamConfig;
use contextvm_sdk::{signer, ClientOpenStreamHandle};
use lru::LruCache;
use nostr_sdk::RelayUrl;
use rmcp::{ClientHandler, RoleClient, ServiceExt};
use tokio::sync::OnceCell;

use crate::config::ShimConfig;
use crate::path::ParsedPath;

/// Minimal rmcp client handler — the shim only makes tool calls, it serves none.
#[derive(Clone, Default)]
pub struct ShimClient;
impl ClientHandler for ShimClient {}

pub type Client = rmcp::service::RunningService<RoleClient, ShimClient>;

/// One shared, process-lifetime CVM transport per outlay identity. All WS
/// connections and NIP-11 requests for a given server pubkey reuse the same
/// transport — one relay subscription and one rmcp token counter, so concurrent
/// streams never collide on a shared inbox and never reuse `token: 0`. Bounded
/// LRU: hot identities stay cached; a drive-by to a new identity evicts the
/// least-recently-used at the cap.
///
/// Eviction drops the cached `Arc<OnceCell<SharedTransport>>`; rmcp's
/// `RunningService` cancels its driver on drop (its `DropGuard` fires the
/// cancellation token), so the relay subscription and connections close without
/// an explicit `cancel().await`. In-flight calls on an evicted identity see
/// their `Peer` die mid-call (rare: requires `cap` other identities touched
/// since this one's last use); they error out rather than hang.
// ponytail: a transport that dies irrecoverably (e.g. relay ban) can't be
// rebuilt — the OnceCell holds the dead value. Process restart recovers it; add
// re-init only if we observe permanent death in the wild.
pub struct TransportCache {
    entries: Mutex<LruCache<String, Arc<OnceCell<SharedTransport>>>>,
    /// Loopback URL of the colocated memoryless relay when it is enabled.
    /// `resolve_relay_urls` rewrites any of the shim's own public URLs to this,
    /// so the bridge dials the in-process relay directly instead of hairpin-
    /// dialing its public address (which times out). `None` when the relay
    /// endpoint is disabled — then no rewriting happens.
    loopback_url: Option<String>,
}

struct SharedTransport {
    /// Drives the transport for the entry's lifetime. Read on every `get` to
    /// hand out fresh `Peer` clones; never explicitly cancelled.
    client: Client,
    handle: ClientOpenStreamHandle,
}

impl TransportCache {
    pub fn new(cap: NonZeroUsize, loopback_url: Option<String>) -> Self {
        Self {
            entries: Mutex::new(LruCache::new(cap)),
            loopback_url,
        }
    }

    /// Returns clones of the shared `Peer` + open-stream handle for `parsed`.
    /// The first call for an identity builds the transport (concurrent
    /// first-callers share a single build); later calls clone the cached value.
    /// A failed build leaves the cell empty, so the next call retries.
    pub async fn get(
        &self,
        cfg: &ShimConfig,
        parsed: &ParsedPath,
    ) -> anyhow::Result<(rmcp::service::Peer<RoleClient>, ClientOpenStreamHandle)> {
        // Brief lock over the LRU map — synchronous get/put only, no await held.
        // `get` touches the entry (recently-used); `put` inserts a fresh cell for
        // a new identity and auto-evicts the LRU at capacity, dropping the evicted
        // Arc (which tears that transport down if no in-flight Peer holds it).
        let cell = {
            let mut entries = lock(&self.entries);
            if let Some(cell) = entries.get(&parsed.hex) {
                cell.clone()
            } else {
                let cell = Arc::new(OnceCell::new());
                entries.put(parsed.hex.clone(), cell.clone());
                cell
            }
        };
        let transport = cell
            .get_or_try_init(|| async {
                let (client, handle) = build_client(cfg, parsed, &self.loopback_url).await?;
                Ok::<_, anyhow::Error>(SharedTransport { client, handle })
            })
            .await?;
        Ok((transport.client.peer().clone(), transport.handle.clone()))
    }
}

/// Lock-poison-tolerant guard: a panicking task must not brick the whole cache.
fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match m.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}

/// Build a started, stateless CVM client transport to the server named by
/// `parsed.hex`. Returns the running client (for `call_tool`) and the
/// open-stream handle (grabbed before `serve` consumes the transport).
///
/// Relay URLs are always supplied explicitly — stage 1 of the SDK's CEP-17
/// resolution, which overrides any nprofile hints. Candidates come from the
/// nprofile hints when present, else the configured `relay_urls`; any candidate
/// that is one of the shim's own public URLs is rewritten to the colocated
/// relay's loopback address (`resolve_relay_urls`), so the bridge never dials
/// its own public URL (a hairpin round-trip that times out on most deploys).
/// Third-party relays pass through untouched.
async fn build_client(
    cfg: &ShimConfig,
    parsed: &ParsedPath,
    loopback_url: &Option<String>,
) -> anyhow::Result<(Client, ClientOpenStreamHandle)> {
    let tcfg = NostrClientTransportConfig::default()
        .with_server_pubkey(parsed.hex.clone())
        .with_encryption_mode(cfg.encryption_mode)
        .with_gift_wrap_mode(cfg.gift_wrap_mode)
        .with_stateless(true)
        .with_open_stream(OpenStreamConfig::enabled())
        .with_timeout(cfg.connect_timeout)
        .with_relay_urls(resolve_relay_urls(cfg, parsed, loopback_url));

    // Test injection: a mock relay pool replaces the real CVM transport
    // (network-free shim↔outlay hop, mirroring outlay's smoke tests).
    #[cfg(feature = "test-utils")]
    if let Some(pool) = cfg.test_relay_pool.clone() {
        let transport = NostrClientTransport::with_relay_pool(tcfg, pool).await?;
        let handle = transport.open_stream_handle();
        let client = tokio::time::timeout(cfg.connect_timeout, ShimClient.serve(transport))
            .await
            .map_err(|_| anyhow::anyhow!("client transport startup timed out"))??;
        return Ok((client, handle));
    }

    let signer = match &cfg.private_key {
        Some(k) => signer::from_sk(k)?,
        None => signer::generate(),
    };
    let transport = NostrClientTransport::new(signer, tcfg).await?;
    let handle = transport.open_stream_handle();
    // `serve` auto-starts the relay connection and drives the (emulated, since
    // stateless) handshake. Bound it so a stuck relay can't hang the caller.
    let client = tokio::time::timeout(cfg.connect_timeout, ShimClient.serve(transport))
        .await
        .map_err(|_| anyhow::anyhow!("client transport startup timed out"))??;
    Ok((client, handle))
}

/// Wrap a JSON object `Value` as rmcp tool arguments (`JsonObject`).
pub fn args(v: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
    match v {
        serde_json::Value::Object(m) => m,
        _ => serde_json::Map::new(),
    }
}

/// Pick the relay URLs the CVM transport will dial, applying the colocated-
/// relay loopback shortcut.
///
/// Candidates: the nprofile hints when present, else the configured `relay_urls`
/// (the fallback for hex/npub paths). Any candidate matching one of the shim's
/// own public URLs is rewritten to the loopback relay — the bridge must reach
/// the in-process relay directly, not hairpin through its own public URL.
/// Third-party relays are untouched, so nprofile hints to other relays keep
/// working.
///
/// `public_urls`: `OUTLAY_SHIM_PUBLIC_URLS` if set, else (when the colocated
/// relay is on) the configured `relay_urls` — the common collapse case where the
/// shim's transport relay IS its public face, so the default deployment works
/// with no extra config. When the colocated relay is off there is nothing to
/// shortcut to, so candidates are returned verbatim.
fn resolve_relay_urls(
    cfg: &ShimConfig,
    parsed: &ParsedPath,
    loopback_url: &Option<String>,
) -> Vec<String> {
    let candidates: Vec<String> = if !parsed.relay_hints.is_empty() {
        parsed.relay_hints.clone()
    } else {
        cfg.relay_urls.clone()
    };
    // Explicit allowlist wins; otherwise infer self from the configured relays
    // (only matters when the colocated relay is on — otherwise `substitute_self`
    // is a no-op anyway).
    let public_urls: &[String] = if !cfg.public_urls.is_empty() {
        &cfg.public_urls
    } else {
        &cfg.relay_urls
    };
    substitute_self(candidates, public_urls, loopback_url.as_deref())
}

/// Rewrite any URL in `candidates` that matches one of `public_urls` to
/// `loopback`. Comparison is normalized through `RelayUrl`
/// (trailing-slash- and default-port-tolerant). No-op when `loopback` is `None`
/// (colocated relay off).
// ponytail: normalization is shallow — `RelayUrl::as_str_without_trailing_slash`.
// A URL that fails to parse is left untouched rather than dropped; if exotic
// forms ( userinfo / non-default ports spelled differently ) ever fail to
// match, parse both sides through `Url` and compare host+port+scheme.
fn substitute_self(
    candidates: Vec<String>,
    public_urls: &[String],
    loopback: Option<&str>,
) -> Vec<String> {
    let Some(loopback) = loopback else {
        return candidates;
    };
    let public: HashSet<String> = public_urls.iter().filter_map(|u| norm_url(u)).collect();
    candidates
        .into_iter()
        .map(|u| match norm_url(&u) {
            Some(n) if public.contains(&n) => loopback.to_owned(),
            _ => u,
        })
        .collect()
}

/// Canonical form for relay-URL comparison: parsed through `RelayUrl`, trailing
/// slash stripped. `None` if `u` is not a valid relay URL.
fn norm_url(u: &str) -> Option<String> {
    RelayUrl::parse(u)
        .ok()
        .map(|r| r.as_str_without_trailing_slash().to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::read_shim_config;
    use std::collections::HashMap;

    fn cfg() -> ShimConfig {
        read_shim_config(&HashMap::new()).unwrap()
    }

    fn parsed(hints: &[&str]) -> ParsedPath {
        ParsedPath {
            hex: "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef".into(),
            relay_hints: hints.iter().map(|s| (*s).to_string()).collect(),
        }
    }

    const LOOPBACK: &str = "ws://127.0.0.1:8086/";

    #[test]
    fn substitute_noop_without_loopback() {
        let out = substitute_self(
            vec!["wss://nostr.wtf".into()],
            &["wss://nostr.wtf".into()],
            None,
        );
        assert_eq!(out, vec!["wss://nostr.wtf"]);
    }

    #[test]
    fn substitute_rewrites_self_url_to_loopback() {
        let out = substitute_self(
            vec!["wss://nostr.wtf".into()],
            &["wss://nostr.wtf".into()],
            Some(LOOPBACK),
        );
        assert_eq!(out, vec![LOOPBACK]);
    }

    #[test]
    fn substitute_leaves_third_party_relays_alone() {
        let out = substitute_self(
            vec!["wss://nostr.wtf".into(), "wss://relay.damus.io".into()],
            &["wss://nostr.wtf".into()],
            Some(LOOPBACK),
        );
        assert_eq!(out, vec![LOOPBACK, "wss://relay.damus.io"]);
    }

    #[test]
    fn substitute_tolerates_trailing_slash_and_case() {
        // Candidate with trailing slash matches a public URL without one.
        let out = substitute_self(
            vec!["wss://Nostr.WTF/".into()],
            &["wss://nostr.wtf".into()],
            Some(LOOPBACK),
        );
        assert_eq!(out, vec![LOOPBACK]);
    }

    #[test]
    fn resolve_no_hint_infers_self_from_relay_urls() {
        // Default deployment: colocated relay on, public_urls unset, relay_urls
        // = [nostr.wtf]. No hint => candidates = relay_urls => all rewritten to
        // loopback (the bridge dials its own relay in-process).
        let mut c = cfg();
        c.relay_urls = vec!["wss://nostr.wtf".into()];
        c.public_urls = vec![];
        let out = resolve_relay_urls(&c, &parsed(&[]), &Some(LOOPBACK.into()));
        assert_eq!(out, vec![LOOPBACK]);
    }

    #[test]
    fn resolve_hint_to_third_party_is_kept() {
        let mut c = cfg();
        c.relay_urls = vec!["wss://nostr.wtf".into()];
        c.public_urls = vec![];
        let out = resolve_relay_urls(
            &c,
            &parsed(&["wss://relay.damus.io"]),
            &Some(LOOPBACK.into()),
        );
        assert_eq!(out, vec!["wss://relay.damus.io"]);
    }

    #[test]
    fn resolve_hint_to_self_is_rewritten_others_kept() {
        let mut c = cfg();
        c.relay_urls = vec!["wss://nostr.wtf".into()];
        c.public_urls = vec!["wss://nostr.wtf".into()];
        let out = resolve_relay_urls(
            &c,
            &parsed(&["wss://nostr.wtf", "wss://relay.damus.io"]),
            &Some(LOOPBACK.into()),
        );
        assert_eq!(out, vec![LOOPBACK, "wss://relay.damus.io"]);
    }

    #[test]
    fn resolve_no_loopback_leaves_candidates_untouched() {
        // Colocated relay off => no rewrite, even when the URL is "self".
        let mut c = cfg();
        c.relay_urls = vec!["wss://nostr.wtf".into()];
        c.public_urls = vec!["wss://nostr.wtf".into()];
        let out = resolve_relay_urls(&c, &parsed(&[]), &None);
        assert_eq!(out, vec!["wss://nostr.wtf"]);
    }
}
