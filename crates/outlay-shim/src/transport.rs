//! CVM client transport construction and the per-outlay transport cache.
//!
//! `build_client` builds one stateless CVM client transport. `TransportCache`
//! shares ONE long-lived transport per outlay identity across every WS
//! connection and NIP-11 request, keyed by hex pubkey (so hex / npub / nprofile
//! of the same identity collapse to one entry). Sharing is the fix for the
//! per-connection churn that caused CVM open-stream cross-delivery, progress-
//! token collisions, and stored-frame replay (design/shim.md §5). Stateless mode
//! skips the `initialize` handshake (design/shim.md §6).

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use contextvm_sdk::transport::client::{NostrClientTransport, NostrClientTransportConfig};
use contextvm_sdk::transport::open_stream::OpenStreamConfig;
use contextvm_sdk::{signer, ClientOpenStreamHandle};
use lru::LruCache;
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
}

struct SharedTransport {
    /// Drives the transport for the entry's lifetime. Read on every `get` to
    /// hand out fresh `Peer` clones; never explicitly cancelled.
    client: Client,
    handle: ClientOpenStreamHandle,
}

impl TransportCache {
    pub fn new(cap: NonZeroUsize) -> Self {
        Self {
            entries: Mutex::new(LruCache::new(cap)),
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
                let (client, handle) = build_client(cfg, parsed).await?;
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
/// `parsed.raw`. Returns the running client (for `call_tool`) and the
/// open-stream handle (grabbed before `serve` consumes the transport).
///
/// `relay_urls` is left empty when the path is an nprofile carrying its own
/// hints, so the SDK resolves via those hints; otherwise the env relays are
/// used.
async fn build_client(
    cfg: &ShimConfig,
    parsed: &ParsedPath,
) -> anyhow::Result<(Client, ClientOpenStreamHandle)> {
    let mut tcfg = NostrClientTransportConfig::default()
        .with_server_pubkey(parsed.raw.clone())
        .with_encryption_mode(cfg.encryption_mode)
        .with_gift_wrap_mode(cfg.gift_wrap_mode)
        .with_stateless(true)
        .with_open_stream(OpenStreamConfig::enabled())
        .with_timeout(cfg.connect_timeout);
    if !parsed.has_relay_hints {
        tcfg = tcfg.with_relay_urls(cfg.relay_urls.clone());
    }

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
