//! CVM client transport construction — shared by the WS connection handler (one
//! transport per connection) and the HTTP NIP-11 path (transient per request).
//! Stateless mode skips the `initialize` handshake (design/shim.md §6).

use contextvm_sdk::transport::client::{NostrClientTransport, NostrClientTransportConfig};
use contextvm_sdk::transport::open_stream::OpenStreamConfig;
use contextvm_sdk::{signer, ClientOpenStreamHandle, GiftWrapMode};
use rmcp::{ClientHandler, RoleClient, ServiceExt};

use crate::config::ShimConfig;
use crate::path::ParsedPath;

/// Minimal rmcp client handler — the shim only makes tool calls, it serves none.
#[derive(Clone, Default)]
pub struct ShimClient;
impl ClientHandler for ShimClient {}

pub type Client = rmcp::service::RunningService<RoleClient, ShimClient>;

/// Build a started, stateless CVM client transport to the server named by
/// `parsed.raw`. Returns the running client (for `call_tool`) and the
/// open-stream handle (grabbed before `serve` consumes the transport).
///
/// `relay_urls` is left empty when the path is an nprofile carrying its own
/// hints, so the SDK resolves via those hints; otherwise the env relays are
/// used.
pub async fn build_client(
    cfg: &ShimConfig,
    parsed: &ParsedPath,
) -> anyhow::Result<(Client, ClientOpenStreamHandle)> {
    let mut tcfg = NostrClientTransportConfig::default()
        .with_server_pubkey(parsed.raw.clone())
        .with_encryption_mode(cfg.encryption_mode)
        .with_gift_wrap_mode(GiftWrapMode::Optional)
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
