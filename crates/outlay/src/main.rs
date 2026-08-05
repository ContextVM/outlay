//! outlay bin entrypoint — loads config, connects the upstream relay pool,
//! wires the ContextVM rmcp server (CEP-41 open-stream enabled) over the Nostr
//! transport, and runs until shutdown.

use anyhow::{Context, Result};
use contextvm_sdk::transport::open_stream::OpenStreamConfig;
use contextvm_sdk::transport::server::{NostrServerTransport, NostrServerTransportConfig};
use contextvm_sdk::{signer, EncryptionMode, ServerInfo};
use rmcp::ServiceExt;

use outlay::config::{self, ServerConfig};
use outlay::handler::OutlayServer;
use outlay::proxy::Proxy;

fn build_transport_config(cfg: &ServerConfig) -> NostrServerTransportConfig {
    let server_info = ServerInfo::default()
        .with_name(cfg.server_name.clone())
        .with_about(cfg.server_about.clone().unwrap_or_default());

    NostrServerTransportConfig::default()
        .with_relay_urls(cfg.cvm_relay_urls.clone())
        .with_announced_server(cfg.is_announced)
        .with_encryption_mode(EncryptionMode::Optional)
        .with_gift_wrap_mode(cfg.gift_wrap_mode)
        .with_server_info(server_info)
        // CEP-41 open-stream is the backbone of `subscribe` (one call == one
        // NIP-01 subscription). Opt in, with bumped keepalives mirroring
        // cordn: the SDK default (30s idle + 20s probe = 50s) is too short for
        // a long-lived subscription that can sit idle between events.
        .with_open_stream(
            OpenStreamConfig::default()
                .with_enabled(true)
                .with_idle_timeout_ms(60_000)
                .with_probe_timeout_ms(60_000),
        )
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                // `nostr_relay_pool` is included so the upstream + CVM relay
                // connection lifecycle is visible: nostr-sdk connects in the
                // background and logs "Connected to '<url>'" under this target.
                tracing_subscriber::EnvFilter::new(
                    "outlay=info,contextvm_sdk=info,nostr_relay_pool=info,nostr=info,rmcp=warn",
                )
            }),
        )
        .init();

    let cfg = config::load().context("reading outlay server config")?;

    let signer = match &cfg.private_key_hex {
        Some(hex) => signer::from_sk(hex).context("OUTLAY_SERVER_PRIVATE_KEY")?,
        None => signer::generate(),
    };
    let server_pubkey = signer.public_key().to_hex();

    // Upstream: the bundled in-process relay runs by default (self-contained).
    // Set OUTLAY_PROXY_RELAY_URL to proxy an external relay instead.
    #[cfg(feature = "bundled-relay")]
    let bundled_relay = if cfg.proxy_relay_url.is_none() {
        let backend = match cfg.bundled.backend.as_str() {
            "memory" => outlay_relay::Backend::Memory,
            // Anything else (incl. the default "sqlite") → persistent SQLite.
            _ => outlay_relay::Backend::Sqlite,
        };
        Some(
            outlay_relay::BundledRelay::spawn(
                backend,
                Some(std::path::Path::new(&cfg.bundled.db_path)),
                cfg.bundled.port,
            )
            .await
            .context("starting bundled relay")?,
        )
    } else {
        None
    };

    #[cfg(feature = "bundled-relay")]
    let upstream_url: String = match &bundled_relay {
        Some(b) => b.url().to_owned(),
        // Proxy mode: OUTLAY_PROXY_RELAY_URL is set (config validated Some here).
        None => cfg.proxy_relay_url.clone().expect("external upstream"),
    };
    #[cfg(not(feature = "bundled-relay"))]
    let upstream_url: String = cfg.proxy_relay_url.clone().expect("external upstream");

    // Upstream: connect the proxy relay pool (background; status logged async).
    let proxy = Proxy::new(upstream_url.clone())
        .await
        .with_context(|| format!("connecting upstream relay {upstream_url}"))?;

    print_banner(&server_pubkey, &cfg, &upstream_url);

    let transport = NostrServerTransport::new(signer, build_transport_config(&cfg))
        .await
        .context("connecting ContextVM server transport")?;

    tracing::info!(
        upstream = %upstream_url,
        mode = if cfg.proxy_relay_url.is_none() { "bundled" } else { "proxy" },
        cvm_relays = ?cfg.cvm_relay_urls,
        announced = cfg.is_announced,
        server_pubkey = %server_pubkey,
        "outlay server starting (relay connections complete in the background)",
    );

    let service = OutlayServer::new(std::sync::Arc::new(proxy))
        .serve(transport)
        .await?;

    tokio::select! {
        result = service.waiting() => {
            if let Err(e) = result {
                tracing::error!(error = ?e, "server service exited with error");
            }
        }
        _ = shutdown_signal() => {
            tracing::info!("outlay server shutting down");
        }
    }

    #[cfg(feature = "bundled-relay")]
    if let Some(b) = &bundled_relay {
        b.shutdown();
    }
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        ctrl_c.await;
    }
}

fn print_banner(server_pubkey: &str, cfg: &ServerConfig, upstream_url: &str) {
    let rule: String = "═".repeat(64);
    let cvm_lines = cfg
        .cvm_relay_urls
        .iter()
        .map(|r| format!("     • {r}"))
        .collect::<Vec<_>>()
        .join("\n");
    // The shim's page for this outlay: first CVM relay (== the shim's public
    // host in the collapsed deploy), scheme-swapped to https, + this server's
    // pubkey. `None` only if no CVM relay is configured (never, given defaults).
    let page_url = cfg
        .cvm_relay_urls
        .first()
        .map(|r| format!("{}/{}", https_url(r), server_pubkey));
    println!();
    println!("  {rule}");
    println!("   outlay — Nostr relay over ContextVM");
    println!("  {rule}");
    println!();
    println!("   server pubkey   {server_pubkey}");
    if let Some(page) = page_url.as_deref() {
        println!("   page            {page}");
    }
    println!();
    println!("   CVM relays (listening)");
    println!("{cvm_lines}");
    println!();
    println!("   upstream relay (proxied)");
    println!("     • {upstream_url}");
    println!();
}

/// Swap a WebSocket scheme to its HTTP/HTTPS equivalent for display — the CVM
/// relay and its shim page are served on the same host behind a reverse proxy.
/// Leaves `http(s)://` as-is; defaults to `https://`.
fn https_url(url: &str) -> String {
    if let Some(rest) = url.strip_prefix("wss://") {
        format!("https://{rest}")
    } else if let Some(rest) = url.strip_prefix("ws://") {
        format!("http://{rest}")
    } else if url.starts_with("https://") || url.starts_with("http://") {
        url.to_owned()
    } else {
        format!("https://{url}")
    }
}

#[cfg(test)]
mod tests {
    use super::https_url;

    #[test]
    fn https_url_swaps_ws_schemes() {
        assert_eq!(https_url("wss://nostr.wtf"), "https://nostr.wtf");
        assert_eq!(https_url("ws://127.0.0.1:8086"), "http://127.0.0.1:8086");
        assert_eq!(https_url("https://nostr.wtf"), "https://nostr.wtf");
        assert_eq!(https_url("nostr.wtf"), "https://nostr.wtf");
    }
}
