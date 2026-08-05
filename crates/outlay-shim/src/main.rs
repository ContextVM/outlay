//! outlay-shim bin: config → router → axum serve. The real logic lives in the
//! library; this is a thin entry point.

use outlay_shim::{config, server};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = config::load()?;

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new(
                    "info,outlay_shim=debug,contextvm_sdk=warn,rmcp=warn,h2=warn,hyper=warn",
                )
            }),
        )
        .init();

    let listen = cfg.listen_addr.clone();
    // Capture the public face before `cfg` moves into AppState. Shown in the
    // banner as the https URL to open in a browser.
    let public_page = cfg.public_url().map(https_url);

    // Colocated memoryless relay at `/`: lets outlays collapse their CVM
    // transport relay into the shim. Soft-fail: if it can't bind, the bridge
    // (the shim's primary role) still serves.
    let enable_relay = cfg.enable_relay;
    let relay_handle = if enable_relay {
        match outlay_shim::relay::spawn().await {
            Ok(h) => {
                tracing::info!(
                    relay_url = %h.url(),
                    "memoryless relay endpoint enabled at / (outlays may use it as their CVM transport relay)"
                );
                tracing::info!(
                    "bridge loopback shortcut active: relay URLs matching this shim are dialed over loopback. \
                     With the relay on, OUTLAY_SHIM_CVM_RELAYS must be this shim's own public URL; \
                     set OUTLAY_SHIM_RELAY=false to use a third-party transport relay"
                );
                Some(h)
            }
            Err(e) => {
                tracing::error!(
                    error = ?e,
                    "failed to start the memoryless relay; continuing without it (the bridge still serves)"
                );
                None
            }
        }
    } else {
        tracing::info!("memoryless relay endpoint disabled (OUTLAY_SHIM_RELAY=false)");
        None
    };
    let relay_url = relay_handle.as_ref().map(|h| h.url().to_owned());

    let app = server::router(server::AppState::new(cfg, relay_url));

    let listener = tokio::net::TcpListener::bind(&listen).await?;
    tracing::info!(
        "outlay-shim listening on http://{listen} — vanilla NIP-01 clients connect at ws://{listen}/<server-pubkey>"
    );
    if let Some(page) = public_page.as_deref() {
        tracing::info!("public page: {page}  (open an outlay at {page}/<server-pubkey>)");
    }
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    // Drop the relay handle on shutdown for a clean stop (process exit would
    // also tear it down).
    drop(relay_handle);
    Ok(())
}

/// Handle SIGINT/SIGTERM so the process actually exits when signalled. Without
/// an explicit handler the kernel drops terminate-signals delivered to PID 1
/// (the binary's role in a container without `--init`), so `docker run` Ctrl+C
/// appears to hang until Docker force-KILLs after the 3rd interrupt. Mirrors
/// `outlay`'s handler; the two crates share no code by design.
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

/// Swap a WebSocket scheme to its HTTP/HTTPS equivalent for display — the shim
/// serves the same host over both behind a reverse proxy. Leaves `http(s)://`
/// as-is; defaults to `https://` for anything else.
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
        assert_eq!(https_url("ws://x:8080"), "http://x:8080");
        assert_eq!(https_url("https://nostr.wtf"), "https://nostr.wtf");
        assert_eq!(https_url("nostr.wtf"), "https://nostr.wtf");
    }
}
