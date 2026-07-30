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
    let app = server::router(server::AppState::new(cfg));

    let listener = tokio::net::TcpListener::bind(&listen).await?;
    tracing::info!(
        "outlay-shim listening on http://{listen} — vanilla NIP-01 clients connect at ws://{listen}/<server-pubkey>"
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
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
