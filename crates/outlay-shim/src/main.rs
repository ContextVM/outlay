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
    let app = server::router(server::AppState { config: cfg });

    let listener = tokio::net::TcpListener::bind(&listen).await?;
    tracing::info!(
        "outlay-shim listening on http://{listen} — vanilla NIP-01 clients connect at ws://{listen}/<server-pubkey>"
    );
    axum::serve(listener, app).await?;
    Ok(())
}
