//! axum routes. `/<pubkey>` dispatches WS-upgrade vs HTTP-NIP-11 on the
//! `Upgrade` header (mirroring nostr-rs-relay's `(path, has_upgrade)` split);
//! `/` serves the synthesized shim doc. HTTP-only.

use axum::extract::ws::WebSocketUpgrade;
use axum::extract::{FromRequest, Path, Request, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;
use std::num::NonZeroUsize;
use std::sync::Arc;

use crate::config::ShimConfig;
use crate::conn;
use crate::nip11;
use crate::relay;
use crate::transport::TransportCache;

#[derive(Clone)]
pub struct AppState {
    pub config: ShimConfig,
    /// One shared CVM transport per outlay identity, reused across all
    /// connections/requests to it.
    pub transports: Arc<TransportCache>,
    /// Loopback URL of the colocated memoryless relay (`ws://127.0.0.1:<port>/`),
    /// piped from the `/` WS upgrade. `None` when the relay endpoint is disabled.
    pub relay_url: Option<String>,
}

impl AppState {
    pub fn new(config: ShimConfig, relay_url: Option<String>) -> Self {
        // `read_shim_config` floors this to >= 1; the cap is NonZero so
        // `LruCache::new` can't panic on 0.
        let cap = NonZeroUsize::new(config.max_cached_outlays)
            .expect("config floors max_cached_outlays to >= 1");
        Self {
            config,
            transports: Arc::new(TransportCache::new(cap)),
            relay_url,
        }
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", any(root_handler))
        .route("/{pubkey}", any(pubkey_handler))
        .with_state(state)
}

async fn root_handler(State(state): State<AppState>, req: Request) -> Response {
    // WS upgrade → relay pipe (when enabled); otherwise the existing HTTP
    // NIP-11. Same `Upgrade`-header dispatch `pubkey_handler` uses for the bridge.
    if state.relay_url.is_some() && req.headers().get(header::UPGRADE).is_some() {
        let relay_url = state.relay_url.clone().expect("checked Some above");
        match WebSocketUpgrade::from_request(req, &state).await {
            Ok(ws) => ws
                .max_message_size(state.config.max_ws_message_bytes)
                .max_frame_size(state.config.max_ws_message_bytes)
                .on_upgrade(move |socket| relay::forward(socket, relay_url))
                .into_response(),
            Err(rej) => (StatusCode::BAD_REQUEST, rej.to_string()).into_response(),
        }
    } else {
        nip11::serve_root(req.headers()).await
    }
}

/// One path, two transports: WS upgrade if the `Upgrade` header is present,
/// otherwise HTTP NIP-11 (content-negotiated). Same dispatch nostr-rs-relay uses.
async fn pubkey_handler(
    State(state): State<AppState>,
    Path(pubkey): Path<String>,
    req: Request,
) -> Response {
    let is_upgrade = req.headers().get(header::UPGRADE).is_some();
    if is_upgrade {
        match WebSocketUpgrade::from_request(req, &state).await {
            Ok(ws) => ws
                .max_message_size(state.config.max_ws_message_bytes)
                .max_frame_size(state.config.max_ws_message_bytes)
                .on_upgrade(move |socket| conn::handle_ws(socket, state, pubkey))
                .into_response(),
            Err(rej) => (StatusCode::BAD_REQUEST, rej.to_string()).into_response(),
        }
    } else {
        nip11::serve_pubkey(&state.transports, &state.config, req.headers(), &pubkey)
            .await
            .into_response()
    }
}
