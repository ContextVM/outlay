//! axum routes. `/<pubkey>` dispatches WS-upgrade vs HTTP-NIP-11 on the
//! `Upgrade` header (mirroring nostr-rs-relay's `(path, has_upgrade)` split);
//! `/` serves the synthesized shim doc. HTTP-only.

use axum::extract::ws::WebSocketUpgrade;
use axum::extract::{FromRequest, Path, Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;
use std::num::NonZeroUsize;
use std::sync::Arc;

use crate::config::ShimConfig;
use crate::conn;
use crate::nip11;
use crate::transport::TransportCache;

#[derive(Clone)]
pub struct AppState {
    pub config: ShimConfig,
    /// One shared CVM transport per outlay identity, reused across all
    /// connections/requests to it.
    pub transports: Arc<TransportCache>,
}

impl AppState {
    pub fn new(config: ShimConfig) -> Self {
        // `read_shim_config` floors this to >= 1; the cap is NonZero so
        // `LruCache::new` can't panic on 0.
        let cap = NonZeroUsize::new(config.max_cached_outlays)
            .expect("config floors max_cached_outlays to >= 1");
        Self {
            config,
            transports: Arc::new(TransportCache::new(cap)),
        }
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", any(root_handler))
        .route("/{pubkey}", any(pubkey_handler))
        .with_state(state)
}

async fn root_handler(headers: HeaderMap) -> Response {
    nip11::serve_root(&headers).await
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
