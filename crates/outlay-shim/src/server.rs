//! axum routes. `/<pubkey>` dispatches WS-upgrade vs HTTP-NIP-11 on the
//! `Upgrade` header (mirroring nostr-rs-relay's `(path, has_upgrade)` split);
//! `/` serves the synthesized shim doc. HTTP-only.

use axum::extract::ws::WebSocketUpgrade;
use axum::extract::{FromRequest, Path, Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;

use crate::config::ShimConfig;
use crate::conn;
use crate::nip11;

#[derive(Clone)]
pub struct AppState {
    pub config: ShimConfig,
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
                .on_upgrade(move |socket| conn::handle_ws(socket, state.config.clone(), pubkey))
                .into_response(),
            Err(rej) => (StatusCode::BAD_REQUEST, rej.to_string()).into_response(),
        }
    } else {
        nip11::serve_pubkey(&state.config, req.headers(), &pubkey)
            .await
            .into_response()
    }
}
