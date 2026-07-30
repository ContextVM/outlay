//! Per-connection WS bridge: vanilla NIP-01 client ↔ outlay CVM server.
//!
//! Single-writer design (design/shim.md §5): `socket.split()` yields a sink and
//! a stream; one writer task drains an mpsc into the sink, the main loop reads
//! inbound frames and spawns per-subscribe / per-publish workers that feed the
//! same channel. Dropping the loop cancels everything.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use contextvm_sdk::{call_tool_stream, ClientOpenStreamHandle};
use futures::{SinkExt, StreamExt};
use rmcp::model::CallToolRequestParams;
use rmcp::service::Peer;
use rmcp::RoleClient;
use serde_json::Value;
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;

use crate::path::parse_path;
use crate::server::AppState;
use crate::translate::{self, ClientMsg};
use crate::transport::args;

const OUTBOUND_BUF: usize = 256;

pub async fn handle_ws(mut socket: WebSocket, state: AppState, pubkey: String) {
    let parsed = match parse_path(&pubkey) {
        Ok(p) => p,
        Err(e) => {
            close_with_notice(&mut socket, &e.to_string()).await;
            return;
        }
    };

    // One shared, long-lived transport per outlay identity (see `TransportCache`).
    // The transport outlives this connection, so there is no per-connection
    // `cancel()` — only per-subscribe teardown.
    let (peer, handle) = match state.transports.get(&state.config, &parsed).await {
        Ok(v) => v,
        Err(e) => {
            close_with_notice(&mut socket, &e.to_string()).await;
            return;
        }
    };

    let (sink, mut stream) = socket.split();
    let (tx, rx) = mpsc::channel::<Message>(OUTBOUND_BUF);
    let writer: JoinHandle<()> = tokio::spawn(async move {
        let mut sink = sink;
        let mut rx = rx;
        while let Some(msg) = rx.recv().await {
            if sink.send(msg).await.is_err() {
                break;
            }
        }
        let _ = sink.close().await;
    });

    // sub_id → (cancel signal, the spawned task owning that subscribe call). On
    // NIP-01 CLOSE the signal fires and the task tears the stream down via the
    // handle's Send-safe `cancel(token)`, publishing an abort frame so outlay
    // unsubscribes the upstream promptly. The plain alternative — abort the task
    // and drop `call` — also works, but outlay only notices the dropped reader
    // when its keepalive probe times out (up to the configured 60 s), leaving
    // the upstream subscription lingering. (`ToolStreamCall::abort()` can't be
    // awaited here: the call is `!Sync` via `result: BoxFuture`, so `&call`
    // isn't `Send`; the `ClientOpenStreamHandle` is `Sync`.)
    let mut subs: HashMap<String, (Arc<Notify>, JoinHandle<()>)> = HashMap::new();

    while let Some(frame) = stream.next().await {
        let text = match frame {
            Ok(Message::Text(t)) => t.as_str().to_owned(),
            Ok(Message::Close(_)) | Err(_) => break,
            // Ping/Pong/Binary: tungstenite auto-pongs; ignore.
            Ok(_) => continue,
        };
        match translate::parse_client_frame(&text) {
            Some(ClientMsg::Req { sub_id, filters }) => {
                // NIP-01 REQ-on-existing-sub == replace: cancel the old one first.
                if let Some((cancel, _)) = subs.remove(&sub_id) {
                    cancel.notify_one();
                }
                let cancel = Arc::new(Notify::new());
                let task = tokio::spawn(subscribe_loop(
                    peer.clone(),
                    handle.clone(),
                    sub_id.clone(),
                    filters,
                    tx.clone(),
                    cancel.clone(),
                ));
                subs.insert(sub_id, (cancel, task));
            }
            Some(ClientMsg::Close { sub_id }) => {
                if let Some((cancel, _)) = subs.remove(&sub_id) {
                    cancel.notify_one();
                }
            }
            Some(ClientMsg::Publish(event)) => {
                tokio::spawn(publish_once(peer.clone(), event, tx.clone()));
            }
            None => {
                let _ = tx
                    .send(Message::text(translate::notice_frame("unrecognized frame")))
                    .await;
            }
        }
    }

    // Cleanup: cancel every subscribe (prompt reader-session teardown via the
    // shared handle) and end the writer. The shared transport is NOT cancelled —
    // it outlives this connection and serves other connections to the same outlay.
    for (cancel, _) in subs.values() {
        cancel.notify_one();
    }
    drop(subs);
    drop(tx);
    let _ = writer.await;
}

/// One CEP-41 `subscribe` call, pumping NIP-01 frame chunks into the channel
/// until the stream ends, errors, or `cancel` is signaled. On cancel it tears
/// the stream down via `handle.cancel(token)` so outlay unsubscribes the
/// upstream promptly instead of waiting on its keepalive probe.
async fn subscribe_loop(
    peer: Peer<RoleClient>,
    handle: ClientOpenStreamHandle,
    sub_id: String,
    filters: Vec<Value>,
    tx: mpsc::Sender<Message>,
    cancel: Arc<Notify>,
) {
    let params = CallToolRequestParams::new("subscribe").with_arguments(args(serde_json::json!({
        "subscription_id": sub_id,
        "filters": filters,
    })));
    // Establish the stream, bailing out if a CLOSE/disconnect beats setup (there
    // is nothing to cancel yet — the connect is simply dropped).
    let mut call = tokio::select! {
        r = call_tool_stream(&peer, &handle, params) => match r {
            Ok(c) => c,
            Err(e) => {
                let _ = tx
                    .send(Message::text(translate::closed_frame(
                        &sub_id,
                        &e.to_string(),
                    )))
                    .await;
                return;
            }
        },
        _ = cancel.notified() => return,
    };
    loop {
        tokio::select! {
            chunk = call.stream.next() => match chunk {
                Some(Ok(c)) => {
                    if tx.send(Message::text(c)).await.is_err() {
                        return;
                    }
                }
                Some(Err(e)) => {
                    let _ = tx
                        .send(Message::text(translate::closed_frame(
                            &sub_id,
                            &e.to_string(),
                        )))
                        .await;
                    return;
                }
                None => return,
            },
            _ = cancel.notified() => {
                let _ = handle
                    .cancel(
                        &call.progress_token,
                        Some("client closed subscription".to_string()),
                    )
                    .await;
                return;
            }
        }
    }
}

/// One synchronous `publish_event` call → synthesize the NIP-01 `OK` frame.
async fn publish_once(peer: Peer<RoleClient>, event: Value, tx: mpsc::Sender<Message>) {
    let id = event
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();
    let params = CallToolRequestParams::new("publish_event")
        .with_arguments(args(serde_json::json!({ "event": event })));
    let frame = match peer.call_tool(params).await {
        Ok(r) => {
            let sc = r.structured_content.unwrap_or(Value::Null);
            let ok = sc.get("ok").and_then(Value::as_bool).unwrap_or(false);
            let msg = sc.get("message").and_then(Value::as_str).unwrap_or("");
            let ok_id = sc.get("event_id").and_then(Value::as_str).unwrap_or(&id);
            translate::ok_frame(ok_id, ok, msg)
        }
        Err(e) => translate::ok_frame(&id, false, &e.to_string()),
    };
    let _ = tx.send(Message::text(frame)).await;
}

async fn close_with_notice(socket: &mut WebSocket, msg: &str) {
    let _ = socket
        .send(Message::text(translate::notice_frame(&format!(
            "error: {msg}"
        ))))
        .await;
    let _ = socket.close().await;
}
