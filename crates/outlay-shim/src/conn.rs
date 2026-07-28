//! Per-connection WS bridge: vanilla NIP-01 client ↔ outlay CVM server.
//!
//! Single-writer design (design/shim.md §5): `socket.split()` yields a sink and
//! a stream; one writer task drains an mpsc into the sink, the main loop reads
//! inbound frames and spawns per-subscribe / per-publish workers that feed the
//! same channel. Dropping the loop cancels everything.

use std::collections::HashMap;

use axum::extract::ws::{Message, WebSocket};
use contextvm_sdk::{call_tool_stream, ClientOpenStreamHandle};
use futures::{SinkExt, StreamExt};
use rmcp::model::CallToolRequestParams;
use rmcp::service::Peer;
use rmcp::RoleClient;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::config::ShimConfig;
use crate::path::parse_path;
use crate::translate::{self, ClientMsg};
use crate::transport::{self, build_client};

const OUTBOUND_BUF: usize = 256;

pub async fn handle_ws(mut socket: WebSocket, config: ShimConfig, pubkey: String) {
    let parsed = match parse_path(&pubkey) {
        Ok(p) => p,
        Err(e) => {
            close_with_notice(&mut socket, &e.to_string()).await;
            return;
        }
    };

    let (client, handle) = match build_client(&config, &parsed).await {
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

    // sub_id → the spawned task owning that subscribe call. Aborting the
    // handle drops `call` (the CEP-41 consumer session); outlay then closes the
    // upstream subscription when its open-stream keepalive notices the dead
    // reader. (An immediate explicit `call.abort()` can't be awaited inside a
    // `tokio::spawn` — `ToolStreamCall` is `!Sync` via its `result: BoxFuture`,
    // so `&call` isn't `Send`. Upgrade path: a Send-safe cancel-by-token API.)
    let mut subs: HashMap<String, JoinHandle<()>> = HashMap::new();
    let peer = client.peer();

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
                if let Some(h) = subs.remove(&sub_id) {
                    h.abort();
                }
                let task = tokio::spawn(subscribe_loop(
                    peer.clone(),
                    handle.clone(),
                    sub_id.clone(),
                    filters,
                    tx.clone(),
                ));
                subs.insert(sub_id, task);
            }
            Some(ClientMsg::Close { sub_id }) => {
                if let Some(h) = subs.remove(&sub_id) {
                    h.abort();
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

    // Cleanup: abort every subscribe, end the writer, cancel the CVM client.
    for h in subs.values() {
        h.abort();
    }
    drop(subs);
    drop(tx);
    let _ = writer.await;
    let _ = client.cancel().await;
}

/// One CEP-41 `subscribe` call, pumping NIP-01 frame chunks into the channel
/// until the stream ends or errors. Cancellation is external (the caller aborts
/// this task's `JoinHandle`).
async fn subscribe_loop(
    peer: Peer<RoleClient>,
    handle: ClientOpenStreamHandle,
    sub_id: String,
    filters: Vec<Value>,
    tx: mpsc::Sender<Message>,
) {
    let params = CallToolRequestParams::new("subscribe").with_arguments(transport::args(
        serde_json::json!({
            "subscription_id": sub_id,
            "filters": filters,
        }),
    ));
    let mut call = match call_tool_stream(&peer, &handle, params).await {
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
    };
    // Dropping `call` on task-abort (or natural return) tears down the CEP-41
    // consumer; the upstream CLOSE rides outlay's open-stream keepalive.
    while let Some(chunk) = call.stream.next().await {
        match chunk {
            Ok(c) => {
                if tx.send(Message::text(c)).await.is_err() {
                    return;
                }
            }
            Err(e) => {
                let _ = tx
                    .send(Message::text(translate::closed_frame(
                        &sub_id,
                        &e.to_string(),
                    )))
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
        .with_arguments(transport::args(serde_json::json!({ "event": event })));
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
