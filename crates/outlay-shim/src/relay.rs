//! The colocated memoryless relay endpoint at `/`.
//!
//! Spawns an [`outlay_relay::MemorylessDatabase`]-backed `LocalRelay` on loopback
//! at startup and serves it at `/` by piping each upgraded WS connection verbatim
//! to it. This lets outlays collapse their CVM transport relay into the shim
//! (one fewer network hop) by advertising the shim's public URL as their relay
//! hint; outlays that override `OUTLAY_RELAY_URLS` to a third-party relay are
//! unaffected.
//!
//! `LocalRelay` owns every NIP-01 semantic (REQ/EVENT/CLOSE/EOSE/OK/NOTICE,
//! filter matching, REQ-replace, rate limits, id verification, broadcast
//! backpressure). The pipe is a dumb byte forwarder — it interprets nothing.

use anyhow::{Context, Result};
use axum::extract::ws::Message as AxumMsg;
use axum::extract::ws::WebSocket;
use futures::{SinkExt, StreamExt};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message as TgMsg;

/// Spawn the memoryless relay on loopback (OS-assigned port). Returns its
/// `ws://127.0.0.1:<port>/` URL, which the axum `/` upgrade pipes to. The
/// returned [`outlay_relay::BundledRelay`] must be held alive for as long as the
/// relay should serve (process lifetime); dropping it shuts the relay down.
pub async fn spawn() -> Result<outlay_relay::BundledRelay> {
    outlay_relay::BundledRelay::spawn_memoryless(0)
        .await
        .context("starting memoryless relay")
}

/// Forward one upgraded client WS connection to the loopback relay verbatim,
/// both directions, until either side closes. No interpretation: the relay on
/// the other end owns all NIP-01 semantics.
pub async fn forward(socket: WebSocket, relay_url: String) {
    let (mut axum_sink, mut axum_stream) = socket.split();

    let upstream = match connect_async(relay_url).await {
        Ok((s, _)) => s,
        Err(e) => {
            tracing::warn!(error = %e, "relay pipe: connect to loopback relay failed");
            return;
        }
    };
    let (mut tg_sink, mut tg_stream) = upstream.split();

    loop {
        tokio::select! {
            msg = axum_stream.next() => match msg {
                Some(Ok(m)) => {
                    if tg_sink.send(to_tungstenite(m)).await.is_err() {
                        break;
                    }
                }
                _ => break,
            },
            msg = tg_stream.next() => match msg {
                Some(Ok(m)) => match from_tungstenite(m) {
                    Some(a) => {
                        if axum_sink.send(a).await.is_err() {
                            break;
                        }
                    }
                    // Close / raw frame ⇒ end the pipe.
                    None => break,
                },
                _ => break,
            },
        }
    }

    let _ = tg_sink.close().await;
    let _ = axum_sink.close().await;
}

/// axum `Message` → tungstenite `Message`. `Binary`/`Ping`/`Pong` are the same
/// `bytes::Bytes` on both sides (axum 0.8 and tungstenite 0.29 share it); only
/// `Text` hops through a `String` because the two `Utf8Bytes` wrapper types
/// differ.
fn to_tungstenite(m: AxumMsg) -> TgMsg {
    match m {
        AxumMsg::Text(t) => TgMsg::Text(t.to_string().into()),
        AxumMsg::Binary(b) => TgMsg::Binary(b),
        AxumMsg::Ping(b) => TgMsg::Ping(b),
        AxumMsg::Pong(b) => TgMsg::Pong(b),
        AxumMsg::Close(_) => TgMsg::Close(None),
    }
}

/// tungstenite `Message` → axum `Message`. `None` for `Close` / raw `Frame`
/// signals the caller to end the pipe.
fn from_tungstenite(m: TgMsg) -> Option<AxumMsg> {
    match m {
        TgMsg::Text(t) => Some(AxumMsg::Text(t.to_string().into())),
        TgMsg::Binary(b) => Some(AxumMsg::Binary(b)),
        TgMsg::Ping(b) => Some(AxumMsg::Ping(b)),
        TgMsg::Pong(b) => Some(AxumMsg::Pong(b)),
        TgMsg::Close(_) => None,
        TgMsg::Frame(_) => None,
    }
}

#[cfg(all(test, not(feature = "test-utils")))]
mod tests {
    // The `from_tungstenite`/`to_tungstenite` round-trip is the only pure logic;
    // the live path is exercised by the `test-utils` smoke test.
    use super::*;

    #[test]
    fn text_roundtrips_via_string() {
        let a = AxumMsg::Text("hello".into());
        let t = to_tungstenite(a);
        let TgMsg::Text(s) = t else {
            panic!("expected Text");
        };
        assert_eq!(s.as_str(), "hello");
        let back = from_tungstenite(TgMsg::Text("world".into())).unwrap();
        match back {
            AxumMsg::Text(s) => assert_eq!(s.as_str(), "world"),
            _ => panic!("expected Text"),
        }
    }

    #[test]
    fn close_signals_end_of_pipe() {
        assert!(from_tungstenite(TgMsg::Close(None)).is_none());
    }
}
