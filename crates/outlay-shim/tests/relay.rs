//! End-to-end test for the colocated memoryless relay endpoint at `/`.
//!
//! A vanilla NIP-01 WS client connects to the shim's `/` (axum upgrade → frame
//! pipe → memoryless `LocalRelay`), subscribes, publishes, and observes the
//! memoryless contract:
//!  - an immediate `EOSE` on REQ (nothing stored),
//!  - a live `EVENT` for the just-published event,
//!  - an `OK true` ack,
//!  - and **no backfill** when a second REQ opens after the publish.
//!
//! Network-free (loopback only). Not gated on `test-utils`: it exercises the
//! relay endpoint, not the CVM transport, so it needs no `MockRelayPool`.

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use nostr_sdk::prelude::*;
use outlay_shim::{config::ShimConfig, relay, server};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use contextvm_sdk::{EncryptionMode, GiftWrapMode};

/// Read one text frame from the WS within `timeout`, panicking on timeout.
async fn recv_text(
    rx: &mut futures::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
    timeout: Duration,
    label: &str,
) -> Value {
    let msg = tokio::time::timeout(timeout, rx.next())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {label}"))
        .unwrap_or_else(|| panic!("stream closed waiting for {label}"))
        .expect("ws error");
    match msg {
        Message::Text(t) => serde_json::from_str(t.as_str()).expect("frame is JSON"),
        other => panic!("expected text frame for {label}, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_relay_endpoint_serves_memoryless_nip01() -> anyhow::Result<()> {
    // Spawn the memoryless relay; keep the handle alive for the test.
    let relay = relay::spawn().await?;
    let relay_url = relay.url().to_owned();

    let cfg = ShimConfig {
        listen_addr: "127.0.0.1:0".into(),
        relay_urls: vec!["wss://unused.example".into()],
        private_key: None,
        encryption_mode: EncryptionMode::Disabled,
        connect_timeout: Duration::from_secs(5),
        gift_wrap_mode: GiftWrapMode::Ephemeral,
        max_cached_outlays: 4,
        max_ws_message_bytes: 1 << 20,
        enable_relay: true,
        public_urls: vec![],
        #[cfg(feature = "test-utils")]
        test_relay_pool: None,
    };
    let app = server::router(server::AppState::new(cfg, Some(relay_url)));

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let _serve = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let url = format!("ws://{addr}/");
    let (ws, _resp) = connect_async(url).await?;
    let (mut tx, mut rx) = ws.split();

    // 1) Subscribe before publishing → expect an immediate EOSE (nothing stored).
    tx.send(Message::Text(r#"["REQ","t",{"kinds":[1]}]"#.into()))
        .await?;
    let frame = recv_text(&mut rx, Duration::from_secs(5), "EOSE for REQ t").await;
    assert_eq!(frame[0], "EOSE", "empty memoryless store => EOSE at once");
    assert_eq!(frame[1], "t");

    // 2) Publish a signed kind:1 text note.
    let keys = Keys::generate();
    let event =
        EventBuilder::new(Kind::TextNote, "hello from the relay test").sign_with_keys(&keys)?;
    let id = event.id.to_hex();
    tx.send(Message::Text(
        serde_json::json!(["EVENT", event]).to_string().into(),
    ))
    .await?;

    // 3) Expect an OK ack (memoryless save_event reports Success) AND the live
    //    EVENT delivered to the open subscription "t". Order between them is not
    //    guaranteed; collect frames until we've seen both.
    let mut got_ok = false;
    let mut got_live_event = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while (!got_ok || !got_live_event) && tokio::time::Instant::now() < deadline {
        let v = tokio::time::timeout(Duration::from_millis(1500), rx.next())
            .await
            .ok()
            .and_then(|x| x)
            .and_then(|r| r.ok());
        let Some(Message::Text(t)) = v else { continue };
        let f: Value = serde_json::from_str(t.as_str())?;
        match f[0].as_str() {
            Some("OK") => {
                assert_eq!(f[1], id, "OK echoes the event id");
                assert_eq!(f[2], true, "memoryless relay accepts the event");
                got_ok = true;
            }
            Some("EVENT") => {
                assert_eq!(f[1], "t", "live EVENT tagged with our sub id");
                assert_eq!(f[2]["id"], id, "live EVENT carries our event");
                got_live_event = true;
            }
            other => panic!("unexpected frame: {other:?} {f}"),
        }
    }
    assert!(got_ok, "expected an OK ack for the published event");
    assert!(got_live_event, "expected the live EVENT on subscription t");

    // 4) Memoryless: a NEW REQ for the same kind, opened AFTER the publish, must
    //    NOT backfill the event — just EOSE.
    tx.send(Message::Text(r#"["REQ","t2",{"kinds":[1]}]"#.into()))
        .await?;
    let frame = recv_text(&mut rx, Duration::from_secs(5), "EOSE for REQ t2").await;
    assert_eq!(frame[0], "EOSE", "memoryless relay must not backfill");
    assert_eq!(frame[1], "t2");

    let _ = tx.close().await;
    relay.shutdown();
    Ok(())
}
