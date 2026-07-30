//! Smoke test: a vanilla NIP-01 WebSocket client → outlay-shim → outlay
//! (network-free CVM hop over a mock relay) → real upstream relay
//! (`wss://relay.primal.net`). Exercises the full translate/bridge path:
//! `subscribe` (streaming), `publish_event` (sync), and `relay_info` (HTTP).
//!
//! Gated behind the `test-utils` feature (injects `MockRelayPool` for the
//! shim↔outlay CVM hop) and `#[ignore]` on each test (they hit the network).
//!
//! ```sh
//! cargo test -p outlay-shim --features test-utils --test smoke -- --ignored --nocapture
//! ```
//!
//! Architecture:
//! ```text
//!   vanilla WS ──(loopback)── outlay-shim ──(mock relay)── outlay ──(real wss)── relay.primal.net
//!                raw NIP-01                 network-free CVM hop      real upstream hop
//! ```

#![cfg(feature = "test-utils")]

use std::sync::Arc;
use std::time::Duration;

use contextvm_sdk::relay::mock::MockRelayPool;
use contextvm_sdk::transport::open_stream::OpenStreamConfig;
use contextvm_sdk::transport::server::{NostrServerTransport, NostrServerTransportConfig};
use contextvm_sdk::{EncryptionMode, GiftWrapMode, RelayPoolTrait};
use futures::{SinkExt, StreamExt};
use outlay::handler::OutlayServer;
use outlay::proxy::Proxy;
use outlay_shim::config::ShimConfig;
use outlay_shim::server;
use rmcp::ServiceExt;
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message;

/// The real upstream relay outlay proxies in these smoke tests.
const UPSTREAM: &str = "wss://relay.primal.net";

struct Fixture {
    /// `http://127.0.0.1:<port>` — use `ws://…` / `http://…` as needed.
    base_url: String,
    server_pubkey: String,
    outlay: tokio::task::JoinHandle<()>,
    shim: tokio::task::JoinHandle<()>,
}

/// Stand up outlay (mock CVM transport + real upstream) and the shim (axum,
/// mock client pool injected), both in-process. The shim↔outlay CVM hop is
/// network-free; only outlay→primal touches the network.
async fn fixture() -> Fixture {
    let (shim_pool, server_pool) = MockRelayPool::create_pair();
    let server_pubkey = server_pool.mock_public_key().to_hex();

    // outlay server: real upstream Proxy + mock CVM transport.
    let proxy = Proxy::new(UPSTREAM.into())
        .await
        .expect("connect upstream proxy");
    let server_transport = NostrServerTransport::with_relay_pool(
        NostrServerTransportConfig::default()
            .with_encryption_mode(EncryptionMode::Disabled)
            .with_open_stream(OpenStreamConfig::enabled()),
        Arc::new(server_pool) as Arc<dyn RelayPoolTrait>,
    )
    .await
    .expect("server transport");
    let outlay_server = OutlayServer::new(Arc::new(proxy));
    let outlay = tokio::spawn(async move {
        outlay_server
            .serve(server_transport)
            .await
            .expect("outlay serve")
            .waiting()
            .await
            .expect("outlay wait");
    });
    // Let outlay's upstream Proxy finish its initial relay handshake.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Shim: axum on an ephemeral port, with the mock client pool injected so
    // build_client() opens a network-free CVM transport to outlay.
    let cfg = ShimConfig {
        listen_addr: "127.0.0.1:0".into(),
        relay_urls: vec!["wss://mock.relay".into()],
        private_key: None,
        encryption_mode: EncryptionMode::Disabled,
        connect_timeout: Duration::from_secs(15),
        gift_wrap_mode: GiftWrapMode::Ephemeral,
        max_cached_outlays: 64,
        max_ws_message_bytes: 1 << 20,
        enable_relay: false,
        test_relay_pool: Some(Arc::new(shim_pool) as Arc<dyn RelayPoolTrait>),
    };
    let app = server::router(server::AppState::new(cfg, None));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind shim");
    let addr = listener.local_addr().expect("local_addr");
    let base_url = format!("http://{addr}");
    let shim = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    Fixture {
        base_url,
        server_pubkey,
        outlay,
        shim,
    }
}

async fn cleanup(fx: Fixture) {
    fx.outlay.abort();
    fx.shim.abort();
}

/// `ws://` URL for the server pubkey path.
fn ws_url(fx: &Fixture) -> String {
    format!(
        "ws://{}/{}",
        fx.base_url.trim_start_matches("http://"),
        fx.server_pubkey
    )
}

/// `subscribe`: the shim translates `REQ` into outlay's streaming `subscribe`
/// and forwards `["EVENT",sub,e]` / `["EOSE",sub]` chunks back as raw NIP-01.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn subscribe_streams_events_and_eose() {
    let fx = fixture().await;
    let (mut ws, _resp) = tokio_tungstenite::connect_async(ws_url(&fx))
        .await
        .expect("ws connect");

    ws.send(Message::Text(
        r#"["REQ","sub1",{"kinds":[1],"limit":3}]"#.into(),
    ))
    .await
    .expect("send REQ");

    let mut events = 0usize;
    let mut got_eose = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => break,
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(t))) => {
                    let v: Value = serde_json::from_str(t.as_str()).expect("chunk is JSON");
                    println!("subscribe recv: {v}");
                    match v[0].as_str() {
                        Some("EVENT") => {
                            assert_eq!(v[1], "sub1", "client's bare sub_id echoed");
                            assert!(v[2].is_object(), "EVENT carries the event object");
                            events += 1;
                        }
                        Some("EOSE") => {
                            assert_eq!(v[1], "sub1");
                            got_eose = true;
                            break;
                        }
                        Some("CLOSED") => break, // upstream closed the sub
                        other => println!("  (ignoring {other:?})"),
                    }
                }
                Some(Ok(_)) => {} // Ping/Pong/Binary
                _ => break,
            },
        }
    }
    let _ = ws.close(None).await;
    cleanup(fx).await;
    assert!(events > 0, "expected at least one EVENT from upstream");
    assert!(got_eose, "expected EOSE for a bounded REQ");
}

/// `publish_event`: the shim translates a 2-element `["EVENT",e]` publish into
/// outlay's `publish_event` and synthesizes the upstream `["OK",id,bool,msg]`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn publish_event_returns_ok() {
    let fx = fixture().await;
    let (mut ws, _resp) = tokio_tungstenite::connect_async(ws_url(&fx))
        .await
        .expect("ws connect");

    // Ephemeral kind 22222 — relays accept and discard, so no litter.
    let keys = nostr_sdk::Keys::generate();
    let event = nostr_sdk::EventBuilder::new(nostr_sdk::Kind::from(22222_u16), "outlay-shim smoke")
        .sign_with_keys(&keys)
        .expect("sign event");
    ws.send(Message::Text(
        serde_json::json!(["EVENT", event]).to_string().into(),
    ))
    .await
    .expect("send EVENT");

    let mut got_ok = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < deadline {
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => break,
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(t))) => {
                    let v: Value = serde_json::from_str(t.as_str()).expect("OK is JSON");
                    println!("publish recv: {v}");
                    if v[0].as_str() == Some("OK") {
                        assert_eq!(
                            v[2].as_bool(),
                            Some(true),
                            "upstream should accept the ephemeral event (got: {v})"
                        );
                        got_ok = true;
                        break;
                    }
                }
                Some(Ok(_)) => {}
                _ => break,
            },
        }
    }
    let _ = ws.close(None).await;
    cleanup(fx).await;
    assert!(got_ok, "expected an OK frame for the published event");
}

/// `relay_info` over HTTP: `GET /<pubkey>` with `Accept: application/nostr+json`
/// proxies outlay's overlaid NIP-11 doc (software == "outlay").
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn relay_info_http_serves_outlay_doc() {
    let fx = fixture().await;
    let url = format!("{}/{}", fx.base_url, fx.server_pubkey);

    let doc: Value = tokio::time::timeout(
        Duration::from_secs(30),
        reqwest::Client::new()
            .get(&url)
            .header("Accept", "application/nostr+json")
            .send(),
    )
    .await
    .expect("http send timed out")
    .expect("http send")
    .json()
    .await
    .expect("nip11 json");

    println!("relay_info http: {doc}");
    assert_eq!(
        doc["software"], "outlay",
        "outlay stamps its software on top"
    );
    assert_eq!(doc["proxy"], true, "proxy marker set");
    cleanup(fx).await;
}

/// `CLOSE`: the subscribe smoke test let EOSE finish the stream; this one drives
/// the `JoinHandle::abort` cancellation path directly. Open a kind:1 firehose,
/// confirm it's streaming, send `CLOSE`, then assert no more events for that
/// sub_id arrive and the connection stays usable for a fresh subscription.
///
/// Robustness: primal's kind:1 volume is high (many/sec), so "0 events in 2s
/// after CLOSE" is a strong signal, not a flaky one. The shim-side stop is
/// immediate (aborting the task drops the CEP-41 consumer); the upstream
/// `CLOSE` rides outlay's keepalive later, but the client never sees those.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn close_cancels_open_subscription() -> anyhow::Result<()> {
    let fx = fixture().await;
    let (mut ws, _resp) = tokio_tungstenite::connect_async(ws_url(&fx))
        .await
        .expect("ws connect");

    // Open firehose: kind:1, no limit → primal streams stored + live events.
    ws.send(Message::Text(r#"["REQ","sub1",{"kinds":[1]}]"#.into()))
        .await
        .expect("send REQ");

    // 1) Confirm it's actually streaming before we CLOSE.
    let mut pre = 0usize;
    let pre_dl = tokio::time::Instant::now() + Duration::from_millis(1500);
    loop {
        if tokio::time::Instant::now() >= pre_dl {
            break;
        }
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(pre_dl) => break,
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(t))) => {
                    let v: Value = serde_json::from_str(t.as_str())?;
                    if v[0].as_str() == Some("EVENT") {
                        pre += 1;
                    }
                }
                None => break,
                _ => {}
            },
        }
    }
    assert!(pre >= 1, "expected live events before CLOSE (got {pre})");

    // 2) CLOSE it.
    ws.send(Message::Text(r#"["CLOSE","sub1"]"#.into()))
        .await
        .expect("send CLOSE");

    // 3) Grace: drain anything already in flight when CLOSE was processed.
    let grace_dl = tokio::time::Instant::now() + Duration::from_millis(800);
    loop {
        if tokio::time::Instant::now() >= grace_dl {
            break;
        }
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(grace_dl) => break,
            msg = ws.next() => { if msg.is_none() { break; } }
        }
    }

    // 4) Post-CLOSE window: no sub1 events should arrive.
    let mut post = 0usize;
    let post_dl = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if tokio::time::Instant::now() >= post_dl {
            break;
        }
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(post_dl) => break,
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(t))) => {
                    let v: Value = serde_json::from_str(t.as_str())?;
                    if v[0].as_str() == Some("EVENT") && v[1].as_str() == Some("sub1") {
                        post += 1;
                    }
                }
                None => break,
                _ => {}
            },
        }
    }
    assert_eq!(post, 0, "CLOSE should stop sub1 events (got {post} in 2s)");

    // 5) The connection survived and is still usable: a fresh bounded REQ EOSEs.
    ws.send(Message::Text(
        r#"["REQ","sub2",{"kinds":[1],"limit":1}]"#.into(),
    ))
    .await
    .expect("send REQ sub2");
    let mut got_eose_sub2 = false;
    let dl = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if tokio::time::Instant::now() >= dl {
            break;
        }
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(dl) => break,
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(t))) => {
                    let v: Value = serde_json::from_str(t.as_str())?;
                    if v[0].as_str() == Some("EOSE") && v[1].as_str() == Some("sub2") {
                        got_eose_sub2 = true;
                        break;
                    }
                }
                None => break,
                _ => {}
            },
        }
    }
    let _ = ws.close(None).await;
    cleanup(fx).await;
    assert!(
        got_eose_sub2,
        "connection should survive CLOSE and serve a new sub"
    );
    Ok(())
}

/// Two bounded REQs issued back-to-back → two concurrent CEP-41 streams feeding
/// the single per-connection writer. Asserts both EOSE under their own sub_id
/// with no cross-talk (no chunk carries an unexpected sub_id).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn concurrent_subscriptions_isolated() -> anyhow::Result<()> {
    let fx = fixture().await;
    let (mut ws, _resp) = tokio_tungstenite::connect_async(ws_url(&fx))
        .await
        .expect("ws connect");

    ws.send(Message::Text(
        r#"["REQ","sub1",{"kinds":[1],"limit":2}]"#.into(),
    ))
    .await
    .expect("REQ sub1");
    ws.send(Message::Text(
        r#"["REQ","sub2",{"kinds":[1],"limit":2}]"#.into(),
    ))
    .await
    .expect("REQ sub2");

    let mut got_sub1 = false;
    let mut got_sub2 = false;
    let mut leak = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if got_sub1 && got_sub2 || tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => break,
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(t))) => {
                    let v: Value = serde_json::from_str(t.as_str())?;
                    let sid = v[1].as_str().unwrap_or("");
                    match v[0].as_str() {
                        Some("EOSE") => {
                            if sid == "sub1" { got_sub1 = true; }
                            else if sid == "sub2" { got_sub2 = true; }
                            else { leak += 1; }
                        }
                        Some("EVENT") if sid != "sub1" && sid != "sub2" => leak += 1,
                        _ => {}
                    }
                }
                None => break,
                _ => {}
            },
        }
    }
    let _ = ws.close(None).await;
    cleanup(fx).await;
    assert!(got_sub1, "sub1 should EOSE");
    assert!(got_sub2, "sub2 should EOSE");
    assert_eq!(leak, 0, "a chunk carried an unexpected sub_id (cross-talk)");
    Ok(())
}
