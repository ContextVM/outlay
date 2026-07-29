//! Network-free E2E smoke test for the `bundled-relay` feature.
//!
//! outlay's upstream is the in-process bundled relay (loopback `LocalRelay`),
//! and the CVM client↔server hop runs over a mock relay pool — so the whole test
//! is **fully network-free** (no public relay, no internet). Proves Shape A:
//! outlay self-contained, proxy + tool handlers unchanged, the bundled relay
//! serving real NIP-01 as the upstream.
//!
//! ```text
//!   rmcp client ──(mock relay)── outlay server ──(loopback ws)── bundled LocalRelay
//!                network-free CVM hop             network-free upstream NIP-01 hop
//! ```
//!
//! Not `#[ignore]` (no network) — but gated on both `test-utils` (mock pool) and
//! `bundled-relay`. Run with:
//!
//! ```sh
//! cargo test --features "test-utils bundled-relay" --test smoke_bundled -- --nocapture
//! ```

use std::sync::Arc;
use std::time::Duration;

use contextvm_sdk::relay::mock::MockRelayPool;
use contextvm_sdk::transport::client::{NostrClientTransport, NostrClientTransportConfig};
use contextvm_sdk::transport::open_stream::OpenStreamConfig;
use contextvm_sdk::transport::server::{NostrServerTransport, NostrServerTransportConfig};
use contextvm_sdk::{call_tool_stream, ClientOpenStreamHandle, EncryptionMode, RelayPoolTrait};
use futures::StreamExt;
use nostr_sdk::prelude::*;
use rmcp::{model::CallToolRequestParams, ClientHandler, ServiceExt};

use outlay::handler::OutlayServer;
use outlay::proxy::Proxy;

#[derive(Clone, Default)]
struct DemoClient;
impl ClientHandler for DemoClient {}

fn args(v: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
    match v {
        serde_json::Value::Object(m) => m,
        _ => panic!("tool arguments must be a JSON object"),
    }
}

struct Fixture {
    client: rmcp::service::RunningService<rmcp::RoleClient, DemoClient>,
    handle: ClientOpenStreamHandle,
    server: tokio::task::JoinHandle<()>,
    relay: outlay_relay::BundledRelay,
}

/// outlay server pointing at a freshly-spawned bundled relay, paired with an
/// rmcp client over a linked mock pool (network-free CVM hop).
async fn fixture() -> Fixture {
    let relay = outlay_relay::BundledRelay::spawn(outlay_relay::Backend::Memory, None, 0)
        .await
        .expect("spawn bundled relay");

    let proxy = Proxy::new(relay.url().to_string())
        .await
        .expect("connect proxy to bundled relay");

    let (client_pool, server_pool) = MockRelayPool::create_pair();
    let server_pubkey = server_pool.mock_public_key().to_hex();

    let server_transport = NostrServerTransport::with_relay_pool(
        NostrServerTransportConfig::default()
            .with_encryption_mode(EncryptionMode::Disabled)
            .with_open_stream(OpenStreamConfig::enabled()),
        Arc::new(server_pool) as Arc<dyn RelayPoolTrait>,
    )
    .await
    .expect("server transport");

    let server = OutlayServer::new(Arc::new(proxy));
    let server = tokio::spawn(async move {
        server
            .serve(server_transport)
            .await
            .expect("server serve")
            .waiting()
            .await
            .expect("server wait");
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client_transport = NostrClientTransport::with_relay_pool(
        NostrClientTransportConfig::default()
            .with_server_pubkey(server_pubkey)
            .with_encryption_mode(EncryptionMode::Disabled)
            .with_relay_urls(vec!["wss://mock.relay".to_string()])
            .with_open_stream(OpenStreamConfig::enabled()),
        Arc::new(client_pool) as Arc<dyn RelayPoolTrait>,
    )
    .await
    .expect("client transport");
    let handle = client_transport.open_stream_handle();
    let client = tokio::time::timeout(Duration::from_secs(15), DemoClient.serve(client_transport))
        .await
        .expect("client startup timed out")
        .expect("client init failed");

    Fixture {
        client,
        handle,
        server,
        relay,
    }
}

async fn cleanup(fx: Fixture) {
    let _ = fx.client.cancel().await;
    fx.server.abort();
    fx.relay.shutdown();
}

/// Open a live subscription, then publish a matching event through outlay and
/// confirm the bundled relay broadcasts it back. The canonical relay round-trip
/// (avoids any store-vs-query race: the event arrives live on the active sub).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bundled_relay_live_publish_round_trip() {
    let fx = fixture().await;

    let keys = Keys::generate();
    let pubkey = keys.public_key().to_hex();

    // 1. Open a live subscription for this (fresh) key's future events.
    let params = CallToolRequestParams::new("subscribe").with_arguments(args(serde_json::json!({
        "subscription_id": "rt",
        "filters": [{ "authors": [pubkey], "kinds": [1] }]
    })));
    let mut call = call_tool_stream(fx.client.peer(), &fx.handle, params)
        .await
        .expect("call_tool_stream");

    // 2. The first message must be EOSE — a fresh key has nothing stored.
    match tokio::time::timeout(Duration::from_secs(5), call.stream.next()).await {
        Ok(Some(Ok(chunk))) => {
            let v: serde_json::Value = serde_json::from_str(&chunk).expect("json chunk");
            match v[0].as_str() {
                Some("EOSE") => assert_eq!(v[1], "rt"),
                Some("EVENT") => panic!("unexpected stored event for a fresh key: {v}"),
                other => panic!("expected initial EOSE, got {other:?}: {v}"),
            }
        }
        _ => panic!("no initial EOSE on the live subscription"),
    }

    // 3. publish_event → the relay broadcasts the new event to the active sub.
    //    Kind 1 (text note) is storable; live broadcast works for any kind.
    let event = EventBuilder::text_note("bundled-relay round trip")
        .sign_with_keys(&keys)
        .expect("sign event");
    let event_id = event.id.to_hex();
    let pub_result = fx
        .client
        .peer()
        .call_tool(
            CallToolRequestParams::new("publish_event")
                .with_arguments(args(serde_json::json!({ "event": event }))),
        )
        .await
        .expect("publish_event call");
    let sc = pub_result
        .structured_content
        .expect("publish_event structured content");
    assert_eq!(
        sc["ok"], true,
        "bundled relay should accept the event: {sc}"
    );

    // 4. The live EVENT arrives on the open subscription.
    let mut got_event = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !got_event && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(2), call.stream.next()).await {
            Ok(Some(Ok(chunk))) => {
                let v: serde_json::Value = serde_json::from_str(&chunk).expect("json chunk");
                if v[0].as_str() == Some("EVENT") {
                    assert_eq!(v[1], "rt", "bare sub_id echoed");
                    assert_eq!(v[2]["id"], event_id, "our published event broadcast back");
                    got_event = true;
                }
            }
            _ => break,
        }
    }
    assert!(
        got_event,
        "published event was not broadcast to the live subscription"
    );

    let _ = call.abort(Some("done".to_string())).await;
    cleanup(fx).await;
}

/// Historical query: publish an event, then open a *new* subscription with a
/// broad filter matching the stored event. The relay must serve it back from the
/// database (not just live-broadcast). (A prior id-only variant returned EOSE
/// with no event — broad filter here isolates whether stored queries work.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bundled_relay_serves_stored_event_on_query() {
    let fx = fixture().await;

    let keys = Keys::generate();
    let pubkey = keys.public_key().to_hex();
    let event = EventBuilder::text_note("stored-query probe")
        .sign_with_keys(&keys)
        .expect("sign event");
    let event_id = event.id.to_hex();

    // Publish + confirm stored.
    let pub_result = fx
        .client
        .peer()
        .call_tool(
            CallToolRequestParams::new("publish_event")
                .with_arguments(args(serde_json::json!({ "event": event }))),
        )
        .await
        .expect("publish_event call");
    assert_eq!(
        pub_result.structured_content.expect("sc")["ok"],
        true,
        "bundled relay should accept the event"
    );

    // Fresh subscription with a broad filter (author + kind) covering the stored
    // event. Kind 1 is storable (unlike ephemeral 20000-29999), so the relay
    // must serve it from the database.
    let params = CallToolRequestParams::new("subscribe").with_arguments(args(serde_json::json!({
        "subscription_id": "hist",
        "filters": [{ "authors": [pubkey], "kinds": [1] }]
    })));
    let mut call = call_tool_stream(fx.client.peer(), &fx.handle, params)
        .await
        .expect("call_tool_stream");

    let mut got_event = false;
    let mut got_eose = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !(got_event && got_eose) && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(2), call.stream.next()).await {
            Ok(Some(Ok(chunk))) => {
                let v: serde_json::Value = serde_json::from_str(&chunk).expect("json chunk");
                match v[0].as_str() {
                    Some("EVENT") => {
                        assert_eq!(v[1], "hist");
                        assert_eq!(v[2]["id"], event_id, "stored event served from db");
                        got_event = true;
                    }
                    Some("EOSE") => {
                        assert_eq!(v[1], "hist");
                        got_eose = true;
                    }
                    _ => {}
                }
            }
            _ => break,
        }
    }
    assert!(
        got_event,
        "stored event not served on historical query (broad filter)"
    );
    assert!(got_eose, "no EOSE on historical query");

    let _ = call.abort(Some("done".to_string())).await;
    cleanup(fx).await;
}

/// Issue 2 regression: `relay_info` against the bundled relay. The bundled relay
/// speaks WS only — nothing answers a plain HTTP GET on its loopback origin, so
/// the upstream NIP-11 fetch must soft-fail to the synthesized doc rather than
/// surface a transport error. Before the fix this call returned an error result
/// (`is_error`, no structured content); after, it returns outlay's synthesized
/// identity (software=outlay, proxy=true, no upstream fields).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bundled_relay_relay_info_falls_back_when_no_nip11() {
    let fx = fixture().await;

    let result = fx
        .client
        .peer()
        .call_tool(CallToolRequestParams::new("relay_info"))
        .await
        .expect("relay_info call");
    assert_ne!(
        result.is_error,
        Some(true),
        "relay_info must not error when the upstream serves no NIP-11: {result:?}"
    );
    let doc = result
        .structured_content
        .as_ref()
        .expect("relay_info structured content");
    assert_eq!(doc["software"], "outlay", "synthesized identity stamped");
    assert_eq!(doc["proxy"], true, "proxy flag set");
    assert!(
        doc.get("supported_nips")
            .and_then(|v| v.as_array())
            .is_some(),
        "synthesized doc carries supported_nips"
    );
    assert!(
        doc.get("upstream").is_none(),
        "no upstream fields when the doc is synthesized"
    );

    cleanup(fx).await;
}
