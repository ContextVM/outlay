//! Integration smoke tests: a real CVM client (over a mock relay, network-free
//! CVM hop) driving the outlay server, whose upstream `Proxy` connects to a
//! REAL public relay (`wss://relay.primal.net`). Proves the full subscribe /
//! publish_event translation end-to-end.
//!
//! Each test is `#[ignore]` (hits the network) and the binary is gated on the
//! `test-utils` feature (for `MockRelayPool`). Run with:
//!
//! ```sh
//! cargo test --features test-utils --test smoke -- --ignored --nocapture
//! ```
//!
//! Architecture:
//! ```text
//!   rmcp client ──(mock relay)── outlay server ──(real wss)── relay.primal.net
//!                network-free CVM hop            real upstream NIP-01 hop
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

/// The real upstream relay outlay proxies in these smoke tests.
const UPSTREAM: &str = "wss://relay.primal.net";

#[derive(Clone, Default)]
struct DemoClient;
impl ClientHandler for DemoClient {}

struct Fixture {
    client: rmcp::service::RunningService<rmcp::RoleClient, DemoClient>,
    handle: ClientOpenStreamHandle,
    server: tokio::task::JoinHandle<()>,
}

/// `CallToolRequestParams::with_arguments` takes a JSON object map, not a Value.
fn args(v: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
    match v {
        serde_json::Value::Object(m) => m,
        _ => panic!("tool arguments must be a JSON object"),
    }
}

/// Build a running outlay server (real upstream Proxy + mock CVM transport)
/// paired with a running rmcp client over the linked mock pool.
async fn fixture() -> Fixture {
    let (client_pool, server_pool) = MockRelayPool::create_pair();
    let server_pubkey = server_pool.mock_public_key().to_hex();

    // Real upstream: outlay's Proxy connects to primal.
    let proxy = Proxy::new(UPSTREAM.into())
        .await
        .expect("connect upstream proxy");

    // CVM server transport over the mock (network-free client<->server hop).
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
    }
}

async fn cleanup(fx: Fixture) {
    let _ = fx.client.cancel().await;
    fx.server.abort();
}

/// `subscribe` streams real `["EVENT",sub,e]` chunks and an `["EOSE",sub]`
/// from primal, tagged with the client's (bare) subscription id.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn subscribe_streams_events_and_eose_from_upstream() {
    let fx = fixture().await;

    let params = CallToolRequestParams::new("subscribe").with_arguments(args(serde_json::json!({
        "subscription_id": "smoke",
        "filters": [{ "kinds": [1], "limit": 3 }]
    })));

    let mut call = call_tool_stream(fx.client.peer(), &fx.handle, params)
        .await
        .expect("call_tool_stream");
    println!(
        "subscribe: stream open (progress_token={})",
        call.progress_token
    );

    let mut got_eose = false;
    let mut events = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while tokio::time::Instant::now() < deadline {
        tokio::select! {
            biased;
            item = call.stream.next() => match item {
                Some(Ok(chunk)) => {
                    let v: serde_json::Value =
                        serde_json::from_str(&chunk).expect("chunk is a JSON array");
                    match v[0].as_str() {
                        Some("EVENT") => {
                            assert_eq!(v[1], "smoke", "client's bare sub_id echoed");
                            assert!(v[2].is_object(), "EVENT carries the event object");
                            events += 1;
                            let ev = &v[2];
                            let id = ev["id"].as_str().unwrap_or("?");
                            let kind = ev["kind"].as_u64().unwrap_or(0);
                            let pubkey = ev["pubkey"].as_str().unwrap_or("?");
                            let content: String =
                                ev["content"].as_str().unwrap_or("").chars().take(70).collect();
                            println!(
                                "  EVENT #{events} kind={kind} id={}.. pubkey={}.. content={content:?}",
                                &id[..8.min(id.len())],
                                &pubkey[..8.min(pubkey.len())],
                            );
                        }
                        Some("EOSE") => {
                            assert_eq!(v[1], "smoke");
                            println!("  EOSE ({events} events delivered)");
                            got_eose = true;
                            break;
                        }
                        other => println!("  {other:?} {v}"),
                    }
                }
                _ => break,
            },
            _ = tokio::time::sleep_until(deadline) => break,
        }
    }
    println!("subscribe: done (events={events}, eose={got_eose})");
    assert!(events > 0, "expected at least one EVENT from upstream");
    assert!(
        got_eose,
        "expected EOSE from upstream within the timeout (check the relay is reachable)"
    );

    // CLOSE == abort the call; the proxy unwinds the upstream subscription.
    let _ = call.abort(Some("done".to_string())).await;
    cleanup(fx).await;
}

/// `publish_event` forwards a client-signed event verbatim and the upstream
/// returns OK. Uses an ephemeral kind (22222) — relays accept and discard it,
/// so no litter is left on the public relay.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn publish_event_accepted_by_upstream() {
    let fx = fixture().await;

    let keys = Keys::generate();
    let event = EventBuilder::new(Kind::from(22222_u16), "outlay smoke test")
        .sign_with_keys(&keys)
        .expect("sign event");

    let result = fx
        .client
        .peer()
        .call_tool(
            CallToolRequestParams::new("publish_event")
                .with_arguments(args(serde_json::json!({ "event": event }))),
        )
        .await
        .expect("publish_event call");

    let sc = result
        .structured_content
        .expect("publish_event returns structured content");
    println!("publish_event: outcome {sc}");
    assert_eq!(
        sc["ok"], true,
        "upstream should accept the ephemeral event (got: {sc})"
    );
    assert!(sc["event_id"].is_string(), "event_id echoed back");

    cleanup(fx).await;
}

/// `relay_info` fetches the upstream's NIP-11 doc over HTTP and overlays outlay's
/// identity: top-level `software`/`version` become outlay's, the upstream's land
/// under `upstream`, and a `proxy: true` marker is set. Unknown upstream fields
/// are preserved (verbatim).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn relay_info_overlays_outlay_identity_on_upstream_nip11() {
    let fx = fixture().await;

    let result = fx
        .client
        .peer()
        .call_tool(CallToolRequestParams::new("relay_info"))
        .await
        .expect("relay_info call");

    let sc = result
        .structured_content
        .expect("relay_info returns structured content");
    println!("relay_info: {sc}");
    assert_eq!(
        sc["software"], "outlay",
        "outlay stamps its software on top"
    );
    assert_eq!(sc["proxy"], true, "proxy marker is set");
    assert_eq!(sc["version"], env!("CARGO_PKG_VERSION"));
    // primal serves a real NIP-11 doc with a `software` field → preserved.
    assert!(
        sc["upstream"]["software"].is_string(),
        "upstream software preserved under `upstream` (got: {sc})"
    );

    cleanup(fx).await;
}
