//! Upstream relay proxy. Holds the SDK [`RelayPool`] connected to the single
//! configured upstream relay and forwards NIP-01 traffic both ways:
//!
//! - `subscribe` — opens a NIP-01 subscription upstream and streams
//!   `["EVENT",sub,e]` / `["EOSE",sub]` / `["CLOSED",sub,msg]` chunks to the
//!   client. One tool call == one subscription; CLOSE == aborting the call.
//! - `publish_event` — forwards a client-signed event verbatim and returns the
//!   upstream `OK` status. Never re-signs (design §8.4).
//!
//! Testability seam: the notification→chunk mapping is a pure function
//! ([`map_notification`]) unit-tested with no relay; the async plumbing is
//! covered separately by an integration test against a real local relay.

use std::time::Duration;

use contextvm_sdk::relay::RelayPool;
use contextvm_sdk::transport::open_stream::OpenStreamWriter;
use nostr_sdk::prelude::*;
use serde_json::json;
use tokio::sync::broadcast;

/// How often the subscribe loop polls the sink's liveness flag, so a silently
/// dropped client (CEP-41 stream aborted without a clean signal here) does not
/// leave an upstream subscription open forever. Ported from cordn's pattern.
const SINK_ACTIVE_POLL: Duration = Duration::from_secs(1);

/// How long `publish_event` waits for the upstream `OK` before giving up
/// (design §8.6).
const PUBLISH_TIMEOUT: Duration = Duration::from_secs(15);

/// How long `relay_info` waits for the upstream NIP-11 document.
const RELAY_INFO_TIMEOUT: Duration = Duration::from_secs(10);

/// outlay's own version, overlaid onto the upstream's NIP-11 document.
const OUTLAY_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, thiserror::Error)]
pub enum ProxyError {
    #[error("upstream relay pool init failed: {0}")]
    Init(String),
    #[error("upstream relay connect failed: {0}")]
    Connect(String),
    #[error("subscribe filters must be non-empty")]
    EmptyFilters,
    #[error("upstream subscribe failed: {0}")]
    Subscribe(String),
    #[error("upstream publish failed: {0}")]
    Publish(String),
    #[error("upstream did not acknowledge the event within timeout")]
    PublishTimeout,
    #[error("relay_info: {0}")]
    RelayInfo(String),
}

/// The CEP-41 sink the proxy writes NIP-01 relay→client chunks to, with the
/// bool-returning surface the subscribe loop wants. The only sink type — the
/// trait it replaced had one impl and no mock-sink test was ever written.
pub struct StreamWriter(pub OpenStreamWriter);

impl StreamWriter {
    pub async fn start(&self) -> bool {
        self.0.start().await.is_ok()
    }
    /// Write one chunk. Returns `false` if the sink is dead and the loop should stop.
    pub async fn write(&self, msg: String) -> bool {
        self.0.write(msg).await.is_ok() && self.0.is_active()
    }
    pub fn is_active(&self) -> bool {
        self.0.is_active()
    }
    pub async fn close(&self) {
        let _ = self.0.close().await;
    }
}

/// Outcome of mapping one upstream notification for a subscription stream.
#[derive(Debug)]
enum Forward {
    /// Forward this chunk and keep streaming.
    Continue(String),
    /// Forward this chunk and then end the stream (upstream closed the sub).
    Stop(String),
}

/// Pure mapping from an upstream [`RelayPoolNotification`] to a NIP-01
/// relay→client chunk, for the given upstream subscription id and the client's
/// bare subscription id. Returns `None` for notifications that do not belong to
/// this subscription (other subs, `OK`, `NOTICE`, the deduped `Event`
/// variant, …).
///
/// Uses the `RelayPoolNotification::Message` variant (not `Event`): `Event`
/// dedupes at the pool level and excludes events sent by this client, both of
/// which break transparent proxying. `Message` fires per-subscription, carries
/// the `subscription_id`, and includes every event — exactly NIP-01 semantics.
fn map_notification(
    n: &RelayPoolNotification,
    upstream_id: &SubscriptionId,
    client_sub: &str,
) -> Option<Forward> {
    let msg = match n {
        RelayPoolNotification::Message { message, .. } => message,
        RelayPoolNotification::Shutdown => {
            return Some(Forward::Stop(chunk(json!([
                "CLOSED",
                client_sub,
                "error: upstream relay pool shut down"
            ]))));
        }
        _ => return None,
    };
    match msg {
        RelayMessage::Event {
            subscription_id,
            event,
        } if subscription_id.as_str() == upstream_id.as_str() => {
            Some(Forward::Continue(chunk(json!([
                "EVENT", client_sub, event
            ]))))
        }
        RelayMessage::EndOfStoredEvents(sid) if sid.as_str() == upstream_id.as_str() => {
            Some(Forward::Continue(chunk(json!(["EOSE", client_sub]))))
        }
        RelayMessage::Closed {
            subscription_id,
            message,
        } if subscription_id.as_str() == upstream_id.as_str() => {
            Some(Forward::Stop(chunk(json!(["CLOSED", client_sub, message]))))
        }
        _ => None,
    }
}

fn chunk(parts: serde_json::Value) -> String {
    serde_json::to_string(&parts).unwrap_or_else(|_| "[]".into())
}

/// HTTP origin (scheme + authority, no path) for the upstream's NIP-11 doc:
/// `wss://host[:port][/path]` → `https://host[:port]`, `ws://…` → `http://…`.
/// NIP-11 is served at the HTTP root of the relay's websocket endpoint.
fn nip11_origin(upstream: &str) -> Option<String> {
    let (scheme, rest) = if let Some(h) = upstream.strip_prefix("wss://") {
        ("https://", h)
    } else {
        let h = upstream.strip_prefix("ws://")?;
        ("http://", h)
    };
    let authority = rest.split('/').next()?;
    Some(format!("{scheme}{authority}"))
}

/// Overlay outlay's identity onto a verbatim upstream NIP-11 document. Operates
/// on the raw JSON value so unknown upstream fields are preserved (the typed
/// `nostr::RelayInformationDocument` drops them). The upstream's
/// `software`/`version` are moved under an `upstream` key; outlay's identity
/// is stamped on top. Design §5.
// ponytail: v1 forwards upstream `supported_nips`/`limitation` verbatim — but
// through outlay a client effectively has only NIP-01. Revisit the honest
// values when a real client is misled (design §5 known mismatch).
fn overlay_proxy_info(mut doc: serde_json::Value) -> serde_json::Value {
    let obj = doc.as_object_mut().expect("nip11 doc is an object");
    let mut upstream = serde_json::Map::new();
    if let Some(s) = obj.remove("software") {
        upstream.insert("software".into(), s);
    }
    if let Some(v) = obj.remove("version") {
        upstream.insert("version".into(), v);
    }
    if !upstream.is_empty() {
        obj.insert("upstream".into(), serde_json::Value::Object(upstream));
    }
    obj.insert("software".into(), json!("outlay"));
    obj.insert("version".into(), json!(OUTLAY_VERSION));
    obj.insert("proxy".into(), json!(true));
    doc
}

/// Minimum NIP-11 document returned when the upstream serves none (or returns
/// non-JSON) — notably the future bundled in-process relay on `ws://127.0.0.1`.
fn synthesized_proxy_info() -> serde_json::Value {
    json!({
        "software": "outlay",
        "version": OUTLAY_VERSION,
        "proxy": true,
        "supported_nips": [1],
    })
}

/// The upstream `OK` outcome mirrored back by `publish_event`.
#[derive(Debug, serde::Serialize)]
pub struct PublishOutcome {
    pub ok: bool,
    pub event_id: EventId,
    pub message: String,
}

pub struct Proxy {
    pool: RelayPool,
    upstream_url: String,
    /// Reused across `relay_info` calls (connection pool, TLS session).
    http: reqwest::Client,
}

impl Proxy {
    /// Connect a fresh pool to the single configured upstream relay. The pool
    /// uses an ephemeral throwaway key: published events are client-signed and
    /// forwarded verbatim via `send_event`, so the pool's own key never signs.
    pub async fn new(upstream_url: String) -> Result<Self, ProxyError> {
        let pool = RelayPool::new(Keys::generate())
            .await
            .map_err(|e| ProxyError::Init(e.to_string()))?;
        pool.connect(std::slice::from_ref(&upstream_url))
            .await
            .map_err(|e| ProxyError::Connect(e.to_string()))?;
        let http = reqwest::Client::builder()
            .timeout(RELAY_INFO_TIMEOUT)
            .build()
            .map_err(|e| ProxyError::Init(format!("http client: {e}")))?;
        Ok(Self {
            pool,
            upstream_url,
            http,
        })
    }

    fn client(&self) -> &std::sync::Arc<Client> {
        self.pool.client()
    }

    /// Open a NIP-01 subscription upstream and forward matching messages to
    /// `sink` as chunks until the client closes the stream (abort) or the
    /// upstream closes the subscription (CLOSED).
    pub async fn subscribe(
        &self,
        client_sub: String,
        filters: Vec<Filter>,
        sink: &StreamWriter,
    ) -> Result<(), ProxyError> {
        if filters.is_empty() {
            return Err(ProxyError::EmptyFilters);
        }

        let client = self.client();

        // Fresh random upstream id — the pool multiplexes one upstream socket
        // and tags subs by id, so two CVM clients that both picked "sub1" would
        // cross-receive events. A per-call random id (within NIP-01's 64-char
        // limit) sidesteps that cleanly; the client sees only its bare sub_id
        // in chunks. (Design §8.3 originally proposed "<uuid>::<sub>"; a random
        // id is strictly better — shorter and length-safe.)
        let upstream_id = SubscriptionId::generate();

        // Register the notifications receiver BEFORE subscribing so we cannot
        // miss the first EVENT/EOSE arriving between subscribe and recv().
        let mut rx = client.notifications();

        // Bypass the Client wrapper (single-filter) and hit the pool directly:
        // pool.subscribe_with_id takes Into<Vec<Filter>>, preserving NIP-01's
        // multiple-filters-under-one-sub semantics.
        client
            .pool()
            .subscribe_with_id(upstream_id.clone(), filters, SubscribeOptions::default())
            .await
            .map_err(|e| ProxyError::Subscribe(e.to_string()))?;

        sink.start().await;
        loop {
            tokio::select! {
                recv = rx.recv() => match recv {
                    Ok(notif) => match map_notification(&notif, &upstream_id, &client_sub) {
                        Some(Forward::Continue(c)) => {
                            if !sink.write(c).await {
                                break;
                            }
                        }
                        Some(Forward::Stop(c)) => {
                            let _ = sink.write(c).await;
                            break;
                        }
                        None => {}
                    },
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                },
                _ = tokio::time::sleep(SINK_ACTIVE_POLL) => {
                    if !sink.is_active() {
                        break;
                    }
                }
            }
        }

        // Always unwind the upstream subscription, whether the client aborted,
        // the upstream closed, or the pool died.
        client.unsubscribe(&upstream_id).await;
        if sink.is_active() {
            sink.close().await;
        }
        Ok(())
    }

    /// Forward a client-signed event to the upstream relay verbatim and report
    /// the upstream `OK` status. `send_event` on a pre-built `Event` does not
    /// re-sign.
    pub async fn publish_event(&self, event: Event) -> Result<PublishOutcome, ProxyError> {
        let id = event.id;
        let client = self.client();
        // ponytail: nostr-sdk's `send_event` aggregates per-relay OKs into
        // success/failed sets and surfaces the relay's error text (often the
        // NIP-01 OK message) in `failed`. It does not expose the raw OK frame,
        // but for a single-upstream proxy this is faithful enough. If a client
        // ever needs the exact machine-readable OK prefix, upgrade to
        // `send_msg` + await `Ok` by event_id.
        let output = tokio::time::timeout(PUBLISH_TIMEOUT, client.send_event(&event))
            .await
            .map_err(|_| ProxyError::PublishTimeout)?
            .map_err(|e| ProxyError::Publish(e.to_string()))?;

        let ok = !output.success.is_empty();
        let message = if ok {
            String::new()
        } else {
            output
                .failed
                .values()
                .next()
                .map(ToString::to_string)
                .unwrap_or_else(|| "error: upstream did not accept the event".into())
        };
        Ok(PublishOutcome {
            ok,
            event_id: id,
            message,
        })
    }

    /// Fetch the upstream's NIP-11 information document and overlay outlay's
    /// identity (design §5). Falls back to a synthesized minimum when the
    /// upstream serves no usable NIP-11 doc.
    // ponytail: no cache — NIP-11 is mostly static and clients rarely hammer
    // it; add a TTL when profiling shows otherwise.
    pub async fn relay_info(&self) -> Result<serde_json::Value, ProxyError> {
        let origin = nip11_origin(&self.upstream_url).ok_or_else(|| {
            ProxyError::RelayInfo(format!(
                "upstream URL must be ws:// or wss://, got: {}",
                self.upstream_url
            ))
        })?;
        // Soft-fail transport errors (notably the bundled in-process relay,
        // which speaks WS only — nothing answers HTTP on its loopback origin)
        // and fall back to the synthesized doc rather than surfacing a hard
        // error. Design §5: relay_info is best-effort; the overlay/synthesized
        // path below already tolerates non-2xx / non-JSON bodies the same way.
        let resp = match self
            .http
            .get(&origin)
            .header("Accept", "application/nostr+json")
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(_) => return Ok(synthesized_proxy_info()),
        };
        // Robust: any 2xx with a JSON object body is the doc. Some relays
        // serve NIP-11 regardless of the Accept header.
        if resp.status().is_success() {
            if let Ok(doc) = resp.json::<serde_json::Value>().await {
                if doc.is_object() {
                    return Ok(overlay_proxy_info(doc));
                }
            }
        }
        Ok(synthesized_proxy_info())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;

    fn event(content: &str) -> Event {
        EventBuilder::text_note(content)
            .sign_with_keys(&Keys::generate())
            .expect("sign")
    }

    fn notif_event(sub: &str, event: Event) -> RelayPoolNotification {
        RelayPoolNotification::Message {
            relay_url: RelayUrl::parse("wss://up.test").unwrap(),
            message: RelayMessage::Event {
                subscription_id: Cow::Owned(SubscriptionId::new(sub)),
                event: Cow::Owned(event),
            },
        }
    }

    fn notif_eose(sub: &str) -> RelayPoolNotification {
        RelayPoolNotification::Message {
            relay_url: RelayUrl::parse("wss://up.test").unwrap(),
            message: RelayMessage::EndOfStoredEvents(Cow::Owned(SubscriptionId::new(sub))),
        }
    }

    fn notif_closed(sub: &str, msg: &str) -> RelayPoolNotification {
        RelayPoolNotification::Message {
            relay_url: RelayUrl::parse("wss://up.test").unwrap(),
            message: RelayMessage::Closed {
                subscription_id: Cow::Owned(SubscriptionId::new(sub)),
                message: Cow::Owned(msg.into()),
            },
        }
    }

    fn notif_ok(event_id: EventId) -> RelayPoolNotification {
        RelayPoolNotification::Message {
            relay_url: RelayUrl::parse("wss://up.test").unwrap(),
            message: RelayMessage::Ok {
                event_id,
                status: true,
                message: Cow::Borrowed(""),
            },
        }
    }

    fn as_array(s: &str) -> serde_json::Value {
        serde_json::from_str(s).unwrap()
    }

    #[test]
    fn maps_event_for_our_sub() {
        let up = SubscriptionId::new("upstream-1");
        let ev = event("hello");
        let n = notif_event("upstream-1", ev.clone());
        match map_notification(&n, &up, "client-sub") {
            Some(Forward::Continue(c)) => {
                let v = as_array(&c);
                assert_eq!(v[0], "EVENT");
                assert_eq!(v[1], "client-sub");
                assert_eq!(v[2]["content"], "hello");
            }
            other => panic!("expected Continue, got {other:?}"),
        }
    }

    #[test]
    fn maps_eose_for_our_sub() {
        let up = SubscriptionId::new("upstream-1");
        let n = notif_eose("upstream-1");
        match map_notification(&n, &up, "client-sub") {
            Some(Forward::Continue(c)) => {
                let v = as_array(&c);
                assert_eq!(v[0], "EOSE");
                assert_eq!(v[1], "client-sub");
            }
            other => panic!("expected Continue, got {other:?}"),
        }
    }

    #[test]
    fn maps_closed_for_our_sub_as_stop() {
        let up = SubscriptionId::new("upstream-1");
        let n = notif_closed("upstream-1", "restricted: not allowed");
        match map_notification(&n, &up, "client-sub") {
            Some(Forward::Stop(c)) => {
                let v = as_array(&c);
                assert_eq!(v[0], "CLOSED");
                assert_eq!(v[1], "client-sub");
                assert_eq!(v[2], "restricted: not allowed");
            }
            other => panic!("expected Stop, got {other:?}"),
        }
    }

    #[test]
    fn ignores_other_subs_and_unrelated_messages() {
        let up = SubscriptionId::new("upstream-1");
        // EVENT for a different upstream sub → ignored.
        assert!(map_notification(&notif_event("upstream-2", event("x")), &up, "c").is_none());
        // EOSE for a different sub → ignored.
        assert!(map_notification(&notif_eose("upstream-2"), &up, "c").is_none());
        // OK (publish ack) → not part of a subscribe stream.
        let id = event("y").id;
        assert!(map_notification(&notif_ok(id), &up, "c").is_none());
    }

    /// The whole point of the per-call random upstream id: events tagged with
    /// a different upstream sub must not leak into this stream.
    #[test]
    fn sub_isolation_by_upstream_id() {
        let mine = SubscriptionId::new("mine");
        let theirs = SubscriptionId::new("theirs");
        let n = notif_event("theirs", event("not mine"));
        assert!(map_notification(&n, &mine, "c").is_none());
        // and vice versa
        let n = notif_event("mine", event("mine"));
        assert!(map_notification(&n, &theirs, "c").is_none());
    }

    #[test]
    fn nip11_origin_converts_scheme_and_strips_path() {
        assert_eq!(
            nip11_origin("wss://relay.primal.net").as_deref(),
            Some("https://relay.primal.net")
        );
        assert_eq!(
            nip11_origin("ws://localhost:8080").as_deref(),
            Some("http://localhost:8080")
        );
        assert_eq!(
            nip11_origin("wss://relay.example.com/some/path?x=1").as_deref(),
            Some("https://relay.example.com")
        );
        assert!(nip11_origin("ftp://host").is_none());
    }

    #[test]
    fn overlay_stamps_outlay_identity_and_preserves_upstream_fields() {
        let doc = json!({
            "name": "Primal",
            "software": "primal-server",
            "version": "1.2.3",
            "supported_nips": [1, 11, 42],
            "custom_field": "preserved",
        });
        let out = overlay_proxy_info(doc);
        assert_eq!(out["software"], "outlay");
        assert_eq!(out["version"], OUTLAY_VERSION);
        assert_eq!(out["proxy"], true);
        // upstream identity preserved under `upstream`.
        assert_eq!(out["upstream"]["software"], "primal-server");
        assert_eq!(out["upstream"]["version"], "1.2.3");
        // other fields untouched (verbatim proxy).
        assert_eq!(out["name"], "Primal");
        assert_eq!(out["supported_nips"][2], 42);
        assert_eq!(out["custom_field"], "preserved");
    }

    #[test]
    fn overlay_without_upstream_software_omits_upstream_key() {
        let doc = json!({ "name": "bare relay" });
        let out = overlay_proxy_info(doc);
        assert_eq!(out["software"], "outlay");
        assert!(out.get("upstream").is_none());
    }
}
