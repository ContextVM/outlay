//! rmcp server handler. Thin glue that adapts [`OpenStreamWriter`] to the
//! proxy's [`MessageSink`] and exposes the CVM tools. Mirrors
//! `cordn-server/src/methods.rs`.
//!
//! Tools (design §3):
//! - `subscribe`     — streaming; one call == one NIP-01 subscription.
//! - `publish_event` — synchronous; returns the upstream OK status.
//!
//! `relay_info` (NIP-11) lands in the next step with its HTTP client.

use std::sync::Arc;

use async_trait::async_trait;
use contextvm_sdk::transport::open_stream::OpenStreamWriter;
use rmcp::{
    handler::server::wrapper::Parameters,
    model::{CallToolResult, ErrorData, Implementation, ServerCapabilities},
    schemars,
    service::{RequestContext, RoleServer},
    tool, tool_handler, tool_router, ServerHandler,
};
use serde::Serialize;

use crate::proxy::{MessageSink, Proxy, ProxyError, PublishOutcome};

#[derive(Clone)]
pub struct OutlayServer {
    proxy: Arc<Proxy>,
}

impl OutlayServer {
    pub fn new(proxy: Arc<Proxy>) -> Self {
        Self { proxy }
    }
}

/// Adapts the rmcp `OpenStreamWriter` to the proxy's [`MessageSink`] trait.
/// Ported from cordn-server/src/methods.rs.
struct StreamWriter(OpenStreamWriter);

#[async_trait]
impl MessageSink for StreamWriter {
    async fn start(&self) -> bool {
        self.0.start().await.is_ok()
    }
    async fn write(&self, msg: String) -> bool {
        self.0.write(msg).await.is_ok() && self.0.is_active()
    }
    fn is_active(&self) -> bool {
        self.0.is_active()
    }
    async fn close(&self) {
        let _ = self.0.close().await;
    }
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct SubscribeParams {
    /// NIP-01 subscription id, client-chosen (max 64 chars). Echoed back in
    /// every streamed EVENT/EOSE/CLOSED chunk.
    subscription_id: String,
    /// NIP-01 filter objects, forwarded verbatim to the upstream relay.
    filters: Vec<serde_json::Value>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct PublishEventParams {
    /// A client-signed NIP-01 event object, forwarded verbatim (never re-signed).
    event: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct SubscribeOutput {
    ok: bool,
}

fn stream_writer(ctx: &RequestContext<RoleServer>) -> Result<StreamWriter, ErrorData> {
    ctx.extensions
        .get::<OpenStreamWriter>()
        .cloned()
        .map(StreamWriter)
        .ok_or_else(|| {
            ErrorData::invalid_params(
                "subscribe requires CEP-41 open-stream; the client must advertise support \
                 and send a progressToken",
                None,
            )
        })
}

fn structured<T: Serialize>(out: T) -> CallToolResult {
    let mut result = CallToolResult::success(vec![]);
    result.structured_content = Some(serde_json::to_value(&out).unwrap_or(serde_json::Value::Null));
    result
}

fn proxy_error(e: ProxyError) -> ErrorData {
    ErrorData::invalid_params(e.to_string(), None)
}

#[tool_router]
impl OutlayServer {
    #[tool(
        description = "Open a NIP-01 subscription on the proxied relay. Streams \
                       [\"EVENT\",sub,e], [\"EOSE\",sub], and [\"CLOSED\",sub,msg] chunks as \
                       they arrive. Cancel by aborting the call (equivalent to NIP-01 CLOSE)."
    )]
    async fn subscribe(
        &self,
        Parameters(p): Parameters<SubscribeParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let sink = stream_writer(&ctx)?;
        let filters: Vec<nostr_sdk::Filter> = p
            .filters
            .into_iter()
            .map(serde_json::from_value::<nostr_sdk::Filter>)
            .collect::<Result<_, _>>()
            .map_err(|e| ErrorData::invalid_params(e.to_string(), None))?;

        self.proxy
            .subscribe(p.subscription_id, filters, &sink)
            .await
            .map_err(proxy_error)?;

        // Final result stays tiny: the open-stream deferred final-response path
        // is not CEP-22-fragmented (design §8.9). Bulk payload rides the stream.
        Ok(structured(SubscribeOutput { ok: true }))
    }

    #[tool(
        description = "Forward a client-signed NIP-01 event to the proxied relay verbatim \
                       (not re-signed). Returns { ok, event_id, message } mirroring the \
                       upstream OK message."
    )]
    async fn publish_event(
        &self,
        Parameters(p): Parameters<PublishEventParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let event: nostr_sdk::Event = serde_json::from_value(p.event)
            .map_err(|e| ErrorData::invalid_params(e.to_string(), None))?;
        let out: PublishOutcome = self.proxy.publish_event(event).await.map_err(proxy_error)?;
        Ok(structured(out))
    }
}

#[tool_handler]
impl ServerHandler for OutlayServer {
    fn get_info(&self) -> rmcp::model::ServerInfo {
        rmcp::model::ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(
                Implementation::new("outlay", env!("CARGO_PKG_VERSION"))
                    .with_title("outlay — Nostr relay over ContextVM"),
            )
            .with_instructions(
                "Transparent Nostr relay proxy. Call subscribe (streaming) to open a NIP-01 \
                 subscription, or publish_event to forward a signed event.",
            )
    }
}
