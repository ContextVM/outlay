//! NIP-11 serving over HTTP. Content-negotiated: `Accept: application/nostr+json`
//! → the server's doc as JSON (+CORS), anything else → a rendered HTML page.
//! `/<pubkey>` proxies outlay's `relay_info`; `/` returns a synthesized shim doc
//! (design/shim.md §4).

use axum::body::Body;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::Response;
use rmcp::model::CallToolRequestParams;
use serde_json::Value;

use crate::config::ShimConfig;
use crate::path::{parse_path, ParsedPath};
use crate::transport::build_client;

const NIP11_JSON: &str = "application/nostr+json";

/// `GET /` — synthesized shim-level doc (JSON or HTML by Accept).
pub async fn serve_root(headers: &HeaderMap) -> Response {
    render(&synth_root(), headers)
}

/// `GET /<pubkey>` — the server's real NIP-11, fetched via outlay's `relay_info`.
pub async fn serve_pubkey(cfg: &ShimConfig, headers: &HeaderMap, pubkey: &str) -> Response {
    let parsed = match parse_path(pubkey) {
        Ok(p) => p,
        Err(e) => return plain(StatusCode::BAD_REQUEST, &e.to_string()),
    };
    let doc = match fetch_nip11(cfg, &parsed).await {
        Ok(d) => d,
        Err(e) => return plain(StatusCode::BAD_GATEWAY, &e.to_string()),
    };
    render(&doc, headers)
}

async fn fetch_nip11(cfg: &ShimConfig, parsed: &ParsedPath) -> anyhow::Result<Value> {
    // Transient transport per request (design §4). Slow; cache later.
    let (client, _handle) = build_client(cfg, parsed).await?;
    let result = client
        .peer()
        .call_tool(CallToolRequestParams::new("relay_info"))
        .await
        .map_err(|e| anyhow::anyhow!("relay_info: {e}"))?;
    let _ = client.cancel().await;
    Ok(result.structured_content.unwrap_or(Value::Null))
}

fn synth_root() -> Value {
    serde_json::json!({
        "name": "outlay-shim",
        "software": "outlay-shim",
        "version": env!("CARGO_PKG_VERSION"),
        "supported_nips": [1, 11],
        "limitation": { "bridge": true },
        "description": "Local NIP-01 bridge to CVM-exposed outlay servers. \
                        Connect at /<server-pubkey> (hex, npub, or nprofile).",
    })
}

/// Content-negotiate one doc into a JSON (+CORS) or HTML response.
fn render(doc: &Value, headers: &HeaderMap) -> Response {
    let wants_json = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.contains(NIP11_JSON))
        .unwrap_or(false);
    if wants_json {
        let body = serde_json::to_string_pretty(doc).unwrap_or_else(|_| "{}".into());
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, NIP11_JSON)
            .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .body(Body::from(body))
            .unwrap();
    }
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .body(Body::from(render_html(doc)))
        .unwrap()
}

fn plain(status: StatusCode, msg: &str) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from(msg.to_owned()))
        .unwrap()
}

fn render_html(doc: &Value) -> String {
    let name = doc
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("Nostr relay");
    let pretty = serde_json::to_string_pretty(doc).unwrap_or_else(|_| "{}".into());
    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><title>{title}</title>\
<style>body{{font-family:system-ui,sans-serif;max-width:46rem;margin:2rem auto;padding:0 1rem}}\
pre{{background:#f5f5f5;padding:1rem;overflow:auto;border-radius:.5rem}}</style></head>\
<body><h1>{name}</h1>\
<p>outlay-shim bridge. Nostr clients: request this URL with \
<code>Accept: application/nostr+json</code> for the NIP-11 document.</p>\
<pre>{doc}</pre></body></html>",
        title = esc(name),
        name = esc(name),
        doc = esc(&pretty),
    )
}

fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}
