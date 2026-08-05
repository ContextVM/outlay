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
use crate::transport::TransportCache;

const NIP11_JSON: &str = "application/nostr+json";

/// `GET /` — synthesized shim-level doc (JSON or HTML by Accept).
pub async fn serve_root(headers: &HeaderMap) -> Response {
    render(&synth_root(), headers, None)
}

/// `GET /<pubkey>` — the server's real NIP-11, fetched via outlay's `relay_info`.
pub async fn serve_pubkey(
    transports: &TransportCache,
    cfg: &ShimConfig,
    headers: &HeaderMap,
    pubkey: &str,
) -> Response {
    let parsed = match parse_path(pubkey) {
        Ok(p) => p,
        Err(e) => return plain(StatusCode::BAD_REQUEST, &e.to_string()),
    };
    let doc = match fetch_nip11(transports, cfg, &parsed).await {
        Ok(d) => d,
        Err(e) => return plain(StatusCode::BAD_GATEWAY, &e.to_string()),
    };
    // "Open in Jumble" target: the shim's public relay URL + this server's hex
    // pubkey, i.e. `wss://<host>/<hex>` — the bridge path a vanilla client dials.
    let jumble = cfg.public_url().map(|u| format!("{u}/{}", parsed.hex));
    render(&doc, headers, jumble.as_deref())
}

async fn fetch_nip11(
    transports: &TransportCache,
    cfg: &ShimConfig,
    parsed: &ParsedPath,
) -> anyhow::Result<Value> {
    // Shares the cached transport for this outlay (one relay subscription, no
    // per-request churn). `_handle` is unused — `relay_info` is a plain RPC.
    let (peer, _handle) = transports.get(cfg, parsed).await?;
    let result = peer
        .call_tool(CallToolRequestParams::new("relay_info"))
        .await
        .map_err(|e| anyhow::anyhow!("relay_info: {e}"))?;
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
fn render(doc: &Value, headers: &HeaderMap, jumble: Option<&str>) -> Response {
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
        .body(Body::from(render_html(doc, jumble)))
        .unwrap()
}

fn plain(status: StatusCode, msg: &str) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from(msg.to_owned()))
        .unwrap()
}

fn render_html(doc: &Value, jumble: Option<&str>) -> String {
    let name = doc
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("Nostr relay");
    let pretty = serde_json::to_string_pretty(doc).unwrap_or_else(|_| "{}".into());
    let jumble_btn = match jumble {
        Some(relay) => format!(
            "<p><a class=\"btn\" href=\"https://jumble.social/?r={relay}\" \
target=\"_blank\" rel=\"noopener noreferrer\">Open feed in Jumble ↗</a></p>",
            relay = esc(relay)
        ),
        None => String::new(),
    };
    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><title>{title}</title>\
<style>body{{font-family:system-ui,sans-serif;max-width:46rem;margin:2rem auto;padding:0 1rem}}\
.btn{{display:inline-block;padding:.5rem 1rem;background:#6c4efc;color:#fff;text-decoration:none;border-radius:.5rem}}\
pre{{background:#f5f5f5;padding:1rem;overflow:auto;border-radius:.5rem}}</style></head>\
<body><h1>{name}</h1>\
<p>outlay-shim bridge. Nostr clients: request this URL with \
<code>Accept: application/nostr+json</code> for the NIP-11 document.</p>\
{jumble_btn}\
<pre>{doc}</pre></body></html>",
        title = esc(name),
        name = esc(name),
        jumble_btn = jumble_btn,
        doc = esc(&pretty),
    )
}

fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}
