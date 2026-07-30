# outlay-shim — Design

`outlay-shim` is the companion to `outlay`: a localhost WebSocket/HTTP server
that lets **vanilla NIP-01 clients** (gossip, web wallets, browser apps) reach
**CVM-exposed outlay servers** they otherwise could not speak to. It is a
multiplexer — one local port, path-keyed by server pubkey — that translates
plain NIP-01 frames into outlay's CVM tool calls and back.

Point a vanilla client at `ws://localhost:<port>/<server-pubkey>` and it sees a
working Nostr relay. The shim opens a CVM client transport to that outlay server
and bridges traffic both ways.

> **Status:** v1 PoC implemented (`crates/outlay-shim`). HTTP NIP-11
> serving (content-negotiated JSON/HTML, synthesized root) is exercised live;
> the WS↔CVM↔upstream frame path compiles against the SDK and awaits a smoke test
> mirroring `outlay`'s. Authz, NIP-42, TLS/public binding, and transport pooling
> remain out of scope (§10). The CVM client half is already proven by `outlay`'s
> smoke tests.

## 1. Role: a CVM *client* (and an optional memoryless relay) for outlay

The shim is a CVM **client** of outlay, and a NIP-01 **server** to vanilla
clients. It holds no database, no event store, and no subscription state beyond
routing — it is a pure protocol translator. This is the key framing: every
NIP-01 frame maps to an outlay tool call defined in [`design.md`](./design.md) §2.

It **additionally** serves an optional **memoryless NIP-01 relay at `/`** (§12):
a storage-less `LocalRelay` (reused from `outlay-relay`, backed by a
`MemorylessDatabase`) that broadcasts live events to current subscribers and
retains nothing. This is outlay's **default CVM transport relay**
(`wss://nostr.wtf`, where the shim is hosted), collapsing the transport relay
into the shim and removing one network hop. An outlay opts *out* by setting
`OUTLAY_RELAY_URLS` to a third-party relay (e.g. `wss://relay.contextvm.org`);
the bridge follows each outlay's nprofile relay hint (§3), so mixed deployments
coexist. The relay is event-driven and zero-cost when idle, defaulting on
(`OUTLAY_SHIM_RELAY=false` to disable).

**The shim does not depend on the `outlay` crate.** It speaks the CVM protocol
(`contextvm-sdk`) to any outlay server. `outlay` and `outlay-shim` are siblings,
both children of `contextvm-sdk`:

```
outlay        → contextvm-sdk  (server side: rmcp handler over NostrServerTransport)
outlay-shim   → contextvm-sdk  (client side: NostrClientTransport)
                 ↑ the shim NEVER imports outlay's proxy/handler code
```

## 2. Core mapping: NIP-01 frame ↔ CVM tool call

The translation mirrors outlay's tool surface (`design.md` §2–3) one-to-one. One
WS message in → one tool call; streamed tool chunks → WS frames out.

| Vanilla client → shim (NIP-01 WS) | shim → outlay (CVM)                    | shim → vanilla client (NIP-01 WS) |
|-----------------------------------|----------------------------------------|-----------------------------------|
| `["REQ", sub, f1, f2, …]`         | `subscribe{subscription_id: sub, filters: [f1,…]}` (streaming) | stream: `["EVENT",sub,e]`, `["EOSE",sub]`, `["CLOSED",sub,msg]` |
| `["CLOSE", sub]`                  | `abort()` the subscribe call handle    | — |
| `["EVENT", e]` (publish, 2-elem)  | `publish_event{event: e}` (sync)       | `["OK", e.id, ok, msg]` (synthesized from `PublishOutcome`) |
| _(HTTP GET, not WS)_              | `relay_info()` (sync)                  | NIP-11 JSON or HTML over HTTP (§4) |

**EVENT direction disambiguation.** A vanilla client sends the 2-element
`["EVENT", e]` to publish; the 3-element `["EVENT", sub, e]` is only ever
*sent by* a relay, never received. So the shim dispatches client `EVENT` frames
as publishes unambiguously (it is the server side of the WS).

**No NOTICE/OK ambiguity from outlay.** outlay never emits `NOTICE`, and
`publish_event` returns a structured `{ ok, event_id, message }` the shim
synthesizes the `OK` frame from. The shim's outbound frame set is fully known.

## 3. Addressing: path-keyed by server pubkey

```
ws://<host>:<port>/<server-pubkey>
```

`<server-pubkey>` identifies the **outlay server's CVM identity** (not a Nostr
user). Three accepted encodings, parsed with `nostr`'s NIP-19 helpers:

| Form          | Parse                                                | Yields |
|---------------|------------------------------------------------------|--------|
| 64-char hex   | `PublicKey::from_hex`                                | pubkey |
| `npub1…`      | `Nip19::from_bech32` → `Nip19::User(pk)`             | pubkey |
| `nprofile1…`  | `Nip19::from_bech32` → `Nip19::Profile(profile)`     | pubkey **+ relay hints** (`profile.relays`) |

**Relay selection:** if the address is an `nprofile` with relay hints, those
hints are the CVM relays used to reach the server. Otherwise fall back to
`OUTLAY_SHIM_RELAY_URLS` (default `wss://nostr.wtf`). Hints win over
env; env wins over the default.

Invalid path segment → HTTP `400` (HTTP) or WS-close-with-`NOTICE` (WS).

## 4. NIP-11 serving (HTTP, content-negotiated)

NIP-11 is HTTP, not WS — a separate concern from the frame loop. Borrowed from
`nostr-rs-relay`'s pattern (see `reference/nostr-rs-relay/src/server.rs`):
**branch on the `Accept` header** to serve machines and humans from one URL.

| Request                                  | Response |
|------------------------------------------|----------|
| `GET /<pubkey>` + `Accept: application/nostr+json` | The server's NIP-11 doc verbatim from outlay's `relay_info` tool (outlay already overlays outlay's identity — `design.md` §5). `Content-Type: application/nostr+json`, `Access-Control-Allow-Origin: *`. |
| `GET /<pubkey>` (any other Accept, e.g. a browser) | A rendered **HTML** page summarizing the doc — the human-facing view. |
| `GET /`                                  | A synthesized shim-level doc (`software: "outlay-shim"`). JSON or HTML by the same Accept rule. |

**Why both JSON and HTML:** NIP-11 itself is JSON (`application/nostr+json`) and
Nostr clients expect exactly that. The HTML view is for humans who open the URL
in a browser. Serving HTML does **not** replace the JSON — it is content
negotiation, same path, two representations.

**Root-NIP-11 caveat (known limitation).** NIP-11 says clients fetch the doc at
the origin *root* (scheme+host+port), dropping the path. A spec-compliant client
pointed at `ws://host:port/<pubkey>` may therefore GET `http://host:port/` and
hit the synthesized shim doc, not that specific server's. NIP-11 is advisory, so
clients still function; the WS connection at `/<pubkey>` is unaffected. Faithful
per-server NIP-11 requires the path. Revisit (e.g. port-per-server mode) only if
a real client breaks.

**NIP-11 needs a live transport.** `relay_info` is a CVM tool call, so serving
`/<pubkey>`'s JSON requires a connected transport to that server. PoC: open a
transient transport per HTTP request. This is slow (full handshake); add a
short TTL cache (§10) the moment a second fetch feels slow. The WS connection's
own transport is separate and already open.

## 5. Connection architecture

One task per vanilla WS connection; a **single writer** to the WS sink;
per-subscribe child tasks feeding an outbound channel. This is the
`nostr-rs-relay` central-`select!` loop, simplified (their `query_rx`/`bcast_rx`
collapse into one `outbound_rx`).

```
on WS upgrade at /<pubkey>:
  parse pubkey + relay hints (§3)
  open NostrClientTransport → server (EncryptionMode from config); timeout §9.1
  on failure: ws.send(["NOTICE","error: ..."]) ; close
  (outbound_tx, outbound_rx) = mpsc::channel        // bounded; the single writer funnel
  select! {
    shutdown            => break
    ping timer          => outbound_tx.send(Ping)
    inbound = ws.next() =>
        ["REQ", sub, …]   => spawn task owning the subscribe stream;
                             it reads chunks and pushes ["EVENT",sub,e]/["EOSE",sub]/["CLOSED",sub,msg]
                             into outbound_tx; store its JoinHandle/abort in subs[sub]
        ["CLOSE", sub]    => abort subs[sub]; remove
        ["EVENT", e]      => call publish_event; push ["OK", id, ok, msg] to outbound_tx
    frame = outbound_rx  => ws.send(frame)          // the ONLY writer to the sink
  }
  on break: drop outbound_tx → all subscribe tasks end → drop the transport
```

**Why single-writer:** no locking the WS sink, clean backpressure (a slow
client fills `outbound_tx`, which blocks the subscribe-task readers, which
propagates to the outlay stream), and trivial shutdown (drop the loop → drop the
channel → cancel every child). Per-connection transport gives sub-id isolation
for free (each connection owns its sub namespace; even a *shared* transport
wouldn't collide because each `subscribe` is its own CEP-41 stream, demuxed by
call not sub_id — `design.md` §8.5).

**NIP-01 REQ-replace:** a client may re-`REQ` on a live `sub_id` to replace it.
Shim aborts the stored handle, then starts the new call.

## 6. CVM client transport

- One `NostrClientTransport` per WS connection (PoC). `server_pubkey` from the
  path (passed through to `with_server_pubkey` as-is — the SDK accepts hex /
  npub / nprofile and runs relay resolution itself); `relay_urls` from env,
 *unless* the path is an nprofile with relay hints (then `relay_urls` is left
  empty so the SDK resolves via the hints).
- **Stateless:** `with_stateless(true)` — the client emulates `initialize`
  locally and never sends it over the wire. outlay is a stateless proxy (each
  tool call is independent; no server session to establish), so the MCP
  handshake buys nothing but a round-trip over slow relays. Skipping it cuts the
  per-connection startup cost hard (§9.1). Verified compatible: the SDK's own
  greybox harness drives a stateless client against a real `NostrServerTransport`
  and its `tools/call` is processed (`reference/rs-sdk/tests/oversized_timeout_e2e.rs`).
  No server-side change required.
- `EncryptionMode` from config (default `optional`, matching outlay's server).
  `disabled` is fine for plaintext-only outlay servers and skips the NIP-44
  handshake latency; `required` when a server demands encryption. Capability
  learning (encryption / gift-wrap) still happens post-startup via inbound
  discovery tags even in stateless mode.
- Client key: ephemeral per run, unless `OUTLAY_SHIM_PRIVATE_KEY` is set.
- `with_open_stream(OpenStreamConfig::enabled())` so `subscribe` can stream.
  Grab `transport.open_stream_handle()` *before* `serve()` consumes the transport.
- `DemoClient.serve(transport)` auto-starts the relay connection and drives the
  (emulated) handshake; no explicit `start()`. Connect is awaited with a timeout
  (§9.1); the vanilla client's first frame is not processed until the transport
  is ready.

## 7. Crate and repo layout (workspace)

The shim is a separate crate. The repo becomes a Cargo workspace:

```
outlay/                          (repo root → workspace)
  Cargo.toml                     [workspace] members = ["crates/*"]
  crates/
    outlay/                      (server — current src/ + tests/ move here)
      Cargo.toml  src/...  tests/...
    outlay-shim/                 (the bridge — this design)
      Cargo.toml  src/...
  design/
    design.md   shim.md          (this file)
  README.md  AGENTS.md  .gitignore
```

- **Enabling step (first implementation task):** move the existing crate into
  `crates/outlay/`, add the root workspace `Cargo.toml` (members + shared
  dependency versions via the one lockfile), then add `crates/outlay-shim/`.
- **No shared core crate.** The shim speaks CVM types, not outlay-internal
  types. Add `crates/outlay-core` only if a genuinely shared pure surface
  emerges (YAGNI).

Internal shim module layout (modeled on `nostr-rs-relay`):

```
crates/outlay-shim/src/
  main.rs         config (env) + axum serve + banner; spawns the memoryless relay
  config.rs       env config + defaults
  server.rs       axum routes, WS upgrade vs HTTP dispatch (Upgrade header)
  conn.rs         per-connection state: HashMap<sub_id, subscribe handle>
  relay.rs        memoryless relay: spawn + `/` upgrade frame-pipe to LocalRelay
  translate.rs    NIP-01 frame ↔ CVM call translation (the testable pure seam)
  nip11.rs        relay_info fetch + JSON/HTML rendering + synthesized root doc
  path.rs         hex/npub/nprofile pubkey + relay-hint parsing
```

HTTP/WS stack: **axum** (`axum::extract::ws` is tungstenite underneath). The
`/` and `/<pubkey>` handlers both inspect the `Upgrade` header — WS upgrade vs
HTTP, mirroring `nostr-rs-relay`'s `(path, has_upgrade)` dispatch with a fraction
of the glue.

## 8. Configuration

Loaded from `.env` then `.env.local` (first-write-wins), then the process env.

| Variable                       | Default                    | Notes |
|--------------------------------|----------------------------|-------|
| `OUTLAY_SHIM_LISTEN_ADDR`      | `127.0.0.1:8088`           | Local bind. Localhost-only by design (§9.7). |
| `OUTLAY_SHIM_RELAY_URLS`       | `wss://nostr.wtf`          | CVM relays (comma-sep). Overridden by nprofile hints.   |
| `OUTLAY_SHIM_PRIVATE_KEY`      | _(ephemeral)_              | hex/nsec client key. New key each run if unset. |
| `OUTLAY_SHIM_ENCRYPTION_MODE`  | `optional`                 | `optional`/`disabled`/`required`. Must be compatible with the server. |
| `OUTLAY_SHIM_CONNECT_TIMEOUT`  | `15` (seconds)             | CVM transport handshake timeout. |
| `OUTLAY_SHIM_MAX_WS_MESSAGE_BYTES` | `1048576` (1 MiB)      | WS frame/message limit (borrowed from `nostr-rs-relay`). |
| `OUTLAY_SHIM_RELAY`            | `true`                     | Run the colocated memoryless relay at `/` (§12). |

## 9. Gotchas

Ranked by severity.

1. **Connect latency (mitigated, not eliminated).** Stateless mode drops the
   `initialize` round-trip (§6), so startup is just relay-connect + first
   response. That is still not instant on real relays, so the vanilla client's
   WS connection must wait until the transport is ready (or fail loudly) before
   processing frames; never let a `REQ` race a half-open transport. On timeout,
   send `["NOTICE","error: …"]` and close.
2. **Encryption mode must match the server.** outlay servers run `optional`.
   A `disabled` shim works against an `optional` server; a `required` shim
   against a `disabled` server fails silently. This is a config concern, not a
   protocol one — make it a knob, not a hardcoded guess.
3. **NIP-11-at-root caveat** (§4). Spec-compliant clients fetch the origin root
   and miss the per-server doc. Advisory only; revisit if a real client breaks.
4. **NIP-11 needs a live transport** (§4). Transient-per-request in the PoC is
   slow; add a TTL cache before it bothers anyone.
5. **Subscription lifecycle (REQ-replace + CLOSE).** REQ on an existing
   `sub_id` replaces it — abort the old task first. CLOSE cancels by **aborting
   the subscribe task's `JoinHandle`** (which drops the `ToolStreamCall`), not by
   awaiting `call.abort()`: that can't run inside `tokio::spawn` because
   `ToolStreamCall` is `!Sync` (its `result: BoxFuture` field), so `&call` isn't
   `Send`. The upstream subscription is therefore closed **eventually** — when
   outlay's open-stream keepalive notices the dead reader — so a CLOSE may
   briefly be followed by a few more events. Upgrade path: a Send-safe
   cancel-by-token API in the SDK.
6. **Backpressure.** `outbound_tx` must be bounded. A slow vanilla client fills
   it, which blocks the subscribe-task readers, propagating backpressure to the
   outlay stream. Choose "block" (safe) over "drop" or "close" for PoC.
7. **Threat model = localhost.** No authz, no TLS, no rate limiting in the PoC.
   The shim serves the user's own Nostr software on `127.0.0.1`. Do not expose
   it remotely. (This is precisely why authz was deferred until the shim existed
   — the shim *is* what clarifies the trust model.)
8. **The shim is a CVM client with its own identity.** It needs a keypair
   (ephemeral or configured). Authz is deferred, so the key gates nothing yet —
   but it exists and is part of the config surface.

## 10. Roadmap

- **v1 (PoC — implemented & smoke-tested):** workspace + `outlay-shim` crate;
  axum WS+HTTP on `127.0.0.1`; path-keyed pubkey (hex/npub/nprofile);
  per-connection stateless CVM transport; full `REQ`/`CLOSE`/`EVENT` translation;
  content-negotiated NIP-11 (JSON + HTML) at `/<pubkey>`, synthesized doc at `/`;
  ephemeral/configured key; WS frame limits + tungstenite auto-pong.
  Smoke-tested end-to-end: vanilla NIP-01 WS client → shim → outlay
  (network-free CVM hop over a `MockRelayPool`) → real upstream. Five tests,
  all green — `subscribe` (streaming EVENTS+EOSE), `publish_event` (OK),
  `relay_info` (HTTP JSON), `close_cancels_open_subscription` (the
  `JoinHandle::abort` cancellation path: 0 events leak after CLOSE, connection
  stays usable), and `concurrent_subscriptions_isolated` (two concurrent CEP-41
  streams through the single per-connection writer, no sub_id cross-talk).
- **Done (§12):** colocated **memoryless relay at `/`**, so outlays can collapse
  their CVM transport relay into the shim. `outlay-relay`'s `LocalRelay` reused
  with a `MemorylessDatabase`; axum `/` upgrade is a verbatim frame-pipe to it.
  Network-free integration test in `crates/outlay-shim/tests/relay.rs` (REQ →
  EOSE, publish → live EVENT + OK, and no-backfill on a later REQ).
- **Next:** NIP-11 TTL cache; transport pooling by server-pubkey (shared across
  WS connections and NIP-11 fetches — safe, because CEP-41 streams demux per
  call); bridge self-transport loopback shortcut to the `/` relay (skip the
  reverse-proxy hop when a hinted outlay lives on the shim's own public URL).
- **Later:** authz + NIP-42 (once the trust model this shim exposes is clear);
  TLS / public bind; rate-limiting + metrics (`governor` + `prometheus`, as
  `nostr-rs-relay` does); multi-relay fan-out.

## 12. Memoryless relay endpoint at `/`

The shim serves an **ephemeral, storage-less NIP-01 relay** at `/`, on by
default (`OUTLAY_SHIM_RELAY=false` disables). It is outlay's **default CVM
transport relay** (`wss://nostr.wtf`, where the shim is hosted), collapsing the
transport relay into the shim — one fewer network hop and no dependency on a
third-party relay for the transport:

```text
  default (nostr.wtf): outlay ──CVM transport──► shim:/ (relay) ──► shim:/<pubkey> (bridge) ──► vanilla client
  opt-out:             outlay ──CVM transport──► <third-party relay, e.g. relay.contextvm.org> ──► shim:/<pubkey> ──► vanilla client
```

**Addressing-driven.** The bridge follows each outlay's nprofile relay hint
(§3), so it reaches that outlay on the shim's relay automatically. An outlay
opts *out* of the collapse by setting `OUTLAY_RELAY_URLS` to a third-party relay
(and advertising it as its hint); mixed deployments coexist on one shim.

**Reuse, not reinvention.** The relay is `outlay-relay`'s `LocalRelay`
(`nostr-sdk` 0.45-alpha, isolated from the shim's 0.44 by the plain-URL boundary)
 plugged with a `MemorylessDatabase` — a `NostrDatabase` whose `save_event`
returns `Success` without storing (so `LocalRelay`'s in-memory broadcast still
fires to live subscribers) and whose `query` returns empty (so every `REQ`
 `EOSE`s at once, then streams live events only). The SDK therefore owns every
NIP-01 semantic (REQ/EVENT/CLOSE/EOSE/OK/NOTICE, filter matching, REQ-replace,
rate limits, id verification, broadcast backpressure); the shim owns only a
**verbatim frame-pipe** from the axum `/` WS upgrade to the loopback `LocalRelay`
(no interpretation). It accepts every kind and stores nothing — correct for
NIP-01 ephemeral events (kinds 20000–29999, e.g. CVM's kind-21059 gift wraps),
which relays must broadcast live and must not persist.

**Why memoryless.** The CVM transport subscribes with `since: now`, i.e. it asks
for no backfill; ephemeral gift wraps are spec-defined as broadcast-only. So a
 storage-less relay matches the transport's contract exactly, with the lowest
 memory pressure and no storage DoS surface. A message published in the gap
 before a recipient's `REQ` lands is dropped (self-heals via the caller's retry);
 this is the same behavior any relay has for ephemeral events on reconnect.

**Co-location / public exposure.** The shim already runs behind a reverse proxy
on clearnet, so outlays anywhere can reach `wss://<shim>/`. The relay is a public
endpoint; lean on the reverse proxy for TLS / per-IP rate-limiting / connection
 caps (the relay itself inherits `LocalRelay`'s per-connection REQ limits, sub-id
length cap, filter-limit cap, and id verification). Authz remains deferred (§9.7).

### 12.1 Bridge loopback shortcut (the hairpin fix)

Collapsing the transport relay into the shim creates a trap: the bridge's CVM
client transport reaches an outlay by dialing a CVM relay, and the outlay's
relay — per the collapse — is the shim's **own public URL** (`wss://nostr.wtf`).
So the shim host opens an outbound TLS connection to its own public address,
which has to hairpin through its reverse proxy / NAT. On most deployments that
self-connection never completes (the SYN goes unanswered) and the transport's
`connect_timeout` fires — the 0.3.0 regression: `nak` against the `/` relay works
(external → server), but `/<outlay-pubkey>` times out (server → its own public
URL). Local dev never saw it because `127.0.0.1` loopback always succeeds.

The fix: when the colocated relay is on, the bridge never dials a public URL
that is its own — it swaps in the relay's loopback address instead.
`transport::resolve_relay_urls` builds the candidate URL list (nprofile hints if
present, else `OUTLAY_SHIM_RELAY_URLS`) and rewrites any candidate matching one
of the shim's own public URLs to the loopback relay. Comparison is normalized
through `RelayUrl` (trailing-slash- and default-port-tolerant). Candidates that
are genuine third-party relays pass through untouched, so nprofile hints to
other relays keep working.

The allowlist is `OUTLAY_SHIM_PUBLIC_URLS`; when unset it defaults to
`OUTLAY_SHIM_RELAY_URLS`, so the common deployment (transport relay == shim's
public face) works with no extra config. The shortcut is inactive when the
colocated relay is off — then there is nothing to loop back to and candidates
are dialed verbatim.

Relay selection is now fully the shim's: it always supplies explicit `relay_urls`
(stage 1 of the SDK's CEP-17 resolution, which overrides nprofile hints), and
passes the hex pubkey to `with_server_pubkey`.

## 11. Locked decisions

1. **Repo:** Cargo workspace, `crates/outlay` + `crates/outlay-shim`. No shared
   core crate. The shim does **not** depend on `outlay`.
2. **Addressing:** path-keyed, `ws://host:port/<server-pubkey>`. Multiplexer.
3. **Path pubkey:** hex / npub / nprofile; nprofile relay hints override
   `OUTLAY_SHIM_RELAY_URLS` (default `wss://nostr.wtf`).
4. **NIP-11:** content-negotiated — `Accept: application/nostr+json` → JSON from
   outlay `relay_info` (+CORS); else → HTML. Both at `/<pubkey>`. Synthesized
   shim doc at `/`.
5. **HTTP/WS stack:** axum; WS-vs-HTTP dispatched on the `Upgrade` header.
6. **Connection model:** per-connection CVM transport; central `select!` loop,
   single WS writer, per-subscribe child tasks.
7. **Translation:** one NIP-01 frame ↔ one CVM tool call (`design.md` §2–3);
   `publish_event` result synthesized into an `OK` frame.
8. **Encryption:** `EncryptionMode::Optional` default (configurable).
9. **Stateless client:** `with_stateless(true)` — no `initialize` handshake.
   outlay is a stateless proxy; the SDK's stateless harness proves a stateless
   client's `tools/call` is processed by a real server. Hardcoded (no env knob);
   revisit only if a server's real `initialize` payload is ever needed.
10. **Client key:** ephemeral per run unless `OUTLAY_SHIM_PRIVATE_KEY` is set.
11. **`/` relay (§12):** an optional memoryless `LocalRelay` (reused from
    `outlay-relay` with a `MemorylessDatabase`), served at `/` by a verbatim
    axum WS frame-pipe. On by default (`OUTLAY_SHIM_RELAY=false` disables).
    Accepts all kinds; stores nothing. Outlays opt in via nprofile relay hints.
12. **Bridge loopback shortcut (§12.1):** when the colocated relay is enabled,
    the bridge's CVM transport rewrites any of the shim's own public URLs
    (`OUTLAY_SHIM_PUBLIC_URLS`, defaulting to `OUTLAY_SHIM_RELAY_URLS`) in its
    relay-URL candidates — nprofile hints or the configured fallback — to the
    relay's loopback address, so it never hairpin-dials its public URL. Explicit
    `relay_urls` is always supplied (stage 1 of CEP-17 resolution), so the shim
    owns relay selection; third-party hints are dialed as-is. Without this the
    collapse times out (0.3.0 regression).
13. **Out of PoC:** authz, NIP-42, transport pooling, NIP-11 cache, TLS/public
    bind, rate-limiting, metrics.
