# outlay

A Nostr relay exposed as a **ContextVM (CVM)** server. `outlay` is a transparent
proxy that binds CVM tool calls to NIP-01 relay traffic: a CVM client calls
`subscribe`/`publish_event`, and outlay translates each call into the
corresponding NIP-01 exchange with an upstream relay, streaming relay events
back over [CEP-41 open-stream](https://github.com/ContextVM/CEPs).

Point it at any relay — clearnet (`wss://relay.primal.net`) or localhost
(`ws://localhost:8080`) — and anything that speaks CVM can read/write that relay
through it.

> **Status:** v1 — proxy mode, working and tested end-to-end against a live
> relay. `subscribe` and `publish_event` are implemented; `relay_info` (NIP-11),
> the companion WS shim for vanilla Nostr clients, and a bundled in-process
> relay are on the roadmap. See [`design/design.md`](./design/design.md) for the
> locked design.

## How it works

A CVM server is an rmcp handler run over `NostrServerTransport` — its surface is
**MCP tools**, not raw WebSocket frames. So "a relay over CVM" means the *tool
surface and streamed payload mirror NIP-01's message shapes*, with CEP-41
open-stream carrying the relay→client direction. Each open-stream chunk is one
verbatim NIP-01 relay→client JSON array.

The core mapping: **one CEP-41 stream == one NIP-01 subscription.**

| NIP-01 (relay)            | CVM (outlay)                                                     |
|---------------------------|------------------------------------------------------------------|
| `["REQ", sub, filters]`   | `tools/call subscribe{subscription_id, filters}` + `progressToken` |
| `["EVENT", sub, e]`       | open-stream chunk `["EVENT","sub",{event}]`                      |
| `["EOSE", sub]`           | open-stream chunk `["EOSE","sub"]`                               |
| `["CLOSED", sub, msg]`    | open-stream chunk `["CLOSED","sub","msg"]`                       |
| `["CLOSE", sub]`          | client aborts the stream (`call.abort()`)                        |
| `["EVENT", e]` (publish)  | `tools/call publish_event{event}` → `{ok, event_id, message}`    |

```text
   CVM client ──── CVM tools over Nostr ──── outlay server ──── NIP-01 ws ──── upstream relay
                   (CEP-41 open-stream)         (proxy + rmcp)                  (any relay)
```

Two independent relay connections live inside outlay:

- **Upstream pool** — outlay's own `Proxy`, a `nostr-sdk` `Client` connected to
  the single configured upstream relay. Uses a throwaway ephemeral key:
  published events are forwarded **verbatim** (client-signed), never re-signed.
- **CVM transport** — the `NostrServerTransport` that CVM clients connect
  through, on the ContextVM relays you configure.

## Quick start

Requires Rust stable (MSRV **1.88**).

```sh
# Proxy a public relay:
OUTLAY_PROXY_RELAY_URL=wss://relay.primal.net cargo run

# Or a local one:
OUTLAY_PROXY_RELAY_URL=ws://localhost:8080 cargo run
```

On startup outlay prints its server pubkey, the CVM relays it listens on, and
the upstream relay it proxies.

## Configuration

Loaded from `.env` then `.env.local` (first-write-wins per key), then the
process environment.

| Variable                      | Default                    | Description                                            |
|-------------------------------|----------------------------|--------------------------------------------------------|
| `OUTLAY_PROXY_RELAY_URL`      | _(required)_               | Upstream relay to proxy (`ws://`/`wss://`).            |
| `OUTLAY_RELAY_URLS`           | `wss://relay.contextvm.org`| Comma-separated CVM relays the server listens on.      |
| `OUTLAY_SERVER_PRIVATE_KEY`   | _(ephemeral)_              | Hex/nsec server key. Unset → new key each start.       |
| `OUTLAY_SERVER_NAME`          | `outlay`                   | CVM profile name.                                      |
| `OUTLAY_SERVER_ABOUT`         | _(none)_                   | CVM profile about.                                     |
| `OUTLAY_ANNOUNCED`            | `false`                    | Public discovery (kind 11316) on/off.                  |

## CVM tool surface

- **`subscribe(subscription_id, filters)`** — streaming. Opens a NIP-01
  subscription upstream and streams `EVENT`/`EOSE`/`CLOSED` chunks. Cancel by
  aborting the call (= NIP-01 `CLOSE`).
- **`publish_event(event)`** — synchronous. Forwards a client-signed event
  verbatim; returns `{ ok, event_id, message }` mirroring the upstream `OK`.

## Testing

```sh
# Unit tests — no network, instant (config + the demux pure function):
cargo test

# Smoke tests — real CVM client (over a mock relay) → outlay → wss://relay.primal.net:
cargo test --features test-utils --test smoke -- --ignored --nocapture
```

Lint/format:

```sh
cargo fmt --all
cargo clippy --all-targets -- -D warnings
```

## Project layout

```text
outlay/
  Cargo.toml        single bin+lib crate (no workspace)
  src/
    lib.rs          library root (so tests/ can import outlay)
    main.rs         thin bin: config → signer → proxy → transport → serve
    config.rs       env config + .env loader
    handler.rs      rmcp OutlayServer + the #[tool] glue
    proxy.rs        upstream RelayPool + forwarding; pure map_notification demux
  tests/
    smoke.rs        end-to-end smoke tests (gated on `test-utils`, #[ignore])
  design/
    design.md       the locked design
  reference/        gitignored, read-only vendored references (cordn-rs, nostr,
                    nostr-rs-relay, rs-sdk, nips) — not required to build
```

## Roadmap

- **`relay_info`** — NIP-11 relay information document (needs an HTTP client).
- **Companion WS shim** — a localhost WebSocket endpoint that translates NIP-01
  for vanilla Nostr clients (gossip, web wallets) into these CVM tool calls, so
  non-CVM clients can reach CVM-exposed relays.
- **Bundled relay** — embed `nostr-rs-relay` in-process as the default upstream;
  proxy code unchanged.
- **Authz** — `allowed_public_keys` (private server by default). Multi-relay
  fan-in. NIP-42 AUTH brokering.

## References

`reference/` holds read-only, gitignored copies of the projects this builds on
and learns from: [`rs-sdk`](https://github.com/ContextVM/rs-sdk) (the CVM Rust
SDK), [`cordn-rs`](https://github.com/Cordn-msg/cordn-rs) (the streaming-CVM
pattern outlay mirrors), [`nostr-rs-relay`](https://github.com/rust-nostr/nostr),
and the [`nostr`](https://github.com/rust-nostr/nostr) library.

## License

MIT.
