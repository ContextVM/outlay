# outlay

A Nostr relay exposed as a **ContextVM (CVM)** server. `outlay` binds CVM tool
calls to NIP-01 relay traffic: a CVM client calls `subscribe` /
`publish_event` / `relay_info`, and outlay translates each call into the
corresponding NIP-01 exchange, streaming relay events back over
[CEP-41 open-stream](https://github.com/ContextVM/CEPs).

**It just runs.** With no configuration, outlay starts a bundled in-process
Nostr relay as its upstream — fully self-contained, persistent (SQLite), zero
external dependencies. Point it at any other relay instead with a single env
var; anything that speaks CVM can then read/write that relay through it.

> **Status:** v1 — `outlay` (bundled-relay default + external-upstream proxy
> mode), `outlay-shim` (vanilla-NIP-01 bridge), and the release pipeline are
> done and tested. See [`design/design.md`](./design/design.md) for the locked
> design and [`design/shim.md`](./design/shim.md) for the shim.

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
   CVM client ──── CVM tools over Nostr ──── outlay server ──── NIP-01 ws ──── upstream
                   (CEP-41 open-stream)         (proxy + rmcp)                  (bundled relay
                                                                                or any external relay)
```

Two independent relay connections live inside outlay:

- **Upstream pool** — outlay's own `Proxy`, a `nostr-sdk` `Client` connected to
  the upstream. By default that upstream is the bundled in-process relay
  (loopback). With `OUTLAY_PROXY_RELAY_URL` set, it's that external relay.
  Published events are forwarded **verbatim** (client-signed), never re-signed.
- **CVM transport** — the `NostrServerTransport` that CVM clients connect
  through, on the ContextVM relays you configure.

## Quick start

Requires Rust stable (MSRV **1.88**).

```sh
# Self-contained: zero config → bundled in-process relay (SQLite) as the upstream.
cargo run

# Or proxy an external relay instead (advanced):
OUTLAY_PROXY_RELAY_URL=wss://relay.primal.net cargo run
OUTLAY_PROXY_RELAY_URL=ws://localhost:8080 cargo run
```

On startup outlay logs its server pubkey, the CVM relays it listens on, the
upstream, and `mode=bundled|proxy`.

## Configuration

Loaded from `.env` then `.env.local` (first-write-wins per key), then the
process environment.

| Variable                      | Default                    | Description                                            |
|-------------------------------|----------------------------|--------------------------------------------------------|
| `OUTLAY_PROXY_RELAY_URL`      | _(unset → bundled)_        | External upstream to proxy. Unset = run the bundled relay (default). |
| `OUTLAY_RELAY_URLS`           | `wss://relay.contextvm.org`| Comma-separated CVM relays the server listens on.      |
| `OUTLAY_SERVER_PRIVATE_KEY`   | _(ephemeral)_              | Hex/nsec server key. Unset → new key each start.       |
| `OUTLAY_SERVER_NAME`          | `outlay`                   | CVM profile name.                                      |
| `OUTLAY_ANNOUNCED`            | `false`                    | Public discovery (kind 11316) on/off.                  |
| `OUTLAY_BUNDLED_BACKEND`      | `sqlite`                   | Bundled relay backend: `sqlite` (persistent) or `memory` (volatile). |
| `OUTLAY_BUNDLED_DB_PATH`      | `outlay-relay.db`          | SQLite path (ignored for `memory`).                    |
| `OUTLAY_BUNDLED_PORT`         | `0`                        | Bundled relay bind port (`0` = scan a free loopback port). |

## CVM tool surface

- **`subscribe(subscription_id, filters)`** — streaming. Opens a NIP-01
  subscription upstream and streams `EVENT`/`EOSE`/`CLOSED` chunks. Cancel by
  aborting the call (= NIP-01 `CLOSE`).
- **`publish_event(event)`** — synchronous. Forwards a client-signed event
  verbatim; returns `{ ok, event_id, message }` mirroring the upstream `OK`.
- **`relay_info()`** — synchronous. Fetches the upstream's NIP-11 document
  over HTTP and overlays outlay's identity (`software`/`version`/`proxy`); the
  upstream's identity is preserved under `upstream` and all other fields pass
  through verbatim. Falls back to a synthesized minimum when the upstream
  serves no NIP-11 (notably the bundled relay).

## outlay-shim — vanilla NIP-01 bridge

`outlay-shim` is a localhost WebSocket endpoint that translates vanilla NIP-01
(`REQ`/`EVENT`/`CLOSE`) into outlay's CVM tool calls, so ordinary Nostr clients
(gossip, web wallets, `nak`) can reach CVM-exposed relays without speaking CVM.
Path-keyed: `ws://localhost:8088/<server-pubkey-or-nprofile>`. Design in
[`design/shim.md`](./design/shim.md).

```sh
cargo run -p outlay-shim
```

## Testing

```sh
cargo test --workspace                                # unit tests (default = bundled)
cargo test -p outlay --no-default-features            # proxy-only config path
cargo test -p outlay --features test-utils --test smoke_bundled   # network-free E2E (bundled relay)
cargo test -features test-utils --test smoke -- --ignored --nocapture  # real network (primal)
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings
```

## Releases

Releases are driven by the `Makefile` + GitHub Actions (`.github/workflows/`):

```sh
make version          # print the current shared version
make release          # tag the CURRENT version + push (inaugural / re-release)
make patch            # or minor / major → bump, commit, tag v<ver>, push
```

A pushed `v*` tag triggers `release.yml`, which builds `outlay` + `outlay-shim`
natively for **linux/amd64** and **linux/arm64** (free arm64 runners — no
cross-compile), attaches the tarballs + a SHA256 `checksums.txt` to the GitHub
Release, and publishes multi-arch Docker images for both crates to GHCR:

```
ghcr.io/contextvm/outlay          ghcr.io/contextvm/outlay-shim
```

Run the images directly:

```sh
docker run --rm -v outlay-data:/data ghcr.io/contextvm/outlay          # zero-config bundled relay
docker run --rm -p 8088:8088 ghcr.io/contextvm/outlay-shim             # NIP-01 bridge on :8088
```

## Project layout

```text
outlay/
  Cargo.toml        workspace root (members: crates/*)
  crates/
    outlay/         the CVM↔NIP-01 relay proxy server (bin+lib; bundled relay default)
      src/          config.rs · handler.rs · proxy.rs · main.rs · lib.rs
      tests/        smoke.rs (network, #[ignore]) · smoke_bundled.rs (network-free)
    outlay-shim/    vanilla NIP-01 client bridge (bin+lib; design/shim.md)
      src/          server.rs · conn.rs · translate.rs · nip11.rs · path.rs · transport.rs
    outlay-relay/   bundled in-process relay on nostr-sdk 0.45-alpha's LocalRelay
  design/           design.md (server) · shim.md (shim)
  reference/        gitignored, read-only vendored references (cordn-rs, nostr,
                    nostr-rs-relay, rs-sdk, nips) — not required to build
```

## Roadmap

- **Authz** — `allowed_public_keys`. Deferred until the shim clarifies the trust
  model; outlay is an open proxy meanwhile.
- **Shape B relay** — expose the bundled relay on a configurable bind (not just
  loopback), which forces the authz decision.
- Multi-relay fan-in; NIP-42 AUTH brokering; a NIP-11 cache.

`reference/` holds read-only, gitignored copies of the projects this builds on:
[`rs-sdk`](https://github.com/ContextVM/rs-sdk) (CVM Rust SDK),
[`cordn-rs`](https://github.com/Cordn-msg/cordn-rs) (the streaming-CVM pattern
outlay mirrors), and the [`nostr`](https://github.com/rust-nostr/nostr) library.

## License

MIT.
