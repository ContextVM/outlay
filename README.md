# outlay

A Nostr relay exposed as a **ContextVM (CVM)** server. `outlay` binds CVM tool
calls to NIP-01 relay traffic: a CVM client calls `subscribe` /
`publish_event` / `relay_info`, and outlay translates each call into the
corresponding NIP-01 exchange, streaming relay events back over
[CEP-41 open-stream](https://github.com/ContextVM/CEPs).

**It just runs.** With no configuration, outlay starts a bundled in-process
Nostr relay as its upstream — a working, persistent (SQLite) relay the moment it
boots. Point it at any other relay instead with a single env var.

> **Status:** v1 — `outlay` (bundled-relay default + external-upstream proxy
> mode), `outlay-shim` (vanilla-NIP-01 bridge), and the release pipeline are
> done and tested. See [`design/design.md`](./design/design.md) for the locked
> design and [`design/shim.md`](./design/shim.md) for the shim.

---

## Run

outlay is self-contained: no config means the bundled relay runs as the
upstream. Pick any install path — all three are zero-config.

**Docker** (no build; the volume persists the relay's SQLite across restarts):
```sh
docker run --rm -v outlay-data:/data ghcr.io/contextvm/outlay
```

**Prebuilt binary** (linux/amd64 or linux/arm64, from
[Releases](https://github.com/ContextVM/outlay/releases)):
```sh
tar xzf outlay-amd64.tar.gz   # or outlay-arm64.tar.gz
./outlay
```

**Build from source** (Rust ≥ 1.88):
```sh
cargo run
```

On startup outlay logs its **server pubkey**, the CVM relays it listens on, the
upstream, and `mode=bundled`. **Copy that pubkey** — it's how clients address
the server.

**Proxy an external relay instead** (advanced) — reach any relay rather than the
bundled one:
```sh
OUTLAY_PROXY_RELAY_URL=wss://relay.primal.net cargo run
# or: docker run --rm -e OUTLAY_PROXY_RELAY_URL=wss://relay.primal.net ghcr.io/contextvm/outlay
```

### Connect a client

outlay has **no inbound port** — it connects *out* to the CVM relays, and
clients reach it there. Two ways in:

- **CVM client** → connect to the CVM relay (`wss://nostr.wtf` by
  default), target the server pubkey outlay printed, and call `subscribe` /
  `publish_event` / `relay_info`.
- **Vanilla Nostr client** (gossip, `nak`, web wallets) → doesn't speak CVM, so
  run the shim and point the client at it:
  ```sh
  cargo run -p outlay-shim   # then connect the client to ws://localhost:8088/<server-pubkey>
  ```

### Try it

With outlay running (`cargo run`), in another terminal publish a note to it
through the shim, then read it back:

```sh
# 1. Start the shim (bridges vanilla NIP-01 → outlay over CVM):
cargo run -p outlay-shim

# 2. Publish a text note via the shim (<server-pubkey> = what outlay printed):
nak event -c "hello from outlay" ws://localhost:8088/<server-pubkey>

# 3. Read it back:
nak req -k 1 -l 1 ws://localhost:8088/<server-pubkey>
```

---

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
  the upstream. By default that's the bundled in-process relay (loopback); with
  `OUTLAY_PROXY_RELAY_URL` set, it's that external relay. Published events are
  forwarded **verbatim** (client-signed), never re-signed.
- **CVM transport** — the `NostrServerTransport` that CVM clients connect
  through, on the ContextVM relays you configure.

## Configuration

Loaded from `.env` then `.env.local` (first-write-wins per key), then the
process environment.

| Variable                      | Default                    | Description                                            |
|-------------------------------|----------------------------|--------------------------------------------------------|
| `OUTLAY_PROXY_RELAY_URL`      | _(unset → bundled)_        | External upstream to proxy. Unset = run the bundled relay (default). |
| `OUTLAY_RELAY_URLS`           | `wss://nostr.wtf`          | Comma-separated CVM relays the server listens on.      |
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
can reach CVM-exposed relays without speaking CVM. Path-keyed:
`ws://localhost:8088/<server-pubkey-or-nprofile>` (hex, npub, or nprofile; an
nprofile's relay hint overrides the configured CVM relays). Design in
[`design/shim.md`](./design/shim.md).

```sh
cargo run -p outlay-shim
```

| Variable                       | Default                    | Description                                            |
|--------------------------------|----------------------------|--------------------------------------------------------|
| `OUTLAY_SHIM_LISTEN_ADDR`      | `127.0.0.1:8088`           | Address to listen on (the Docker image sets `0.0.0.0:8088`). |
| `OUTLAY_SHIM_RELAY_URLS`       | `wss://nostr.wtf`          | Comma-separated CVM relays used to find outlay servers.|
| `OUTLAY_SHIM_PUBLIC_URLS`      | _(unset → `RELAY_URLS`)_   | The shim's own public URL(s). When the colocated relay is on, the bridge dials any matching relay (from a hint or the fallback) over loopback instead of hairpin-dialing its public URL — which times out on most deploys. Defaults to `OUTLAY_SHIM_RELAY_URLS`. |
| `OUTLAY_SHIM_PRIVATE_KEY`      | _(ephemeral)_              | Hex/nsec shim key.                                     |
| `OUTLAY_SHIM_ENCRYPTION_MODE`  | `optional`                 | CVM transport encryption: `disabled` / `optional` / `required`. |

## Testing

```sh
cargo test --workspace                                # unit tests (default = bundled)
cargo test -p outlay --no-default-features            # proxy-only config path
cargo test -p outlay --features test-utils --test smoke_bundled   # network-free E2E (bundled relay)
cargo test --features test-utils --test smoke -- --ignored --nocapture  # real network (primal)
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings
```

## Releases

Binaries (linux/amd64 + linux/arm64) and multi-arch Docker images are published
on every `v*` tag — see [Releases](https://github.com/ContextVM/outlay/releases)
and `ghcr.io/contextvm/outlay` · `ghcr.io/contextvm/outlay-shim`. The `Makefile`
+ GitHub Actions drive it:

```sh
make version    # print the current shared version
make release    # tag the CURRENT version + push (inaugural / re-release)
make patch      # or minor / major → bump, commit, tag v<ver>, push
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
