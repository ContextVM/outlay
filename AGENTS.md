# AGENTS.md

Instructions for coding agents working on `outlay`. Read this before editing.
The locked design lives in [`design/design.md`](./design/design.md) — it is the
source of truth for architecture and decisions; do not relitigate it without an
explicit change request.

## Project overview

`outlay` is a Nostr relay exposed as a **ContextVM (CVM)** server: a transparent
proxy that binds CVM tool calls to NIP-01 relay traffic. A CVM client calls
`subscribe`/`publish_event`; outlay translates each into the corresponding
NIP-01 exchange with an upstream relay, streaming relay events back over CEP-41
open-stream. Point it at any relay (clearnet or localhost) and anything that
speaks CVM can read/write that relay through it.

**Stack:** Rust 2021, MSRV **1.88**. `contextvm-sdk` 0.2.1 (CVM transport +
`RelayPool`), `rmcp` 1.8 (MCP server handler + `#[tool]` macros), `nostr-sdk`
0.44 (upstream relay client + NIP-01 types), `tokio`, `serde`/`serde_json`,
`schemars`, `async-trait`, `thiserror`, `anyhow`, `tracing`.

**Core invariant (do not break):** one CEP-41 open-stream == one NIP-01
subscription. The stream's lifetime is the subscription's lifetime; cancelling
the call is `CLOSE`.

**Two independent relay connections inside the server (keep them distinct):**
1. The **upstream `Proxy`** — outlay's own `nostr-sdk` `Client` connected to the
   single configured upstream relay (throwaway ephemeral key; events forwarded
   verbatim, never re-signed).
2. The **CVM transport** (`NostrServerTransport`) that CVM clients connect
   through, on the ContextVM relays.

## Setup

```sh
cargo build                       # build the bin + lib
cargo run                         # run the server (needs OUTLAY_PROXY_RELAY_URL)
```

Required env (see `src/config.rs` for the full set and `.env` loader):

- `OUTLAY_PROXY_RELAY_URL` — **required**, the upstream relay to proxy.
- `OUTLAY_RELAY_URLS` — CVM relays to listen on (default `wss://relay.contextvm.org`).
- `OUTLAY_SERVER_PRIVATE_KEY` — hex/nsec; unset → ephemeral.

`.env` and `.env.local` are loaded first-write-wins, then the process env.

## Development workflow

Run the server against a real relay:

```sh
OUTLAY_PROXY_RELAY_URL=wss://relay.primal.net cargo run
OUTLAY_PROXY_RELAY_URL=ws://localhost:8080 cargo run   # local relay
```

Fast feedback loop while editing:

```sh
cargo check                       # fast type-check
cargo fmt --all                   # format
cargo clippy --all-targets -- -D warnings   # lint (warnings are CI failures)
```

The `reference/` directory is **read-only and gitignored** — vendored copies of
`rs-sdk`, `cordn-rs`, `nostr`, `nostr-rs-relay`, and the NIPs. Treat it as a
library of examples to learn from, never as something to edit or as part of the
build (it is not a Cargo workspace member and not required to compile outlay).

## Testing

Two surfaces — know which one you are touching.

**Unit tests** — no network, instant. Config parsing and the pure demux
(`proxy::map_notification`):

```sh
cargo test
```

**Smoke tests** — end-to-end, hit the network. A real CVM client (over a mock
relay — network-free client↔server CVM hop) drives the outlay server, whose
upstream `Proxy` connects to `wss://relay.primal.net` for real:

```sh
cargo test --features test-utils --test smoke -- --ignored --nocapture
```

- Gated behind the `test-utils` feature (pulls `contextvm-sdk`'s `MockRelayPool`).
- Each test is `#[ignore]` (touches the network). Always pass `--ignored`.
- `--nocapture` shows the streamed events; useful when debugging the proxy.
- Run one: `cargo test --features test-utils --test smoke subscribe -- --ignored --nocapture`.
- The upstream URL is the `UPSTREAM` const at the top of `tests/smoke.rs` (not an
  env var) — change it there to point the smoke tests at a different relay.

Default `cargo test` (no `--features test-utils`) skips the smoke binary
entirely. Keep it that way: do not remove the `#[ignore]` or the feature gate.

When you change behavior, add or update a test. Non-trivial pure logic gets a
unit test (see how `map_notification` is tested without any relay); plumbing
gets a smoke test.

## Code style

- `cargo fmt --all` and `cargo clippy --all-targets -- -D warnings` must be clean
  before commit. The `clippy::redundant_closure` and `cloned_ref_to_slice_refs`
  lints have already bitten us — prefer the clippy-suggested form.
- Single bin+lib crate (no workspace). `src/lib.rs` re-exports the modules so
  `tests/` can import them; `src/main.rs` is a thin bin over `outlay::`.
- New rmcp tools: derive params with `serde::Deserialize` + `schemars::JsonSchema`.
  `nostr` types (`Filter`, `Event`) do **not** impl `JsonSchema` — accept them as
  raw `serde_json::Value` in tool params and parse inside the handler (see
  `handler.rs`). The rmcp-side chunk framing is hand-built with `serde_json::json!`.
- Mirror `cordn-rs` patterns where applicable: the `MessageSink` trait + the
  `select! { recv / sleep(is_active poll) }` loop in `proxy.rs` are ported from
  `cordn-server/src/adapter.rs`.

## Locked design decisions (do not change without a change request)

These are settled in `design/design.md`. Highlights that are easy to get wrong:

1. **Use `RelayPoolNotification::Message`, not `::Event`.** The `Event` variant
   dedupes at pool level and excludes events sent by our own client — both break
   transparent proxying. `Message` fires per-subscription, carries the
   `subscription_id`, and includes every event (NIP-01 semantics).
2. **Per-call random upstream `SubscriptionId`, mapped back to the client's bare
   sub_id in chunks.** The pool multiplexes one upstream socket; two CVM clients
   that both pick `"sub1"` would otherwise cross-receive. (The design doc's
   original `<uuid>::<sub>` was abandoned — it can exceed NIP-01's 64-char limit.
   `SubscriptionId::generate()` is length-safe.)
3. **Forward events verbatim, never re-sign.** `publish_event` uses
   `client.send_event(&event)` (pre-built), not `send_event_builder`. Re-signing
   turns this into a publisher, not a proxy.
4. **Multi-filter REQs go through `client.pool().subscribe_with_id(id, Vec<Filter>, ..)`**
   — the SDK's `Client` wrapper only takes a single `Filter` and would replace
   the sub on each call. Reach the pool via `client.pool()`.
5. **Register the notifications receiver BEFORE `subscribe_with_id`** so the
   first EVENT/EOSE cannot be missed in the race between subscribe and recv.
6. **Always unsubscribe on exit** (client abort, upstream CLOSED, or pool death)
   — the `subscribe` loop's cleanup is unconditional.

## Build and release

```sh
cargo build --release            # release bin at target/release/outlay
```

Release profile: LTO, single codegen unit, stripped (see `Cargo.toml`). No CI is
wired yet; no automated release pipeline.

## Gotchas

- **The upstream `Proxy` cannot be mocked.** It uses the concrete
  `nostr-sdk` `Client` (for `pool().subscribe_with_id` and `notifications()`),
  which `RelayPoolTrait`/`MockRelayPool` do not expose. That is why the upstream
  hop is exercised by the network smoke tests against a real relay, not a unit
  test. The *demux* is unit-tested by extracting it as a pure function.
- **`publish_event` loses the upstream's exact OK message prefix** (`duplicate:`,
  `blocked:`, …). `send_event` aggregates per-relay OKs into success/failed sets
  and surfaces the relay error text, not the raw frame. Good enough for v1; noted
  in `proxy.rs` with a `ponytail:` comment and an upgrade path
  (`send_msg` + await `Ok` by event_id).
- **Authz is open by default.** `allowed_public_keys` is not wired yet (design
  calls for private-by-default). Do not deploy openly without addressing this.
- **Single upstream only (v1).** Multi-relay fan-in makes EOSE semantics
  ambiguous; deferred.

## PR guidelines

- Title format: `area: brief description` (e.g. `proxy: forward CLOSED as a stop chunk`).
- Required before commit: `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings`,
  `cargo test`.
- If you change proxy/forwarding behavior, run the smoke tests too:
  `cargo test --features test-utils --test smoke -- --ignored --nocapture`.
- Update `design/design.md` when a locked decision actually changes (not for
  implementation detail) — and call it out in the PR description.
