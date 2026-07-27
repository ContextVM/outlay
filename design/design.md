# outlay — Design

A Nostr relay exposed as a ContextVM (CVM) server. `outlay` sits between a
CVM client and an upstream Nostr relay, translating CVM tool calls into NIP-01
relay traffic and streaming relay events back over CEP-41 open-stream. The
upstream relay is pluggable: any clearnet or localhost relay. A bundled
in-process relay is a later milestone that reuses this proxy unchanged.

## 1. Framing

A CVM server is **not** a NIP-01 WebSocket relay. It is an `rmcp` server
handler run over `NostrServerTransport`; its client-facing surface is **MCP
tools**, not `["REQ", ...]` frames. "Following NIP-01" therefore means:

- the **tool surface and the streamed payload format mirror NIP-01 message
  shapes**;
- CEP-41 open-stream carries the relay→client direction;
- each open-stream chunk is **one verbatim NIP-01 relay→client JSON array**
  (`["EVENT", sub, e]`, `["EOSE", sub]`, `["CLOSED", sub, msg]`).

The wire-level contract is maximally faithful; the transport is CVM instead of
raw WebSocket.

## 2. Core insight: NIP-01 subscription lifecycle ≅ one CEP-41 stream

CEP-41 open-stream is **server→client only**. One `tools/call` takes one set
of params, optionally emits an unbounded stream of chunks, then a final
`CallToolResult`. There is no channel for a client to push additional frames
into a running call; the only client→server signal after start is `abort()`.

This maps 1:1 onto NIP-01 subscriptions:

| NIP-01 (relay)            | CVM (outlay)                                                   |
|---------------------------|----------------------------------------------------------------|
| `["REQ", sub, filters]`   | `tools/call` `subscribe{subscription_id, filters}` + `progressToken` |
| `["EVENT", sub, e]`       | open-stream chunk `["EVENT","sub",{event}]`                    |
| `["EOSE", sub]`           | open-stream chunk `["EOSE","sub"]`                             |
| `["CLOSED", sub, msg]`    | open-stream chunk `["CLOSED","sub","msg"]`                     |
| `["CLOSE", sub]`          | client aborts the stream (`call.abort()`)                      |
| `["EVENT", e]` (publish)  | `tools/call` `publish_event{event}` → final result = `OK`      |

**One tool call is one subscription.** The stream's lifetime *is* the
subscription's lifetime; cancelling the call *is* `CLOSE`. No separate
`close` tool; no cross-call sub-id registry required.

## 3. Tool surface (v1)

Three specialized tools. No single-endpoint `postMessage` dispatcher — see
§4 for the rationale.

```
subscribe(subscription_id: String, filters: Vec<Filter>)
    -> open-stream of ["EVENT",sub,e] / ["EOSE",sub] / ["CLOSED",sub,msg]
    final: { "ok": true }     // final result kept tiny: deferred path is not CEP-22-fragmented

publish_event(event: Event)
    -> { "ok": bool, "event_id": .., "message": ".." }   // mirrors ["OK", id, bool, msg]

relay_info()
    -> NIP-11 document of the upstream (see §5)
```

Declared contracts: `subscribe` is streaming; `publish_event` and
`relay_info` are synchronous. The caller knows from the schema whether a call
will stream.

`close` is deliberately omitted — aborting the `subscribe` call closes the
upstream subscription.

## 4. Why not a single `postMessage` dispatcher

Considered and rejected for v1. A `postMessage(verb, payload)` tool mirroring
the WS endpoint cannot be a real persistent endpoint, because CEP-41 has no
client→server stream. It collapses to "one tool name, one call per frame,"
which fails three ways:

1. **`CLOSE` is a lie.** A `CLOSE` call is a different `tools/call` than the
   `REQ` it targets, and one call cannot wind down another call's CEP-41
   stream. Real close is `abort()` on the REQ call. A faithful-looking
   `postMessage(["CLOSE", sub])` would silently do nothing, or force a
   `(peer, sub_id) → cancellation-token` registry — reintroducing the
   cross-call coordination the "call == sub" model avoids.
2. **Polymorphic streaming.** A caller cannot tell from the schema whether a
   given `postMessage` invocation will stream (REQ) or return synchronously
   (EVENT). Specialized tools declare this honestly.
3. **No benefit to the future client shim.** The companion WS shim (vanilla
   Nostr client → CVM, a later milestone) cannot be a dumb frame-forwarder
   either way: `CLOSE` must become `abort()`, so the shim is sub-id-aware
   under both designs.

The dispatcher's only genuine edge is a cleaner growth path for future NIPs
(COUNT NIP-45, AUTH NIP-42, NEG NIP-77). That is real but cheaply reachable
**later**, because the internal relay-pool layer is verb-agnostic: collapsing
to a dispatcher then is a same-file edge rewrite, not an architecture change.
YAGNI — don't pay for verbs that don't exist yet.

When a second streaming verb or COUNT lands, collapse the *synchronous* verbs
into a dispatcher if the count justifies it; leave `subscribe` streaming on
its own.

## 5. `relay_info` (NIP-11)

NIP-11 is a separate HTTP `GET` with `Accept: application/nostr+json` — not a
WS frame, so it is its own tool regardless of the §4 decision.

Return policy (v1 = **proxy upstream verbatim** with minimal overlay):

- HTTP GET the upstream's base URL, parse the NIP-11 document.
- Overlay our `software` (`outlay`) and `version`, and add a `proxy` marker.
- Graceful fallback when the upstream has no/minimal NIP-11 (notably the
  future bundled in-process relay on `ws://127.0.0.1:<port>`): return a
  synthesized minimum instead.

Known field mismatch to revisit: upstream `limitation.max_subscriptions` is
per-WS-connection; the CVM reality is per-CEP-41-stream
(`max_concurrent_streams`). v1 forwards upstream's value; grow toward a merged
`limitation` rewrite when a real client is misled.

## 6. Upstream transport: reuse the SDK `RelayPool`

The `rs-sdk` already ships `RelayPool`, wrapping `nostr-sdk`'s `Client`:
`connect`, `subscribe`, `publish_event`, `fetch_events`, `notifications()`
(a `broadcast::Receiver<RelayPoolNotification>`), behind a mockable
`RelayPoolTrait`. It is already a transitive dependency. **Use it for v1.**
Do not pull a separate WebSocket client unless a transparency wall forces it
(see §8, gotcha #1).

**v1 scope: exactly one upstream URL.** Multi-relay fan-in makes EOSE
semantics ambiguous (when to EOSE across relays). Single upstream keeps the
proxy genuinely transparent. Multi-relay is a later mode.

## 7. Architecture and crate layout

Mirrors the `cordn-rs` streaming-server pattern (the closest analog), but
**starts as a single binary crate**. Our core logic (forwarding glue) is
thin; do not pre-split a `core` lib. Split out a network-free `core` crate
only if a genuinely testable non-network surface emerges (e.g. the sub-id
namespacing + filter mapping logic).

```
outlay/
  src/
    main.rs        // signer, RelayPool wiring, NostrServerTransport, banner
    handler.rs     // rmcp #[tool] glue (subscribe / publish_event / relay_info)
                   //   + MessageSink trait + StreamWriter(OpenStreamWriter)
    proxy.rs       // upstream forwarding loops, notification demux, sub-id namespace
  design/
    design.md      // this document
```

Reused near-verbatim from `cordn-rs`:

- `MessageSink` trait + `StreamWriter(OpenStreamWriter)` adapter
  (`cordn-server/src/methods.rs`).
- The `select! { recv / sleep(is_active poll) }` loop with `SINK_ACTIVE_POLL`
  (`cordn-server/src/adapter.rs`) — backpressure and client-disconnect
  handling.

## 8. Gotchas

Ranked by severity.

1. **Subscription-ID must be preserved upstream.** NIP-01 `subscription_id`
   is per-connection and client-chosen. The SDK wrapper
   `RelayPool::subscribe` calls `client.subscribe(filter, None)`, which
   **auto-generates** the id and discards it — useless for transparent
   proxying. Drive the underlying `Client` directly via `relay.client()`
   with the client's sub_id. **Verify the exact `subscribe_with_id` (or
   equivalent) API on `nostr-sdk` 0.44** before committing; this is the
   riskiest unknown and the target of the first implementation spike.

2. **EOSE / OK / CLOSED arrive as `RelayPoolNotification::Message`.** The
   notification stream yields `Event { subscription_id, event }` for events
   and `Message { relay_url, message: RelayMessage }` for `EOSE`/`Ok`/
   `Closed`/`Notice`. The forwarding loop must handle both variants: route
   `Event` by sub_id; parse `Message` (carries sub_id for EOSE/CLOSED,
   event_id for OK).

3. **Namespace upstream sub-ids to avoid collisions.** The pool multiplexes
   one upstream socket and tags subs by id; two CVM clients that both pick
   `"sub1"` would cross-receive events. Fix: transform the upstream id as
   `<call-uuid>::<client-sub>` and strip the prefix on the way back. The
   pool sees unique ids; the client still sees its bare id. One-line
   transform; removes the whole collision class. (This is the one place a
   pure one-socket-per-sub WS client would be more obviously correct; the
   namespace transform gets us the same correctness with the pool.)

4. **Transparency = forward verbatim, never re-sign.** `publish_event`
   forwards the client's already-signed `Event` via `send_event(event)` (not
   `send_event_builder`), so the client's pubkey stays the author. Re-signing
   would make this a *publisher*, not a proxy.

5. **Per-peer isolation.** Because each `subscribe` call owns its own
   upstream sub, isolation is automatic as long as the client's sub_id is
   used only within that call (plus the §8.3 namespace prefix). No global
   `(peer, sub_id)` map needed. The NIP-01 "REQ replaces REQ on the same
   sub_id" edge is deferred until a real client needs it.

6. **`publish_event` when upstream never returns `OK`.** Forward, await the
   matching `Ok` notification by `event_id` with a timeout. On timeout,
   return `{ ok: false, message: "error: upstream did not acknowledge" }`.
   Do not hang the call indefinitely.

7. **NIP-42 AUTH — defer, decide the failure mode.** If the upstream
   requires AUTH, v1 cannot fulfill it transparently (no client signer).
   Surface the `AUTH` challenge as a `CLOSED`/`NOTICE` chunk and let the
   client fail. Do not broker AUTH in v1.

8. **Authorization / open-proxy.** Without `allowed_public_keys`, anyone who
   discovers the server relays arbitrary events through it to the upstream
   (and the upstream sees our IP). Default to `allowed_public_keys`
   (private server); open mode is an explicit opt-in.

9. **Chunk sizing.** Open-stream caps: 512 KiB / 64 chunks buffered per
   stream. One chunk per NIP-01 message is safe (a single Nostr event is
   well under the ~64 KiB relay-event ceiling). The final `CallToolResult`
   rides the non-CEP-22-fragmented deferred path — keep it to a short
   `{ok:true}`; never put bulk data in the final result.

## 9. Roadmap

- **v1 (this design):** proxy of a single upstream relay. `subscribe` +
  `publish_event` + `relay_info`. Private by default. Namespaced sub-ids.
- **Next:** companion WS shim ("proxy for the proxies") so vanilla Nostr
  clients (gossip, nostr, web wallets) can reach CVM-exposed relays via a
  localhost WS endpoint that translates to these tool calls.
- **Later:** bundled in-process `nostr-rs-relay`, pointed at
  `ws://127.0.0.1:<port>` as the upstream. Proxy code unchanged.
- **Later:** multi-relay fan-in; synchronous-verb dispatcher if COUNT/AUTH
  land; NIP-42 AUTH brokering.

## 10. Locked decisions

1. Tool surface: `subscribe` (streaming) + `publish_event` (sync) +
   `relay_info` (sync). No `postMessage` dispatcher in v1.
2. Upstream transport: the SDK `RelayPool`. Pure-WS only if forced.
3. v1: single upstream URL.
4. Authorization: private by default (`allowed_public_keys`); open is opt-in.
5. Upstream sub-ids namespaced as `<call-uuid>::<client-sub>`.
6. Crate layout: single binary crate to start; split a `core` lib only if a
   testable non-network surface emerges.
7. `relay_info`: proxy upstream NIP-11 verbatim, overlay our `software`/
   `version` + `proxy` marker; synthesize a minimum when upstream has none.
8. Transparency: forward client-signed events verbatim; never re-sign.
