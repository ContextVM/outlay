//! outlay — a Nostr relay exposed as a ContextVM (CVM) server.
//!
//! A transparent proxy that binds CVM tool calls to NIP-01 relay traffic:
//! `subscribe`/`publish_event` on the CVM side map to `REQ`/`EVENT` upstream,
//! with relay→client messages streamed back over CEP-41 open-stream.
//!
//! Library entrypoint so integration tests (`tests/`) can construct the proxy
//! and handler directly. The binary (`src/main.rs`) is a thin wrapper.
pub mod config;
pub mod handler;
pub mod proxy;
