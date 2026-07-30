//! Bundled in-process Nostr relays for outlay, built on `nostr-sdk`'s
//! `LocalRelay` (0.45-alpha).
//!
//! ## Why a separate crate (the version-skew isolation)
//!
//! `LocalRelay` — a real NIP-01 WebSocket server — shipped in `nostr-sdk`
//! **0.45-alpha**, behind its `local-relay` feature. But `outlay` and
//! `outlay-shim` are pinned to `nostr-sdk` **0.44** transitively via
//! `contextvm-sdk`, and a caret on 0.x forbids 0.45.
//!
//! A single crate cannot import two `nostr-sdk` versions, so the relay lives
//! here on 0.45-alpha and talks to the rest of the workspace only through a
//! **plain `String` URL** (`BundledRelay::url()`). No `nostr` types cross the
//! boundary, so the two `nostr-sdk` versions compile side by side in the final
//! binary without type conflict.
//!
//! ## Two shapes (one `LocalRelay`, pluggable storage)
//!
//! `LocalRelay` keeps live fan-out and storage separate: a tokio `broadcast`
//! channel delivers new events to current subscribers, and the `NostrDatabase`
//! only backs stored `REQ` queries. So a database that saves nothing but reports
//! success yields a memoryless relay — `save_event` returns `Success` (so the
//! broadcast still fires) and `query` returns empty (so every `REQ` `EOSE`s at
//! once, then streams live events only). That is [`MemorylessDatabase`], always
//! compiled.
//!
//! - **`MemorylessDatabase`** (outlay-shim's `/` relay): an ephemeral,
//!   storage-less relay the shim exposes so outlays can collapse their CVM
//!   transport relay into the shim and drop one network hop. Correct for NIP-01
//!   ephemeral events (kinds 20000–29999, e.g. CVM's kind-21059 gift wraps),
//!   which relays must broadcast live and must not persist. Serves any kind
//!   live; it just never backfills history.
//! - **Bundled upstream relay** (outlay's "Shape A"): a persistent/volatile
//!   in-process relay outlay proxies as its upstream, making it self-contained.
//!   Backends: SQLite (default, statically bundled) or memory. Behind the
//!   `bundled-backends` feature (on for outlay, off for the shim).
//!
//! See `design/design.md` §9–10 and `design/shim.md`.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use nostr_database::error::Error;
use nostr_database::{DatabaseEventStatus, Events, Features, NostrDatabase, SaveEventStatus};
use nostr_sdk::local_relay::LocalRelay;
use nostr_sdk::prelude::{Event, EventId, Filter};
#[cfg(feature = "bundled-backends")]
use std::path::Path;

/// A `NostrDatabase` that retains nothing. `save_event` reports [`SaveEventStatus::Success`]
/// without storing, so `LocalRelay` still broadcasts the event to its live
/// subscribers; every read path returns empty, so a `REQ` receives an immediate
/// `EOSE` and then streams only events published while it is open.
///
/// Nothing is ever persisted — the correct behavior for a relay carrying NIP-01
/// ephemeral events (kinds 20000–29999, e.g. CVM's kind-21059 gift wraps), which
/// relays must broadcast to current subscribers and must not store. Non-ephemeral
/// kinds are likewise served live; they simply are never backfilled.
#[derive(Debug, Default)]
pub struct MemorylessDatabase;

impl NostrDatabase for MemorylessDatabase {
    fn backend(&self) -> nostr_database::Backend {
        nostr_database::Backend::custom("memoryless")
    }

    fn features(&self) -> Features {
        Features {
            persistent: false,
            event_expiration: false,
            full_text_search: false,
            request_to_vanish: false,
        }
    }

    fn save_event<'a>(
        &'a self,
        _event: &'a Event,
    ) -> Pin<Box<dyn Future<Output = Result<SaveEventStatus, Error>> + Send + 'a>> {
        Box::pin(async move { Ok(SaveEventStatus::Success) })
    }

    fn check_id<'a>(
        &'a self,
        _event_id: &'a EventId,
    ) -> Pin<Box<dyn Future<Output = Result<DatabaseEventStatus, Error>> + Send + 'a>> {
        Box::pin(async move { Ok(DatabaseEventStatus::NotExistent) })
    }

    fn event_by_id<'a>(
        &'a self,
        _event_id: &'a EventId,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Event>, Error>> + Send + 'a>> {
        Box::pin(async move { Ok(None) })
    }

    fn count(
        &self,
        _filter: Filter,
    ) -> Pin<Box<dyn Future<Output = Result<usize, Error>> + Send + '_>> {
        Box::pin(async move { Ok(0) })
    }

    fn query(
        &self,
        _filter: Filter,
    ) -> Pin<Box<dyn Future<Output = Result<Events, Error>> + Send + '_>> {
        Box::pin(async move { Ok(Events::default()) })
    }

    fn delete(
        &self,
        _filter: Filter,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + '_>> {
        Box::pin(async move { Ok(()) })
    }

    fn wipe(&self) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + '_>> {
        Box::pin(async move { Ok(()) })
    }
}

/// A running relay bound to loopback. Not auto-shutdown on `Drop`; call
/// [`BundledRelay::shutdown`] for a clean stop, or let process exit tear it down.
pub struct BundledRelay {
    url: String,
    relay: LocalRelay,
}

impl BundledRelay {
    /// Start a memoryless (ephemeral, storage-less) relay on `127.0.0.1:<port>`
    /// (`0` = OS-assigned ephemeral port — recommended). Live events broadcast
    /// to current subscribers; nothing is stored. Returns the
    /// `ws://127.0.0.1:<port>` URL.
    pub async fn spawn_memoryless(port: u16) -> Result<Self> {
        let (relay, url) = bind_with_port_scan(Arc::new(MemorylessDatabase), port).await?;
        tracing::info!(%url, "memoryless relay started");
        Ok(Self { url, relay })
    }

    /// The `ws://127.0.0.1:<port>` URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Stop the relay. Idempotent.
    pub fn shutdown(&self) {
        self.relay.shutdown();
    }
}

// ── Persistent/volatile backends (outlay's bundled upstream) ─────────────────

/// Storage backend for the bundled upstream relay. Only compiled with the
/// `bundled-backends` feature (on for outlay; off for the shim, which only uses
/// the memoryless backend).
#[cfg(feature = "bundled-backends")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    /// Volatile, in-process. Lost on shutdown. Good for tests / ephemeral runs.
    Memory,
    /// Persistent single-file SQLite. `nostr-sqlite`'s default `bundled` feature
    /// statically links SQLite — no system `libsqlite3` required.
    Sqlite,
}

#[cfg(feature = "bundled-backends")]
impl BundledRelay {
    /// Build and start the bundled upstream relay bound to `127.0.0.1:<port>`
    /// (`0` = OS-assigned ephemeral port — recommended, avoids conflicts).
    /// Returns the `ws://127.0.0.1:<port>` URL for outlay's proxy to use as its
    /// upstream.
    ///
    /// `db_path` is only consulted for [`Backend::Sqlite`].
    pub async fn spawn(backend: Backend, db_path: Option<&Path>, port: u16) -> Result<Self> {
        let db: Arc<dyn NostrDatabase> = match backend {
            Backend::Memory => Arc::new(nostr_memory::MemoryDatabase::unbounded()),
            Backend::Sqlite => {
                let path = db_path.ok_or_else(|| anyhow!("sqlite backend requires a db path"))?;
                ensure_parent(path).await?;
                Arc::new(
                    nostr_sqlite::store::NostrSqlite::builder()
                        .in_file(path)
                        .build()
                        .await
                        .context("opening sqlite database")?,
                )
            }
        };

        let (relay, url) = bind_with_port_scan(db, port).await?;
        tracing::info!(%url, backend = ?backend, "bundled relay started");
        Ok(Self { url, relay })
    }
}

/// Bind the relay, scanning a small loopback range when `port == 0`.
///
/// `LocalRelay` can't report an OS-assigned ephemeral port — its `url()` echoes
/// the *configured* address verbatim (a `OnceCell` set at build time, never
/// updated from `TcpListener::local_addr()`), so `port(0)` would advertise
/// `ws://127.0.0.1:0` and the client could never connect. We work around it by
/// trying concrete ports until one binds; `url()` then reports the real port.
async fn bind_with_port_scan(
    db: Arc<dyn NostrDatabase>,
    port: u16,
) -> Result<(LocalRelay, String)> {
    const EPHEMERAL_BASE: u16 = 8086;
    const EPHEMERAL_TRIES: u16 = 50;

    // `port == 0` → scan; a fixed port is tried exactly once (hard error if busy).
    let (start, tries): (u16, u16) = if port == 0 {
        (EPHEMERAL_BASE, EPHEMERAL_TRIES)
    } else {
        (port, 1)
    };

    let mut last_err: Option<String> = None;
    for offset in 0..tries {
        let candidate = start.checked_add(offset).context("port range overflow")?;
        let relay = LocalRelay::builder()
            .port(candidate)
            .database(Arc::clone(&db))
            .build();
        match relay.run().await {
            Ok(_) => {
                let url = relay.url().await.to_string();
                return Ok((relay, url));
            }
            Err(e) => {
                if port != 0 {
                    anyhow::bail!("bundled relay failed to bind 127.0.0.1:{candidate}: {e}");
                }
                tracing::debug!(port = candidate, error = %e, "port busy, trying next");
                last_err = Some(e.to_string());
            }
        }
    }

    let tried = format!("{start}..={}", start + tries - 1);
    Err(anyhow!(
        "no free loopback port for bundled relay in {tried} (last error: {})",
        last_err.unwrap_or_default()
    ))
}

/// Ensure the parent directory of a SQLite db path exists. Only used by the
/// sqlite backend.
#[cfg(feature = "bundled-backends")]
async fn ensure_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("creating database directory {}", parent.display()))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The memoryless relay broadcasts live events to subscribers but stores
    /// nothing: a live subscriber receives a just-published event, and a later
    /// `REQ` (which would backfill on a storing relay) gets nothing.
    #[tokio::test]
    async fn memoryless_relay_broadcasts_live_and_stores_nothing() {
        use std::time::Duration;

        use nostr_sdk::prelude::*;

        let relay = BundledRelay::spawn_memoryless(0).await.unwrap();
        let url = relay.url().to_owned();

        // Subscriber: subscribe BEFORE publishing (memoryless = live-only).
        let sub = Client::default();
        sub.add_relay(&url).and_connect().await.unwrap();
        let id = SubscriptionId::new("ml");
        let mut notifications = sub.notifications();
        sub.subscribe(Filter::new().kind(Kind::TextNote))
            .with_id(id.clone())
            .await
            .unwrap();
        // Let the REQ land at the relay before publishing.
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Publisher: a separate client so the subscriber's Event notification
        // fires (a client doesn't get Event notifications for its own sends).
        let pubc = Client::default();
        pubc.add_relay(&url).and_connect().await.unwrap();
        let keys = Keys::generate();
        let event = EventBuilder::new(Kind::TextNote, "hello memoryless")
            .finalize(&keys)
            .unwrap();
        pubc.send_event(&event).await.unwrap();

        let received = tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(notification) = notifications.next().await {
                if let ClientNotification::Event {
                    subscription_id,
                    event,
                    ..
                } = notification
                {
                    if subscription_id == id {
                        return event;
                    }
                }
            }
            panic!("notifications ended before event was received");
        })
        .await
        .expect("live event delivered within timeout");
        assert_eq!(received.id, event.id, "subscriber got the published event");

        // Nothing retained: a fresh client querying the same filter after the
        // fact gets an immediate EOSE with zero events.
        let qry = Client::default();
        qry.add_relay(&url).and_connect().await.unwrap();
        let stored = qry.fetch_events(Filter::new().kind(Kind::TextNote)).await;
        let stored = stored.unwrap();
        assert!(
            stored.is_empty(),
            "memoryless relay must not retain events (got {})",
            stored.len()
        );

        relay.shutdown();
    }

    #[cfg(feature = "bundled-backends")]
    #[tokio::test]
    async fn memory_relay_binds_real_loopback_port() {
        let r = BundledRelay::spawn(Backend::Memory, None, 0).await.unwrap();
        let url = r.url();
        assert!(url.starts_with("ws://127.0.0.1:"), "got {url}");
        let port: u16 = url.rsplit(':').next().unwrap().parse().unwrap();
        assert!(
            port > 0,
            "ephemeral port must resolve to a real port, got {url}"
        );
        r.shutdown();
    }

    #[cfg(feature = "bundled-backends")]
    #[tokio::test]
    async fn sqlite_relay_opens_persistent_backend() {
        let dir = std::env::temp_dir().join(format!("outlay-relay-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.join("relay.db");
        let r = BundledRelay::spawn(Backend::Sqlite, Some(&path), 0)
            .await
            .unwrap();
        let port: u16 = r.url().rsplit(':').next().unwrap().parse().unwrap();
        assert!(port > 0, "got {}", r.url());
        r.shutdown();
        assert!(
            path.exists(),
            "sqlite db file not created at {}",
            path.display()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
