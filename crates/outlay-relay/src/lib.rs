//! Bundled in-process Nostr relay for outlay (design "Shape A": **internal
//! upstream only** — loopback, never exposed to clearnet).
//!
//! ## Why a separate crate (the version-skew isolation)
//!
//! `LocalRelay` — a real NIP-01 WebSocket server — shipped in `nostr-sdk`
//! **0.45-alpha**, behind its `local-relay` feature. But `outlay` is pinned to
//! `nostr-sdk` **0.44** transitively via `contextvm-sdk` (`contextvm_sdk::signer`
//! re-exports `nostr_sdk::prelude::{Keys, …}`), and a caret on 0.x forbids 0.45.
//!
//! A single crate cannot import two `nostr-sdk` versions, so the bundled relay
//! lives here on 0.45-alpha and talks to `outlay` only through a **plain `String`
//! URL** (`BundledRelay::url()`). No `nostr` types cross the boundary, so the two
//! `nostr-sdk` versions compile side by side in the final binary without type
//! conflict. The default `outlay` build (no `bundled-relay` feature) pulls
//! neither this crate nor 0.45-alpha — the alpha is fully opt-in.
//!
//! ## Why this still satisfies "proxy code unchanged"
//!
//! `LocalRelay` is a genuine WS server: `outlay`'s proxy connects to
//! [`BundledRelay::url`] exactly as it would to any remote upstream.

use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use nostr_database::NostrDatabase;
use nostr_sdk::local_relay::LocalRelay;
use nostr_sqlite::store::NostrSqlite;

/// Storage backend for the bundled relay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    /// Volatile, in-process. Lost on shutdown. Good for tests / ephemeral runs.
    Memory,
    /// Persistent single-file SQLite. `nostr-sqlite`'s default `bundled` feature
    /// statically links SQLite — no system `libsqlite3` required.
    Sqlite,
}

/// A running bundled relay bound to loopback. Not auto-shutdown on `Drop`; call
/// [`BundledRelay::shutdown`] for a clean stop, or let process exit tear it down.
pub struct BundledRelay {
    url: String,
    relay: LocalRelay,
}

impl BundledRelay {
    /// Build and start the relay bound to `127.0.0.1:<port>` (`0` = OS-assigned
    /// ephemeral port — recommended, avoids conflicts). Returns the
    /// `ws://127.0.0.1:<port>` URL for the proxy to use as its upstream.
    ///
    /// `db_path` is only consulted for [`Backend::Sqlite`].
    pub async fn spawn(backend: Backend, db_path: Option<&Path>, port: u16) -> Result<Self> {
        // Build the database once; held as a trait object so the port-scan below
        // can clone it into each bind attempt without rebuilding it.
        let db: Arc<dyn NostrDatabase> = match backend {
            Backend::Memory => Arc::new(nostr_memory::MemoryDatabase::unbounded()),
            Backend::Sqlite => {
                let path = db_path.ok_or_else(|| anyhow!("sqlite backend requires a db path"))?;
                ensure_parent(path).await?;
                Arc::new(
                    NostrSqlite::builder()
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

    /// The `ws://127.0.0.1:<port>` URL the proxy connects to as its upstream.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Stop the relay. Idempotent.
    pub fn shutdown(&self) {
        self.relay.shutdown();
    }
}

/// Bind the relay, scanning a small loopback range when `port == 0`.
///
/// `LocalRelay` can't report an OS-assigned ephemeral port — its `url()` echoes
/// the *configured* address verbatim (a `OnceCell` set at build time, never
/// updated from `TcpListener::local_addr()`), so `port(0)` would advertise
/// `ws://127.0.0.1:0` and the proxy could never connect. We work around it by
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
        // The sqlite backend should have created the db file.
        assert!(
            path.exists(),
            "sqlite db file not created at {}",
            path.display()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
