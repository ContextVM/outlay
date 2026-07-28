# Packages a *prebuilt* outlay binary. CI builds natively per-arch (amd64/arm64)
# and COPYs the matching binary in — so there's no Rust compile in Docker (fast,
# and the bundled-SQLite C toolchain stays out of the image). To build locally
# for your host arch: `cargo build --release -p outlay`, then copy
# `target/release/outlay` next to this Dockerfile and run `docker build`.

FROM debian:stable-slim

# ca-certificates: the upstream CVM relays (relay.contextvm.org) are wss://.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY outlay /usr/local/bin/outlay

# Persist the bundled relay's SQLite database across container restarts.
# (Zero-config default: the bundled in-process relay runs as the upstream.
# Set OUTLAY_PROXY_RELAY_URL to proxy an external relay instead.)
ENV OUTLAY_BUNDLED_DB_PATH=/data/outlay-relay.db
VOLUME /data

ENTRYPOINT ["outlay"]
