# SPDX-License-Identifier: Apache-2.0
# Multi-stage Docker build for NeuralBase.
#
# Stage 1 (builder): compiles the release binary with full C++ toolchain
#                    (required by rocksdb crate).
# Stage 2 (runtime): slim Debian image with only the runtime deps.
#
# Build:  docker build -t neuralbase:latest .
# Run:    docker run -e NODE_ID=node1 -e RAFT_ADDR=0.0.0.0:7001 \
#                    -e LISTEN_ADDR=0.0.0.0:5432 \
#                    -p 5432:5432 -p 7001:7001 neuralbase:latest

# ── Stage 1: builder ──────────────────────────────────────────────────────

FROM rust:1.85-slim-bookworm AS builder

# Install system deps required by rocksdb-sys (LLVM + clang are recommended).
RUN apt-get update && apt-get install -y --no-install-recommends \
        clang \
        libclang-dev \
        cmake \
        make \
        g++ \
        pkg-config \
        libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy manifests first for layer caching of dependency compilation.
COPY Cargo.toml Cargo.lock ./

# Create a stub main.rs so cargo can compile all dependencies before
# we copy the real source tree.
RUN mkdir -p src && echo 'fn main() {}' > src/main.rs
RUN cargo build --release --locked 2>/dev/null || true
RUN rm -rf src

# Now copy real source and build.
COPY src ./src
# Touch main.rs so cargo rebuilds the final binary.
RUN touch src/main.rs && cargo build --release --locked

# ── Stage 2: runtime ──────────────────────────────────────────────────────

FROM debian:bookworm-slim AS runtime

# Runtime libraries needed by rocksdb.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libgcc-s1 \
        libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/neuralbase /app/neuralbase

# Default environment — overridden by docker-compose or Kubernetes.
ENV LISTEN_ADDR="0.0.0.0:5432" \
    RAFT_ADDR="0.0.0.0:7001" \
    NODE_ID="node1" \
    PEERS="" \
    METRICS_PORT="9090"

# SQL wire protocol (PostgreSQL-compatible).
EXPOSE 5432
# Raft RPC port.
EXPOSE 7001
# Query exchange / fragment shuffling.
EXPOSE 8001
# Prometheus metrics scrape endpoint.
EXPOSE 9090

# HEALTHCHECK: verify the SQL listener is accepting TCP connections.
# The check writes a zero-length probe to port 5432 and expects the
# connection to succeed within 2 seconds.
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
    CMD timeout 2 bash -c 'echo > /dev/tcp/127.0.0.1/5432' || exit 1

ENTRYPOINT ["/app/neuralbase"]
