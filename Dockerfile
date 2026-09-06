# SPDX-License-Identifier: Apache-2.0
# Multi-stage Docker build for NeuralBase.
#
# The release image includes the `tls` Cargo feature, but remains plaintext
# unless TLS environment variables are explicitly configured at runtime.

FROM rust:1.88-slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        clang \
        libclang-dev \
        cmake \
        make \
        g++ \
        pkg-config \
        libssl-dev \
        nasm \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml Cargo.lock ./

# Prime dependency compilation without allowing a failed warm-up to mask the
# real build below.
RUN mkdir -p src && echo 'fn main() {}' > src/main.rs \
    && cargo build --release --locked --features tls \
    && rm -rf src

COPY src ./src
RUN touch src/main.rs \
    && cargo build --release --locked --features tls

FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        libgcc-s1 \
        libstdc++6 \
        coreutils \
        bash \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/neuralbase /app/neuralbase

ENV NEURALBASE_LISTEN_ADDR="0.0.0.0:5432" \
    NEURALBASE_RAFT_ADDR="0.0.0.0:7001" \
    NEURALBASE_METRICS_PORT="9090"

EXPOSE 5432
EXPOSE 7001
EXPOSE 8001
EXPOSE 9090

HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
    CMD timeout 2 bash -c 'echo > /dev/tcp/127.0.0.1/5432' || exit 1

ENTRYPOINT ["/app/neuralbase"]
