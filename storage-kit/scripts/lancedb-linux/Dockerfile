FROM rust:1.99.0-bookworm

# Ordinary embedded-test build tools; Cargo dependencies remain governed by Cargo.lock.
RUN apt-get update && apt-get install -y --no-install-recommends \
    clang libclang-dev cmake protobuf-compiler libprotobuf-dev python3 pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

RUN rustup component add --toolchain 1.99.0 rustfmt clippy

ENV RUSTUP_TOOLCHAIN=1.99.0 \
    CARGO_INCREMENTAL=0 \
    CARGO_BUILD_JOBS=1 \
    CARGO_PROFILE_DEV_DEBUG=0 \
    CARGO_PROFILE_TEST_DEBUG=0

WORKDIR /workspace
