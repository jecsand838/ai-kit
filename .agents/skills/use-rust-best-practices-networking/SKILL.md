---
name: use-rust-best-practices-networking
description: "Apply this when writing or reviewing Rust networking code: HTTP/REST clients and servers, gRPC with tonic, database access and pools, retries, timeouts, backpressure, and connection reuse. Use it to reuse reqwest clients and tonic channels correctly, choose crate-provided pools over custom pools, separate transport DTOs from domain code, and avoid rebuilding clients, over-pooling, unsafe retries, and weak network error handling."
---

# Rust Networking Best Practices

## Purpose

Guide HTTP, gRPC, database-client, pooling, retry, timeout, and backpressure
decisions.

## Trigger Boundary

Use this skill for HTTP/REST, gRPC, database client, connection-pool, and other
network-bound Rust code.

## Prerequisites

- Load `use-rust-best-practices-async` when task orchestration or shutdown
  dominates.
- Load `use-rust-best-practices-dependencies-platforms` when dependency choice
  or platform support dominates.

## Workflow

1. Reuse long-lived clients, channels, and pools.
2. Prefer crate-provided pools before building a custom pool.
3. Set timeouts, surface error context, and apply retries only when safe.
4. Keep transport DTOs and transport errors separate from domain models and
   domain errors.
5. Choose the simplest concurrency model that preserves backpressure.

## Output Rules

- Avoid ad-hoc retry loops around non-idempotent operations.
- Bound network fan-out to protect upstreams and your own process.

## References

- `references/networking.md`
- `assets/templates/typed_http_client.rs`
- `assets/templates/grpc_client.rs`
- `assets/templates/sqlx_pool.rs`
