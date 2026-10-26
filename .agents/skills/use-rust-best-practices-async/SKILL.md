---
name: use-rust-best-practices-async
description: "Apply this when writing or reviewing async Rust, Tokio, or futures-based code. Use it for task spawning, join/select patterns, bounded fan-out, cancellation, graceful shutdown, stream handling, mutex and channel choices, LocalSet/spawn_local, and bridging sync and async code while eliminating blocking calls, await-while-locked bugs, and clone-heavy ownership mistakes."
---

# Rust Async Best Practices

## Purpose

Guide async runtime shape, task orchestration, cancellation, and concurrency
control.

## Trigger Boundary

Use this skill whenever a Rust task includes `async`, Tokio, futures, streams,
spawned tasks, cancellation, or graceful shutdown.

## Prerequisites

- Load `use-rust-best-practices-networking` when network or database clients
  dominate the task.
- Load `use-rust-best-practices-testing` when the task is mostly about async
  test behavior.
- Load `use-rust-best-practices-abstractions` when private async traits are
  part of a closed enum-dispatch boundary.

## Workflow

1. Use async only where it buys concurrency or latency hiding.
2. Keep async all the way to natural process boundaries.
3. Choose between direct await, `join!`, `select!`, `JoinSet`, and bounded
   stream fan-out based on concurrency shape.
4. Design the sync/async boundary before selecting a blocking bridge. Prefer
   async-native APIs first; for long-lived synchronous resources, use a private
   owner thread or worker runtime with bounded `mpsc` command queues and typed
   reply channels before reaching for `spawn_blocking`, `block_in_place`,
   `block_on`, or semaphore throttling.
5. Use short lock scopes and avoid holding guards across `.await`.
6. Own values at task boundaries and borrow within a task.
7. Design shutdown up front: cancellation signal, task tracking, and bounded
   cleanup.
8. Use native `async fn` in private traits when dynamic dispatch and old MSRV
   support are not required.

## Output Rules

- Prefer direct `.await` over spawning when the task does not need to outlive
  the caller.
- Prefer bounded concurrency over unbounded fan-out.
- Prefer queues and worker ownership over `Semaphore` when serializing access
  to a naturally single-owner synchronous resource.
- Treat `spawn_blocking` as a rare bridge for short, finite, unavoidable
  blocking work. Do not use it for long-running worker loops or persistent
  sync-engine adapters.
- Treat `block_on` as valid only at sync entry points, not inside async code.
- Treat `block_in_place` as exceptional, multi-thread-runtime-only, and
  non-cancellable.
- Do not add `async_trait` for closed private enum-dispatch traits; redesign if
  the code truly needs `dyn Trait`.

## References

- `references/async.md`
- `assets/templates/sync_worker_bridge.rs`
- `assets/templates/graceful_shutdown.rs`
- `assets/templates/bounded_fanout.rs`
- `assets/templates/localset_spawn_local.rs`
