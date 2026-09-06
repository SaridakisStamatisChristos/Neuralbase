# ADR-0001: Foundation choices

## Status

Accepted (historical foundation decision)

## Context

NeuralBase began with a deliberately small Rust-only SQL/protocol foundation before storage and distributed components were added.

## Decision

- Use Rust for the database engine implementation.
- Implement a PostgreSQL wire-protocol endpoint as the initial client interface.
- Use `sqlparser-rs` for SQL parsing.
- Keep catalog access behind a trait boundary so persistence can evolve independently.

## Consequences

The initial architecture established a small testable front end and left room for later persistent storage and consensus work. The repository has since added MVCC/RocksDB persistence, broader execution, authentication, and Raft; the original in-memory-only implementation state is historical rather than current behavior.
