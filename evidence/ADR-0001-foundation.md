# ADR-0001: Session 1 Foundation Choices

## Status
Accepted

## Decision
- Use Rust only for Session 1 implementation.
- Implement PostgreSQL wire protocol v3 simple query support first.
- Use `sqlparser-rs` for SQL parsing.
- Keep catalog in-memory behind a trait interface for future persistence swap.

## Consequences
- Enables a minimal, testable baseline with low dependency surface.
- Defers storage and distributed complexity to later sessions.
