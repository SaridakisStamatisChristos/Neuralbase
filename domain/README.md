# Domain Layer

Session 1 domain logic includes:
- SQL parsing through `sqlparser-rs`
- Binding SQL AST to a bound logical representation
- Validation of table existence through trait-based catalog access

Execution remains minimal and returns deterministic mock rows for table scans.
