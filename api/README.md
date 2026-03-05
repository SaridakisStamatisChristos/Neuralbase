# API Layer

Session 1 exposes PostgreSQL wire protocol v3 over TCP on `127.0.0.1:5432`.

Current capability:
- Startup handshake (`SSLRequest`, protocol v3 startup)
- Simple query protocol (`Q`)
- Responses: `RowDescription`, `DataRow`, `CommandComplete`, `ReadyForQuery`, `ErrorResponse`

Extended protocol and auth methods are intentionally deferred.
