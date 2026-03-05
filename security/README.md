# Security Baseline

Session 1 security posture:
- Input parsing uses explicit `Result` paths and returns protocol errors instead of panicking.
- Wire frame lengths are validated before payload reads.
- No secrets are stored in repository files.

Authentication hardening (SCRAM/TLS) is planned for later sessions.
