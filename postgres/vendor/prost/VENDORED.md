# prost 0.13.5

Copied from the crates.io release already selected in Cargo.lock. Its existing licenses are retained.

Local change: optional `turso-stack-guard` feature holds Turso's existing `NestGuard` across recursive message/group/map decoding and unknown-field skipping. The PG parser enables it with `no-recursion-limit`, replacing the independent 100-message ceiling with the shared stack authority. Refusal text uses the shared message, translated to SQLSTATE 54001 by the PG parser.

The recursive guards are the only modification to upstream Rust source. A focused integration test checks unknown-group recursion with the default limit and with the shared guard. Review these four sites when updating prost. Other prost versions remain unpatched; this feature unifies across users of prost 0.13 in the same build.
