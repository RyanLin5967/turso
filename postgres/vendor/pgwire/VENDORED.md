# pgwire 0.36.3, vendored

Source: crates.io `pgwire-0.36.3.crate`, sha256
`70a2bcdcc4b20a88e0648778ecf00415bbd5b447742275439c22176835056f99` (the checksum the workspace's
Cargo.lock held for it). Copied whole except `Cargo.lock`, `tests-integration/`, `pgbench/`,
`.github/`, `flake.*` and `release.toml`, which no target of this package needs; `src/` and
`examples/` are byte-identical to the crate as first committed here. Wired in by the workspace's
`[patch.crates-io]` and excluded from its members, so its dev-dependencies are not resolved.

Why: the frontend-message decoders read a message body from the whole read buffer with unchecked
reads, so one client's malformed Bind or Parse panicked the server before any login (the release
profile aborts on panic), a parameter length past its frame read the messages pipelined behind it,
and every negative parameter length read as NULL (wire review 10 item 2). A wrapper codec cannot
replace pgwire's: `ClientInfo` and the rest are implemented for `Framed<_, PgWireMessageServerCodec>`
only, and the orphan rule forbids implementing them for a wrapper.

Every change from the crate is listed here, newest last; `diff -r` against the crate shows each.
