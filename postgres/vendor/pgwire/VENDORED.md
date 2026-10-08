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

## Changes

1. Every frontend message body is bounded by its frame and read checked (wire review 10 item 2).
   - `messages/codec.rs`: `decode_packet` refuses a length below 4 (`InvalidMessageLength`,
     FATAL 08P01) and hands the decoder `buf.split_to(msg_len - 4)`, so no decoder reads past its
     frame and whatever a decoder leaves is dropped with it. New checked reads (`read_u8`,
     `read_u16`, `read_u32`, `read_i32`, `read_bytes`, `read_cstring`, `read_end`) refuse with
     `MalformedMessage` in PostgreSQL's words ("insufficient data left in message", "invalid
     string in message", "invalid message format"). `get_cstring` no longer panics on a string
     with no terminator.
   - `messages/extendedquery.rs` (Parse, Close, Bind, Describe, Execute, Flush, Sync),
     `messages/simplequery.rs` (Query), `messages/copy.rs` (CopyFail): checked reads, and the
     body must be read to its end. In Bind only -1 is NULL; any other negative length is refused.
   - `error.rs`: `PgWireError::InvalidMessageLength` (FATAL 08P01) and
     `PgWireError::MalformedMessage` (ERROR 08P01).
   - `messages/mod.rs`: `PgWireFrontendMessage::Malformed(type, fault)`. A body fault of a typed
     message is returned as this message, not as a codec error, because tokio-util's `Framed`
     ends the stream after a decoder error; `is_extended_query` is true for the extended types.
   - `tokio/server.rs` `process_message`: a `Malformed` message is discarded while awaiting Sync
     and otherwise answered with its ERROR.
