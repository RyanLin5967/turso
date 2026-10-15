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
2. A malformed Sync is not an extended message (`is_extended_query` covers P, B, C, D, E, H):
   PostgreSQL ends a skip at any Sync before it reads the body, so the server answers it itself,
   ending the round as on_sync does, with ReadyForQuery (wire review 12 item 6, review 15 item 4).
   A malformed simple Query is answered the same way outside a skip (wire review 12 item 7); during
   one it is ignored, as PostgreSQL ignores every message but Sync there before reading its body
   (wire review 15 item 5: answering it ended the skip).
3. `read_cstring` decodes strictly: invalid UTF-8 is 22021 "invalid byte sequence for encoding
   \"UTF8\": 0x.." (it was read lossily), in one scan for the terminator. `MalformedMessage` and
   `PgWireFrontendMessage::Malformed` carry the SQLSTATE and the message (wire review 12 item 8).
4. `api/auth/noop.rs`: `NoopStartupHandler` gains `server_parameters`, the provider of the
   ParameterStatus values startup announces, `DefaultServerParameterProvider::default()` unless
   the handler overrides it; `on_startup` used that default unconditionally, so the server could
   not announce the settings SHOW answers (`ISO, YMD` against `ISO, MDY`, `Etc/UTC` against `UTC`,
   `public` against `"$user", public`; wire review 18 item 19).
5. `types/format/float_digits.rs`: at extra_float_digits >= 1 (the default, 1) a float's text is
   PostgreSQL's float8out / float4out layout (`postgres_shortest`): the shortest exact digits (Rust's
   `{:e}`), fixed notation while the decimal exponent is in [-4, 15) (float8) or [-4, 6) (float4),
   else `d.ddde+XX`. ryu's text was `1.0`, `100.0`, `1e16`, `0.00001` (PostgreSQL 18.6 recorded,
   artie lanes/wire/raw/loud-20261015/pg18-diff/pg18-float.out). The infinities are item 6. This crate's tests do not build alone here
   (rustc 1.88 is below its MSRV); test_format's
   PI rows were checked by reading: `{:e}` of PI is `3.141592653589793e0`, written `3.141592653589793`.
6. `types/format/float_digits.rs`: the infinities are written `Infinity` / `-Infinity` and a
   negative zero `-0`, as PostgreSQL 18.6's float8out / float4out write them (recorded, artie
   lanes/wire/raw/loud-20261015/pg18-diff/pg18-inf.out); upstream wrote `inf`, `-inf` and `0`.
   FLAGGED TEST EDIT, ruled by the lead (2026-10-15T07:40:59Z): test_format's two infinity rows
   now assert `Infinity` / `-Infinity`. The crate's tests do not build alone here (MSRV), so the
   wire test infinities_and_negative_zero_are_written_without_nan is the check that runs.
7. `api/auth/noop.rs`: `NoopStartupHandler::backend_key`, set on the client before
   `finish_authentication0` sends BackendKeyData. Upstream never set a key, so every session sent
   process id 0 and secret 0, and a CancelRequest could not name one session (the wire server's
   cancel handler, 2026-10-15).
8. `messages/mod.rs`, `api/results.rs`, `api/query.rs`: `PgWireBackendMessage::Inline` and
   `Response::Inline`, backend messages the server encoded itself into a fixed 256-byte buffer and
   written as they are, so a branch call's simple-Query reply (RowDescription, DataRow,
   CommandComplete) costs no heap allocation (L5). pgwire's own simple-query loop feeds one as it
   is; its extended loop refuses one (a pre-encoded reply holds a RowDescription, which Execute
   never sends). The server checks the bytes equal pgwire's encoding of the same reply
   (inline_replies_are_the_messages_pgwire_encodes).
9. `tokio/server.rs`: `MaybeTls::Blocking(BlockingTcp)`, a std socket in blocking mode whose
   AsyncRead/AsyncWrite polls always complete (the read or write blocks in the system call; a read
   deadline, re-armed as SO_RCVTIMEO before every read so a trickling client cannot stretch it, reads
   as the stream's end with `timed_out` set), and `negotiate_blocking`,
   `negotiate_tls(.., None)` on such a socket. `peek_for_sslrequest` takes any IO and is told
   whether the first byte began a direct TLS handshake. The server runs each session on one with
   no runtime, so the session thread makes no kevent (L5 step 3(a), lead 2026-10-15).
