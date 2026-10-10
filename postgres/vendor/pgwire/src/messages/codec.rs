use std::str;

use bytes::{Buf, BufMut, BytesMut};

use crate::error::{PgWireError, PgWireResult};

/// Get null-terminated string, returns None when empty cstring read.
///
/// Note that this implementation will also advance cursor by 1 after reading
/// empty cstring. This behaviour works for how postgres wire protocol handling
/// key-value pairs, which is ended by a single `\0`
pub(crate) fn get_cstring(buf: &mut BytesMut) -> Option<String> {
    let mut i = 0;

    if buf.remaining() == 0 {
        return None;
    }

    // with bound check to prevent invalid format
    while i < buf.remaining() && buf[i] != b'\0' {
        i += 1;
    }

    // i+1: include the '\0'
    // move cursor to the end of cstring
    // (vendored change: a string with no '\0' takes the rest of the buffer; split_to(i + 1)
    // panicked past its end. The frontend decoders read with `read_cstring`, which refuses it.)
    let string_buf = buf.split_to((i + 1).min(buf.remaining()));

    if i == 0 {
        None
    } else {
        Some(String::from_utf8_lossy(&string_buf[..i]).into_owned())
    }
}

/// A message body that holds less than it says (a count, length or field past its end): PostgreSQL's
/// pq_getmsg* answer, an ERROR (08P01). The body is its frame alone ([`decode_packet`]), so the
/// stream stays in step and the session goes on (vendored change, wire review 10 item 2: every
/// read here was unchecked against the whole read buffer).
pub(crate) const INSUFFICIENT_DATA: &str = "insufficient data left in message";

/// A protocol fault (08P01) in PostgreSQL's words.
pub(crate) fn malformed(message: &str) -> PgWireError {
    PgWireError::MalformedMessage {
        code: "08P01",
        message: message.to_owned(),
    }
}

fn take<const N: usize>(buf: &mut BytesMut) -> PgWireResult<[u8; N]> {
    if buf.remaining() < N {
        return Err(malformed(INSUFFICIENT_DATA));
    }
    let mut bytes = [0u8; N];
    buf.copy_to_slice(&mut bytes);
    Ok(bytes)
}

/// Checked reads of a frontend message body (vendored change): each refuses a body that ends
/// first with [`PgWireError::MalformedMessage`] (08P01).
pub(crate) fn read_u8(buf: &mut BytesMut) -> PgWireResult<u8> {
    take::<1>(buf).map(|b| b[0])
}

pub(crate) fn read_u16(buf: &mut BytesMut) -> PgWireResult<u16> {
    take(buf).map(u16::from_be_bytes)
}

pub(crate) fn read_u32(buf: &mut BytesMut) -> PgWireResult<u32> {
    take(buf).map(u32::from_be_bytes)
}

pub(crate) fn read_i32(buf: &mut BytesMut) -> PgWireResult<i32> {
    take(buf).map(i32::from_be_bytes)
}

/// The next `len` bytes of the body.
pub(crate) fn read_bytes(buf: &mut BytesMut, len: usize) -> PgWireResult<BytesMut> {
    if buf.remaining() < len {
        return Err(malformed(INSUFFICIENT_DATA));
    }
    Ok(buf.split_to(len))
}

/// [`get_cstring`], refusing a string the body does not terminate (PostgreSQL's "invalid string
/// in message", 08P01) and one that is not valid UTF-8 (its "invalid byte sequence for encoding",
/// 22021): read lossily, the names '\xff' and '\xfe' were one name (wire review 12 item 8).
pub(crate) fn read_cstring(buf: &mut BytesMut) -> PgWireResult<Option<String>> {
    let Some(end) = buf.iter().position(|&c| c == b'\0') else {
        return Err(malformed("invalid string in message"));
    };
    let bytes = buf.split_to(end + 1);
    let text = &bytes[..end];
    if text.is_empty() {
        return Ok(None);
    }
    match str::from_utf8(text) {
        Ok(s) => Ok(Some(s.to_owned())),
        Err(e) => {
            let at = e.valid_up_to();
            let len = e.error_len().unwrap_or(text.len() - at).clamp(1, 4);
            let sequence: Vec<String> = text[at..(at + len).min(text.len())]
                .iter()
                .map(|b| format!("0x{b:02x}"))
                .collect();
            Err(PgWireError::MalformedMessage {
                code: "22021",
                message: format!(
                    "invalid byte sequence for encoding \"UTF8\": {}",
                    sequence.join(" ")
                ),
            })
        }
    }
}

/// The body has been read to its end: bytes left over are PostgreSQL's "invalid message format"
/// (pq_getmsgend).
pub(crate) fn read_end(buf: &BytesMut) -> PgWireResult<()> {
    if buf.has_remaining() {
        return Err(malformed("invalid message format"));
    }
    Ok(())
}

/// Put null-termianted string
///
/// You can put empty string by giving `""` as input.
pub(crate) fn put_cstring(buf: &mut BytesMut, input: &str) {
    buf.put_slice(input.as_bytes());
    buf.put_u8(b'\0');
}

pub(crate) fn put_option_cstring(buf: &mut BytesMut, input: &Option<String>) {
    if let Some(input) = input {
        put_cstring(buf, input);
    } else {
        buf.put_u8(b'\0');
    }
}

/// Try to read message length from buf, without actually move the cursor
pub(crate) fn get_length(buf: &BytesMut, offset: usize) -> Option<usize> {
    if buf.remaining() >= 4 + offset {
        Some((&buf[offset..4 + offset]).get_i32() as usize)
    } else {
        None
    }
}

/// Check if message_length matches and move the cursor to right position then
/// call the `decode_fn` for the body
pub(crate) fn decode_packet<T, F>(
    buf: &mut BytesMut,
    offset: usize,
    max_size: usize,
    decode_fn: F,
) -> PgWireResult<Option<T>>
where
    F: Fn(&mut BytesMut, usize) -> PgWireResult<T>,
{
    if let Some(msg_len) = get_length(buf, offset) {
        if msg_len > max_size {
            return Err(PgWireError::MessageTooLarge(msg_len, max_size));
        }
        // A length that cannot hold itself leaves no frame to skip: the stream is out of step
        // (vendored change; PostgreSQL ends the session on "invalid message length").
        if msg_len < 4 {
            return Err(PgWireError::InvalidMessageLength(msg_len));
        }

        if buf.remaining() >= msg_len + offset {
            buf.advance(offset + 4);
            // The body is its frame and nothing past it (vendored change): decode_fn read from
            // the whole buffer, so a length or count past the frame read the messages pipelined
            // behind it, or panicked past the buffer's end (wire review 10 item 2). Whatever a
            // decoder leaves of its body is dropped with the frame.
            let mut body = buf.split_to(msg_len - 4);
            return decode_fn(&mut body, msg_len).map(|r| Some(r));
        }
    }

    Ok(None)
}

// pub(crate) fn get_and_ensure_message_type(buf: &mut BytesMut, t: u8) -> PgWireResult<()> {
//     let msg_type = buf[0];
//     // ensure the type is corrent
//     if msg_type != t {
//         return Err(PgWireError::InvalidMessageType(t, msg_type));
//     }

//     Ok(())
// }

pub(crate) fn option_string_len(s: &Option<String>) -> usize {
    1 + s.as_ref().map(|s| s.len()).unwrap_or(0)
}

#[cfg(test)]
mod test {
    use super::get_cstring;
    use bytes::{BufMut, BytesMut};

    #[test]
    fn get_cstring_valid() {
        let mut buf = BytesMut::new();
        buf.put(&b"a cstring\0"[..]);
        buf.put(&b"\0"[..]);

        assert_eq!(Some("a cstring".into()), get_cstring(&mut buf));
        assert_eq!(None, get_cstring(&mut buf));
    }

    #[test]
    fn get_cstring_empty() {
        let mut buf = BytesMut::new();

        assert_eq!(None, get_cstring(&mut buf));
    }
}
