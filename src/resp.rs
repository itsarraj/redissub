use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>),
    Array(Option<Vec<RespValue>>),
}

impl RespValue {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            RespValue::SimpleString(s) => Some(s.as_str()),
            RespValue::Error(s) => Some(s.as_str()),
            RespValue::BulkString(Some(b)) => std::str::from_utf8(b).ok(),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            RespValue::BulkString(Some(b)) => Some(b.as_slice()),
            RespValue::SimpleString(s) => Some(s.as_bytes()),
            RespValue::Error(s) => Some(s.as_bytes()),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            RespValue::Integer(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&[RespValue]> {
        match self {
            RespValue::Array(Some(arr)) => Some(arr.as_slice()),
            _ => None,
        }
    }
}

impl fmt::Display for RespValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RespValue::SimpleString(s) => write!(f, "{}", s),
            RespValue::Error(e) => write!(f, "ERR: {}", e),
            RespValue::Integer(i) => write!(f, "{}", i),
            RespValue::BulkString(Some(b)) => {
                if let Ok(s) = std::str::from_utf8(b) {
                    write!(f, "{}", s)
                } else {
                    write!(f, "{:?}", b)
                }
            }
            RespValue::BulkString(None) => write!(f, "(nil)"),
            RespValue::Array(Some(arr)) => {
                write!(f, "[")?;
                for (i, item) in arr.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", item)?;
                }
                write!(f, "]")
            }
            RespValue::Array(None) => write!(f, "(nil)"),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RespError {
    #[error("Protocol error: {0}")]
    Protocol(String),
    #[error("Integer parse error: {0}")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("UTF-8 error: {0}")]
    Utf8(#[from] std::str::Utf8Error),
}

/// Encode a command and arguments as a RESP Array of Bulk Strings.
pub fn encode_command<T: AsRef<[u8]>>(args: &[T]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for arg in args {
        let bytes = arg.as_ref();
        buf.extend_from_slice(format!("${}\r\n", bytes.len()).as_bytes());
        buf.extend_from_slice(bytes);
        buf.extend_from_slice(b"\r\n");
    }
    buf
}

/// Attempts to decode a single RESP value from the front of `buf`.
/// Returns `Ok(Some((value, bytes_consumed)))` if a complete value was parsed.
/// Returns `Ok(None)` if the buffer does not yet contain a complete value.
/// Returns `Err(RespError)` if the data is malformed.
pub fn decode_resp(buf: &[u8]) -> Result<Option<(RespValue, usize)>, RespError> {
    decode_resp_depth(buf, 0)
}

fn decode_resp_depth(buf: &[u8], depth: usize) -> Result<Option<(RespValue, usize)>, RespError> {
    if depth > 64 {
        return Err(RespError::Protocol("Recursion depth limit exceeded".into()));
    }
    if buf.is_empty() {
        return Ok(None);
    }

    match buf[0] {
        b'+' => {
            // Simple String: +OK\r\n
            if let Some(pos) = find_crlf(&buf[1..]) {
                let s = std::str::from_utf8(&buf[1..=pos])?.to_string();
                Ok(Some((RespValue::SimpleString(s), pos + 3))) // 1 ('+') + pos + 2 (\r\n)
            } else {
                Ok(None)
            }
        }
        b'-' => {
            // Error: -Error message\r\n
            if let Some(pos) = find_crlf(&buf[1..]) {
                let s = std::str::from_utf8(&buf[1..=pos])?.to_string();
                Ok(Some((RespValue::Error(s), pos + 3)))
            } else {
                Ok(None)
            }
        }
        b':' => {
            // Integer: :1000\r\n
            if let Some(pos) = find_crlf(&buf[1..]) {
                let s = std::str::from_utf8(&buf[1..=pos])?;
                let val: i64 = s.parse()?;
                Ok(Some((RespValue::Integer(val), pos + 3)))
            } else {
                Ok(None)
            }
        }
        b'$' => {
            // Bulk String: $<len>\r\n<data>\r\n or $-1\r\n
            if let Some(pos) = find_crlf(&buf[1..]) {
                let len_str = std::str::from_utf8(&buf[1..=pos])?;
                let len: i64 = len_str.parse()?;
                let header_len = pos + 3; // 1 ('$') + pos + 2 (\r\n)

                if len < 0 {
                    // Null bulk string
                    return Ok(Some((RespValue::BulkString(None), header_len)));
                }

                let usize_len = len as usize;
                let total_needed = header_len + usize_len + 2; // + \r\n
                if buf.len() < total_needed {
                    return Ok(None);
                }

                let data = buf[header_len..header_len + usize_len].to_vec();
                if &buf[header_len + usize_len..total_needed] != b"\r\n" {
                    return Err(RespError::Protocol("Bulk string missing trailing CRLF".into()));
                }

                Ok(Some((RespValue::BulkString(Some(data)), total_needed)))
            } else {
                Ok(None)
            }
        }
        b'*' => {
            // Array: *<len>\r\n<elements>... or *-1\r\n
            if let Some(pos) = find_crlf(&buf[1..]) {
                let len_str = std::str::from_utf8(&buf[1..=pos])?;
                let count: i64 = len_str.parse()?;
                let mut consumed = pos + 3;

                if count < 0 {
                    return Ok(Some((RespValue::Array(None), consumed)));
                }

                let count = count as usize;
                let mut items = Vec::with_capacity(count);

                for _ in 0..count {
                    if consumed >= buf.len() {
                        return Ok(None);
                    }
                    match decode_resp_depth(&buf[consumed..], depth + 1)? {
                        Some((item, n)) => {
                            items.push(item);
                            consumed += n;
                        }
                        None => return Ok(None),
                    }
                }

                Ok(Some((RespValue::Array(Some(items)), consumed)))
            } else {
                Ok(None)
            }
        }
        other => Err(RespError::Protocol(format!(
            "Unexpected RESP frame prefix byte: '{}' (0x{:02x})",
            other as char, other
        ))),
    }
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    for i in 0..buf.len().saturating_sub(1) {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_command() {
        let cmd = encode_command(&["SUBSCRIBE", "chat", "news"]);
        assert_eq!(cmd, b"*3\r\n$9\r\nSUBSCRIBE\r\n$4\r\nchat\r\n$4\r\nnews\r\n");
    }

    #[test]
    fn test_decode_simple_string() {
        let res = decode_resp(b"+PONG\r\n").unwrap();
        assert_eq!(res, Some((RespValue::SimpleString("PONG".into()), 7)));
    }

    #[test]
    fn test_decode_error() {
        let res = decode_resp(b"-ERR unknown command\r\n").unwrap();
        assert_eq!(res, Some((RespValue::Error("ERR unknown command".into()), 22)));
    }

    #[test]
    fn test_decode_integer() {
        let res = decode_resp(b":42\r\n").unwrap();
        assert_eq!(res, Some((RespValue::Integer(42), 5)));
    }

    #[test]
    fn test_decode_bulk_string() {
        let res = decode_resp(b"$5\r\nhello\r\n").unwrap();
        assert_eq!(res, Some((RespValue::BulkString(Some(b"hello".to_vec())), 11)));

        let nil = decode_resp(b"$-1\r\n").unwrap();
        assert_eq!(nil, Some((RespValue::BulkString(None), 5)));

        let empty = decode_resp(b"$0\r\n\r\n").unwrap();
        assert_eq!(empty, Some((RespValue::BulkString(Some(vec![])), 6)));
    }

    #[test]
    fn test_decode_array() {
        let raw = b"*2\r\n$4\r\necho\r\n$5\r\nworld\r\n";
        let res = decode_resp(raw).unwrap();
        match res {
            Some((RespValue::Array(Some(arr)), len)) => {
                assert_eq!(len, raw.len());
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0].as_str(), Some("echo"));
                assert_eq!(arr[1].as_str(), Some("world"));
            }
            _ => panic!("Expected array"),
        }

        let nil_arr = decode_resp(b"*-1\r\n").unwrap();
        assert_eq!(nil_arr, Some((RespValue::Array(None), 5)));
    }

    #[test]
    fn test_decode_partial() {
        assert_eq!(decode_resp(b"$5\r\nhel").unwrap(), None);
        assert_eq!(decode_resp(b"*2\r\n$4\r\necho\r\n").unwrap(), None);
    }
}
