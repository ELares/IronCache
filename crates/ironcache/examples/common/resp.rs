// SPDX-License-Identifier: MIT OR Apache-2.0
//! A tiny blocking RESP (REdis Serialization Protocol) client shared by the runnable
//! examples in this directory.
//!
//! It uses ONLY the standard library (no third-party crate), so the examples build with
//! zero added dependencies and stay trivially supply-chain clean, and it doubles as a
//! compact, readable demonstration of the RESP2 wire format IronCache speaks.
//!
//! It is deliberately minimal: just enough of RESP2 to run the examples (send a command as
//! an array of bulk strings; read a simple string, error, integer, bulk string, or array).
//! It is NOT a production client. For real work use a real Redis client library (redis-py,
//! go-redis, ioredis, ...); see `docs/CLIENT_LIBRARIES.md`.
//!
//! Every example imports this module with `#[path = "common/resp.rs"] mod resp;`. Not every
//! example uses every helper, so the module allows dead code.
#![allow(dead_code)]

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;

/// The address the examples connect to. Defaults to the IronCache default listener
/// (`127.0.0.1:6379`); override with the `IRONCACHE_ADDR` environment variable.
pub fn server_addr() -> String {
    std::env::var("IRONCACHE_ADDR").unwrap_or_else(|_| "127.0.0.1:6379".to_owned())
}

/// One parsed RESP2 reply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    /// A simple string, e.g. `+OK\r\n`.
    Simple(String),
    /// An error, e.g. `-ERR wrong number of arguments\r\n`.
    Error(String),
    /// An integer, e.g. `:42\r\n`.
    Int(i64),
    /// A bulk string, e.g. `$5\r\nhello\r\n`. `None` is the null bulk string (`$-1\r\n`).
    Bulk(Option<Vec<u8>>),
    /// An array, e.g. `*2\r\n...`. `None` is the null array (`*-1\r\n`).
    Array(Option<Vec<Reply>>),
}

impl Reply {
    /// The text of a simple or bulk reply (a bulk is decoded as UTF-8 lossily); `None` for
    /// any other shape.
    pub fn as_text(&self) -> Option<String> {
        match self {
            Reply::Simple(s) => Some(s.clone()),
            Reply::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
            _ => None,
        }
    }

    /// The value of an integer reply; `None` for any other shape.
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Reply::Int(i) => Some(*i),
            _ => None,
        }
    }

    /// Whether this is a null bulk string or null array (Redis "nil").
    pub fn is_nil(&self) -> bool {
        matches!(self, Reply::Bulk(None) | Reply::Array(None))
    }

    /// The elements of an array reply; `None` for any other shape.
    pub fn into_array(self) -> Option<Vec<Reply>> {
        match self {
            Reply::Array(Some(items)) => Some(items),
            _ => None,
        }
    }

    /// Return `self` unchanged, or panic with the server's error text if it is an error.
    /// Handy for examples that want a loud failure on an unexpected `-ERR`.
    pub fn ok_or_panic(self) -> Reply {
        if let Reply::Error(e) = &self {
            panic!("server returned an error: {e}");
        }
        self
    }
}

/// A blocking RESP2 connection over a single TCP socket.
pub struct Conn {
    writer: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Conn {
    /// Open a connection to `addr` (e.g. `127.0.0.1:6379`).
    pub fn connect(addr: &str) -> io::Result<Conn> {
        let writer = TcpStream::connect(addr)?;
        let reader = BufReader::new(writer.try_clone()?);
        Ok(Conn { writer, reader })
    }

    /// Encode and send one command as a RESP array of bulk strings, without reading a reply.
    /// Sending several commands before reading any reply is exactly how pipelining works.
    pub fn send(&mut self, args: &[&str]) -> io::Result<()> {
        let mut buf: Vec<u8> = Vec::new();
        write!(buf, "*{}\r\n", args.len())?;
        for a in args {
            write!(buf, "${}\r\n", a.len())?;
            buf.extend_from_slice(a.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&buf)?;
        self.writer.flush()
    }

    /// Send one command and read its single reply (the common request/response case).
    pub fn command(&mut self, args: &[&str]) -> io::Result<Reply> {
        self.send(args)?;
        self.read_reply()
    }

    /// Read exactly one RESP2 reply from the connection (arrays are read recursively).
    pub fn read_reply(&mut self) -> io::Result<Reply> {
        let line = self.read_line()?;
        let kind = line.as_bytes().first().copied();
        let rest = &line[1..];
        match kind {
            Some(b'+') => Ok(Reply::Simple(rest.to_owned())),
            Some(b'-') => Ok(Reply::Error(rest.to_owned())),
            Some(b':') => Ok(Reply::Int(parse_int(rest)?)),
            Some(b'$') => {
                let n = parse_int(rest)?;
                if n < 0 {
                    return Ok(Reply::Bulk(None));
                }
                // The body is n bytes followed by a trailing CRLF; read both, keep the body.
                let mut body = vec![0u8; n as usize + 2];
                self.reader.read_exact(&mut body)?;
                body.truncate(n as usize);
                Ok(Reply::Bulk(Some(body)))
            }
            Some(b'*') => {
                let n = parse_int(rest)?;
                if n < 0 {
                    return Ok(Reply::Array(None));
                }
                let mut items = Vec::with_capacity(n as usize);
                for _ in 0..n {
                    items.push(self.read_reply()?);
                }
                Ok(Reply::Array(Some(items)))
            }
            other => Err(invalid(format!("unexpected reply type byte: {other:?}"))),
        }
    }

    /// Read one CRLF-terminated protocol line and return it with the trailing CRLF stripped.
    fn read_line(&mut self) -> io::Result<String> {
        let mut line = String::new();
        if self.reader.read_line(&mut line)? == 0 {
            return Err(invalid("connection closed by server".to_owned()));
        }
        Ok(line.trim_end().to_owned())
    }
}

fn parse_int(s: &str) -> io::Result<i64> {
    s.trim()
        .parse::<i64>()
        .map_err(|_| invalid(format!("invalid RESP integer: {s:?}")))
}

fn invalid(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}
