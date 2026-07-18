// SPDX-License-Identifier: MIT OR Apache-2.0
//! #515 ZERO-COPY GET (io_uring). Drives the RAW io_uring datapath end-to-end (the backend whose
//! `send_zc` submits `iovec`s pointing STRAIGHT at pinned store memory, read by the kernel at
//! completion time -- the real use-after-free surface, not the copying materialize fallback the
//! tokio-uring backend takes) and asserts:
//!
//! 1. A large String GET is byte-identical to the copying path -- `$<len>\r\n<bytes>\r\n` -- for a
//!    plain value, a BINARY value with embedded NUL/CR/LF, and values straddling the `ZC_THRESHOLD`
//!    (just below = copy path, at/above = zero-copy path). Both paths must produce the same wire
//!    bytes.
//! 2. A PIPELINED batch of several large GETs (several splices in ONE `send_zc` flush) returns every
//!    reply correct and in FIFO order.
//! 3. THE MEMORY-CRITICAL RACE: a reader connection hammering `GET k` on a large value while a second
//!    connection concurrently OVERWRITES `k` with a different large value / DELETEs it / churns the
//!    table (forcing slot `Arc` COWs + resizes). Every GET reply must be EITHER the null bulk OR a
//!    well-framed bulk whose body is EXACTLY one of the known values -- NEVER a torn mix of the two.
//!    A mix would mean the value blob was mutated in place while the zero-copy send was reading it;
//!    the frozen-slot-`Arc` pin (#576 COW) must make that impossible. Under ASan (the Linux io_uring
//!    CI job) an actual use-after-free of the pinned bytes also trips the sanitizer.
//!
//! The whole file is a no-op unless built for Linux with the `io_uring_raw` feature (the only config
//! in which the raw backend is honored); off that config it compiles to an empty crate.
#![cfg(all(target_os = "linux", feature = "io_uring_raw"))]

use ironcache::test_support::run_server_io_uring_raw_for_test;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// jemalloc as this test binary's global allocator, mirroring the server binary.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// The #515 zero-copy size floor (`serve::ZC_THRESHOLD`), mirrored here so the boundary cases pin to
/// the real value. A value at/above this takes the splice path; below it takes the #511 copy path.
const ZC_THRESHOLD: usize = 16 * 1024;

/// Grab a free TCP port by binding an ephemeral listener and dropping it.
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// Connect with a few short retries (the shards bind asynchronously after `run_server`).
async fn connect_retry(port: u16) -> TcpStream {
    for _ in 0..50 {
        if let Ok(s) = TcpStream::connect(("127.0.0.1", port)).await {
            let _ = s.set_nodelay(true);
            return s;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("server never came up on port {port}");
}

/// Encode a RESP2 command array from BYTE args (binary-safe: values may contain NUL/CR/LF).
fn encode_args(args: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// Send one command and read the reply until AT LEAST `expect_len` bytes have arrived (a large bulk
/// spans several socket reads), then return exactly what was read. Binary-safe.
async fn cmd_expect(client: &mut TcpStream, args: &[&[u8]], expect_len: usize) -> Vec<u8> {
    client.write_all(&encode_args(args)).await.unwrap();
    read_exact_len(client, expect_len).await
}

/// Read from `client` until at least `expect_len` bytes have arrived.
async fn read_exact_len(client: &mut TcpStream, expect_len: usize) -> Vec<u8> {
    let mut got = Vec::with_capacity(expect_len);
    // Heap-backed so the read buffer does not bloat this (and every awaiting) future's stack size.
    let mut buf = vec![0u8; 16 * 1024];
    while got.len() < expect_len {
        let n = client.read(&mut buf).await.unwrap();
        assert!(n > 0, "connection closed before full reply");
        got.extend_from_slice(&buf[..n]);
    }
    got
}

/// Build the expected `$<len>\r\n<bytes>\r\n` bulk frame for `data`.
fn bulk_frame(data: &[u8]) -> Vec<u8> {
    let mut f = format!("${}\r\n", data.len()).into_bytes();
    f.extend_from_slice(data);
    f.extend_from_slice(b"\r\n");
    f
}

fn rt() -> (tokio::runtime::Runtime, tokio::task::LocalSet) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    (rt, local)
}

#[test]
fn zero_copy_get_is_byte_identical_across_the_threshold() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_io_uring_raw_for_test(port, 1);
        let mut c = connect_retry(port).await;

        // A binary value comfortably above the threshold: mostly a marker byte, with NUL/CR/LF/0xFF
        // sprinkled in so the splice is proven binary-safe (no framing confusion on embedded CRLF).
        let mut binary = vec![b'Z'; 40 * 1024];
        for (i, b) in binary.iter_mut().enumerate() {
            if i % 512 == 0 {
                *b = [0u8, b'\r', b'\n', 0xFF][(i / 512) % 4];
            }
        }

        let big = vec![b'Q'; 64 * 1024]; // 4x threshold -> zero-copy path.
        let at = vec![b'A'; ZC_THRESHOLD]; // exactly the floor -> zero-copy path.
        let below = vec![b'b'; ZC_THRESHOLD - 1]; // one under -> #511 copy path.
        let small = vec![b's'; 8]; // tiny -> copy path.
        let cases: &[(&[u8], &[u8])] = &[
            (b"big", &big),
            (b"binZC", &binary),
            (b"at_floor", &at),
            (b"below_floor", &below),
            (b"small", &small),
        ];

        for (key, val) in cases {
            let set = cmd_expect(&mut c, &[b"SET", key, val], 5).await;
            assert_eq!(&set[..5], b"+OK\r\n", "SET {key:?} reply");
            let expected = bulk_frame(val);
            let got = cmd_expect(&mut c, &[b"GET", key], expected.len()).await;
            assert_eq!(
                got.len(),
                expected.len(),
                "GET {key:?} reply length ({} vs {})",
                got.len(),
                expected.len()
            );
            assert!(
                got == expected,
                "GET {key:?} must be byte-identical to the copy path"
            );
        }
    });
}

#[test]
fn zero_copy_get_pipelined_batch_keeps_order_and_bytes() {
    let (r, local) = rt();
    local.block_on(&r, async {
        let port = free_port();
        let _server = run_server_io_uring_raw_for_test(port, 1);
        let mut c = connect_retry(port).await;

        // Three distinct large values -> three splices in ONE flush. Distinct fill bytes + lengths so
        // a mis-ordered or mis-offset splice is caught (not just a length check).
        let vals: [Vec<u8>; 3] = [
            vec![b'1'; 20 * 1024],
            vec![b'2'; 48 * 1024],
            vec![b'3'; 33 * 1024],
        ];
        for (i, v) in vals.iter().enumerate() {
            let key = format!("p{i}");
            let set = cmd_expect(&mut c, &[b"SET", key.as_bytes(), v], 5).await;
            assert_eq!(&set[..5], b"+OK\r\n");
        }

        // Pipeline all three GETs in one write; the server encodes all replies into one batch and
        // flushes them with three ZcInserts in a single send_zc.
        let mut pipeline = Vec::new();
        for i in 0..3 {
            pipeline.extend_from_slice(&encode_args(&[b"GET", format!("p{i}").as_bytes()]));
        }
        let mut expected = Vec::new();
        for v in &vals {
            expected.extend_from_slice(&bulk_frame(v));
        }
        c.write_all(&pipeline).await.unwrap();
        let got = read_exact_len(&mut c, expected.len()).await;
        assert_eq!(got.len(), expected.len(), "pipelined batch total length");
        assert!(got == expected, "pipelined batch bytes + order must match");
    });
}

#[test]
fn zero_copy_get_survives_concurrent_same_key_mutation_and_churn() {
    let (r, local) = rt();
    local.block_on(&r, async {
        const N: usize = 64 * 1024;
        let port = free_port();
        let _server = run_server_io_uring_raw_for_test(port, 1);

        let val_a = vec![b'A'; N];
        let val_b = vec![b'B'; N];
        let frame_a = bulk_frame(&val_a);
        let frame_b = bulk_frame(&val_b);
        let null = b"$-1\r\n".to_vec();

        // Seed the key with A.
        let mut seed = connect_retry(port).await;
        let ok = cmd_expect(&mut seed, &[b"SET", b"k", &val_a], 5).await;
        assert_eq!(&ok[..5], b"+OK\r\n");

        // READER: hammer GET k. Each reply must be EXACTLY one of {frame_a, frame_b, null}: a large
        // GET pins a frozen snapshot of the value's slot Arc, so the bytes it splices can never be a
        // torn A/B mix even though the writer is overwriting k under it.
        let reader = async {
            let mut c = connect_retry(port).await;
            for _ in 0..600u32 {
                c.write_all(&encode_args(&[b"GET", b"k"])).await.unwrap();
                // Read the header, learn the declared length, then read exactly that many body bytes
                // + CRLF (or recognize the null bulk). This detects a SHORT/torn frame directly.
                let reply = read_one_bulk_or_null(&mut c).await;
                assert!(
                    reply == frame_a || reply == frame_b || reply == null,
                    "GET k returned a torn/unknown reply of {} bytes (first 16: {:?})",
                    reply.len(),
                    &reply[..reply.len().min(16)]
                );
            }
        };

        // WRITER: churn k (overwrite A<->B, occasional DEL+reset) and insert many OTHER large keys to
        // force slot-Arc COWs + HashTable resizes on the shard while GETs are in flight.
        let writer = async {
            let mut c = connect_retry(port).await;
            for i in 0..600u32 {
                let v: &[u8] = if i % 2 == 0 { &val_b } else { &val_a };
                let ok = cmd_expect(&mut c, &[b"SET", b"k", v], 5).await;
                assert_eq!(&ok[..5], b"+OK\r\n");
                if i % 37 == 0 {
                    // Momentarily delete, then restore, so the reader also exercises the null reply.
                    let _ = cmd_expect(&mut c, &[b"DEL", b"k"], 4).await;
                    let _ = cmd_expect(&mut c, &[b"SET", b"k", &val_a], 5).await;
                }
                // Table churn: distinct keys with sizeable values to grow/rehash the shard's slots.
                let churn_key = format!("churn:{i}");
                let churn_val = vec![b'c'; 1024];
                let _ = cmd_expect(&mut c, &[b"SET", churn_key.as_bytes(), &churn_val], 5).await;
            }
        };

        // Run reader + writer concurrently (they interleave at every socket await; the server races
        // the GET's pinned zero-copy send against the SET's slot COW on the shared shard thread).
        tokio::join!(reader, writer);
    });
}

/// Read ONE reply that is either a RESP bulk string (`$<len>\r\n<len bytes>\r\n`) or the null bulk
/// (`$-1\r\n`), returning the full raw frame. Reads the header first to learn the exact body length,
/// then reads precisely that many trailing bytes -- so a SHORT or over-long frame is caught here
/// rather than masked by an over-eager buffered read.
async fn read_one_bulk_or_null(c: &mut TcpStream) -> Vec<u8> {
    let mut frame = Vec::with_capacity(64 * 1024);
    // Read up to and including the first CRLF (the `$<len>` header line).
    loop {
        let mut one = [0u8; 1];
        let n = c.read(&mut one).await.unwrap();
        assert!(n == 1, "connection closed mid-header");
        frame.push(one[0]);
        if frame.len() >= 2 && frame[frame.len() - 2] == b'\r' && frame[frame.len() - 1] == b'\n' {
            break;
        }
    }
    assert_eq!(frame[0], b'$', "expected a bulk reply, got {frame:?}");
    let header = &frame[1..frame.len() - 2];
    let len: i64 = std::str::from_utf8(header).unwrap().parse().unwrap();
    if len < 0 {
        return frame; // null bulk `$-1\r\n`
    }
    // Read exactly `len` body bytes + the trailing CRLF.
    let want = usize::try_from(len).unwrap() + 2;
    let start = frame.len();
    frame.resize(start + want, 0);
    c.read_exact(&mut frame[start..]).await.unwrap();
    assert_eq!(
        &frame[frame.len() - 2..],
        b"\r\n",
        "bulk body not CRLF-terminated (torn frame)"
    );
    frame
}
