// SPDX-License-Identifier: MIT OR Apache-2.0
//! Cross-shard hop OVERLAP correctness (#8): a pipeline of remote single-key commands is now
//! DEFERRED (enqueued, not awaited inline) and its replies are assembled at the next barrier / end
//! of batch. These tests boot the REAL multi-shard `run_server` (the tokio serve loop, which opts
//! into hop deferral) and drive PIPELINED RESP over one socket, asserting the replies come back in
//! EXACT command order (FIFO on the wire) with the right values -- the cardinal property the overlap
//! must preserve. `shards == 1` (no hops) is exercised as the byte-identical control.

use ironcache::test_support::run_server_for_test;
use std::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn connect_retry(port: u16) -> TcpStream {
    for _ in 0..50 {
        if let Ok(s) = TcpStream::connect(("127.0.0.1", port)).await {
            s.set_nodelay(true).unwrap();
            return s;
        }
        tokio::task::yield_now().await;
    }
    panic!("could not connect to test server on {port}");
}

fn boot(shards: usize) -> (ironcache_runtime::bootstrap::ShardSet, u16) {
    let port = free_port();
    let set = run_server_for_test(port, shards);
    (set, port)
}

/// A RESP2 array command frame (each arg a bulk string).
fn cmd(args: &[&str]) -> String {
    use std::fmt::Write as _;
    let mut f = format!("*{}\r\n", args.len());
    for a in args {
        write!(f, "${}\r\n{}\r\n", a.len(), a).unwrap();
    }
    f
}

fn bulk(val: &str) -> Vec<u8> {
    format!("${}\r\n{}\r\n", val.len(), val).into_bytes()
}

/// Write `frame` in ONE write (a pipelined batch), then read EXACTLY `expected` bytes and assert
/// they match -- so both the reply CONTENT and the reply ORDER (FIFO) are verified in one shot.
async fn pipeline_expect(client: &mut TcpStream, frame: &str, expected: &[u8]) {
    client.write_all(frame.as_bytes()).await.unwrap();
    let mut got = vec![0u8; expected.len()];
    client.read_exact(&mut got).await.unwrap();
    assert_eq!(
        got,
        expected,
        "\n got: {:?}\nwant: {:?}",
        String::from_utf8_lossy(&got),
        String::from_utf8_lossy(expected)
    );
}

/// TEST 1 + 5 (parametrised): a pipeline of 20 SETs then 20 GETs over distinct keys, in ONE batch.
/// With `shards > 1` the keys spread across owners, so most GET/SETs take the DEFERRED hop; the
/// exact-byte assertion proves every reply lands in command order with its own value. `shards == 1`
/// is the no-hop control (byte-identical path).
async fn pipelined_set_then_get_distinct_keys(shards: usize) {
    let (_set, port) = boot(shards);
    let mut c = connect_retry(port).await;

    let n = 20;
    let mut frame = String::new();
    let mut expected: Vec<u8> = Vec::new();
    for i in 0..n {
        frame.push_str(&cmd(&["SET", &format!("k{i}"), &format!("v{i}")]));
        expected.extend_from_slice(b"+OK\r\n");
    }
    for i in 0..n {
        frame.push_str(&cmd(&["GET", &format!("k{i}")]));
        expected.extend_from_slice(&bulk(&format!("v{i}")));
    }
    pipeline_expect(&mut c, &frame, &expected).await;
}

#[tokio::test(flavor = "current_thread")]
async fn pipelined_cross_shard_set_then_get_preserves_order_3_shards() {
    pipelined_set_then_get_distinct_keys(3).await;
}

#[tokio::test(flavor = "current_thread")]
async fn pipelined_set_then_get_single_shard_control() {
    pipelined_set_then_get_distinct_keys(1).await;
}

/// TEST 2: same-key read-modify-write pipelined onto ONE remote owner -- `SET counter 0; INCR; INCR;
/// GET` in one batch. The owner is a single FIFO consumer, so the increments must apply in order and
/// the final GET must see both. Proves same-owner ordering is preserved through the overlap.
#[tokio::test(flavor = "current_thread")]
async fn pipelined_same_key_rmw_preserves_owner_order() {
    let (_set, port) = boot(3);
    let mut c = connect_retry(port).await;
    let frame = [
        cmd(&["SET", "counter", "0"]),
        cmd(&["INCR", "counter"]),
        cmd(&["INCR", "counter"]),
        cmd(&["GET", "counter"]),
    ]
    .concat();
    let mut expected = b"+OK\r\n:1\r\n:2\r\n".to_vec();
    expected.extend_from_slice(&bulk("2"));
    pipeline_expect(&mut c, &frame, &expected).await;
}

/// TEST 3: BARRIER commands (PING = AlwaysHome, synchronous) interleaved with remote hops. Each PING
/// forces the pending hop-run to DRAIN before the PING's own reply is appended, so the splice
/// (drain-before-barrier) must keep strict FIFO: reply order = command order.
#[tokio::test(flavor = "current_thread")]
async fn pipelined_barrier_between_hops_keeps_fifo() {
    let (_set, port) = boot(3);
    let mut c = connect_retry(port).await;
    let frame = [
        cmd(&["SET", "a", "1"]),
        cmd(&["PING"]),
        cmd(&["GET", "a"]),
        cmd(&["PING"]),
        cmd(&["SET", "b", "2"]),
        cmd(&["GET", "b"]),
    ]
    .concat();
    let mut expected = b"+OK\r\n+PONG\r\n".to_vec();
    expected.extend_from_slice(&bulk("1"));
    expected.extend_from_slice(b"+PONG\r\n+OK\r\n");
    expected.extend_from_slice(&bulk("2"));
    pipeline_expect(&mut c, &frame, &expected).await;
}

/// TEST 4: a large pipeline (100 distinct keys) SET then GET, proving NO reply cross-talk -- each of
/// the 100 GETs returns its OWN value in order. A reply-misassignment bug (the worst-case overlap
/// defect) would surface here as a value landing at the wrong index.
#[tokio::test(flavor = "current_thread")]
async fn pipelined_100_keys_no_reply_crosstalk() {
    let (_set, port) = boot(4);
    let mut c = connect_retry(port).await;
    let n = 100;
    let mut frame = String::new();
    let mut expected: Vec<u8> = Vec::new();
    for i in 0..n {
        frame.push_str(&cmd(&["SET", &format!("key:{i}"), &format!("val-{i}")]));
        expected.extend_from_slice(b"+OK\r\n");
    }
    for i in 0..n {
        frame.push_str(&cmd(&["GET", &format!("key:{i}")]));
        expected.extend_from_slice(&bulk(&format!("val-{i}")));
    }
    pipeline_expect(&mut c, &frame, &expected).await;
}

/// TEST 6: an ERROR reply mid-pipeline on a remote owner (`INCR` on a non-integer value) must land
/// in its exact command position, with the surrounding replies intact -- so the error is assembled
/// in order and `record_command_stats` reads the right (leading-`-`) slice.
#[tokio::test(flavor = "current_thread")]
async fn pipelined_error_reply_lands_in_order() {
    let (_set, port) = boot(3);
    let mut c = connect_retry(port).await;
    // SET s hello; INCR s (errors: not an integer); GET s -> still "hello".
    let frame = [
        cmd(&["SET", "s", "hello"]),
        cmd(&["INCR", "s"]),
        cmd(&["GET", "s"]),
    ]
    .concat();
    client_write(&mut c, &frame).await;

    // reply 1: +OK\r\n
    expect_exact(&mut c, b"+OK\r\n").await;
    // reply 2: an error line "-...\r\n" -- read until the CRLF, assert it starts with '-'.
    let err = read_line(&mut c).await;
    assert!(
        err.starts_with(b"-"),
        "expected an error reply in position 2, got {:?}",
        String::from_utf8_lossy(&err)
    );
    // reply 3: the unchanged bulk "hello".
    expect_exact(&mut c, &bulk("hello")).await;
}

/// TEST (design #4): a SELECT (a state-mutating AlwaysHome BARRIER) between remote hops must take
/// effect BEFORE the following command routes -- the overlap must never let a later command run
/// against the wrong `db`. `SET k1 aaa` (db0); `SELECT 1`; `GET k1` (db1 -> nil); `SELECT 0`;
/// `GET k1` (db0 -> aaa). If SELECT were overlapped past, the db-1 GET would wrongly see `aaa`.
#[tokio::test(flavor = "current_thread")]
async fn pipelined_select_barrier_takes_effect_before_next_command() {
    let (_set, port) = boot(3);
    let mut c = connect_retry(port).await;
    let frame = [
        cmd(&["SET", "k1", "aaa"]),
        cmd(&["SELECT", "1"]),
        cmd(&["GET", "k1"]),
        cmd(&["SELECT", "0"]),
        cmd(&["GET", "k1"]),
    ]
    .concat();
    let mut expected = b"+OK\r\n+OK\r\n$-1\r\n+OK\r\n".to_vec();
    expected.extend_from_slice(&bulk("aaa"));
    pipeline_expect(&mut c, &frame, &expected).await;
}

/// TEST (review #1 regression): a valid deferred remote hop followed by a MALFORMED frame in the
/// same batch. The protocol-error close path must FIRST drain the pending hop so the valid command's
/// reply still goes out (in order) before the error + close -- the overlap must not silently drop it.
#[tokio::test(flavor = "current_thread")]
async fn deferred_hop_reply_survives_a_trailing_protocol_error() {
    let (_set, port) = boot(3);
    let mut c = connect_retry(port).await;
    // Seed a value (its own batch), then pipeline: GET a (a deferred remote hop) + a malformed
    // array-count frame (`*x\r\n`) which decodes to a protocol Error and closes the connection.
    client_write(&mut c, &cmd(&["SET", "a", "hello"])).await;
    expect_exact(&mut c, b"+OK\r\n").await;

    let mut frame = cmd(&["GET", "a"]);
    frame.push_str("*x\r\n"); // malformed: non-numeric array count -> DecodeOutcome::Error
    client_write(&mut c, &frame).await;

    // The deferred GET's reply must arrive FIRST (drained before the error), in order.
    expect_exact(&mut c, &bulk("hello")).await;
    // Then the protocol error line.
    let err = read_line(&mut c).await;
    assert!(
        err.starts_with(b"-"),
        "expected the protocol error after the drained hop reply, got {:?}",
        String::from_utf8_lossy(&err)
    );
}

async fn client_write(client: &mut TcpStream, frame: &str) {
    client.write_all(frame.as_bytes()).await.unwrap();
}

async fn expect_exact(client: &mut TcpStream, expected: &[u8]) {
    let mut got = vec![0u8; expected.len()];
    client.read_exact(&mut got).await.unwrap();
    assert_eq!(got, expected, "got {:?}", String::from_utf8_lossy(&got));
}

/// Read one CRLF-terminated line (the leading type byte through `\r\n`), returning it WITH the CRLF.
async fn read_line(client: &mut TcpStream) -> Vec<u8> {
    let mut line = Vec::new();
    let mut b = [0u8; 1];
    loop {
        client.read_exact(&mut b).await.unwrap();
        line.push(b[0]);
        if line.len() >= 2 && line[line.len() - 2] == b'\r' && line[line.len() - 1] == b'\n' {
            return line;
        }
    }
}
