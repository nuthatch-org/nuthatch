//! #1660: a connection that never finishes its request headers is closed by the bind, not held
//! forever. Tested over a real socket through [`nuthatch::serve::bind_and_serve`], because the bound
//! lives in the connection builder, below every route-level guard.

use std::time::Duration;

use axum::{routing::get, Router};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// How long past the bound the server is given to hang up before the test calls it held.
const GRACE: Duration = Duration::from_secs(5);

async fn serving() -> (String, tokio::task::JoinHandle<anyhow::Result<()>>) {
    let addr = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().to_string()
    };
    let app = Router::new().route("/health", get(|| async { "ok" }));
    let task = {
        let addr = addr.clone();
        tokio::spawn(async move {
            nuthatch::serve::bind_and_serve(&addr, app, None, Default::default()).await
        })
    };
    let started = std::time::Instant::now();
    while reqwest::get(format!("http://{addr}/health")).await.is_err() {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "never came up on {addr}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    (addr, task)
}

/// Read until the server hangs up, or give up at the bound plus [`GRACE`].
async fn closed_within_the_bound(read: &mut tokio::net::tcp::OwnedReadHalf) -> bool {
    let mut buf = [0u8; 1024];
    let wait = nuthatch::serve::HEADER_READ_TIMEOUT + GRACE;
    tokio::time::timeout(wait, async {
        loop {
            match read.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
        }
    })
    .await
    .is_ok()
}

#[tokio::test]
async fn a_client_dribbling_its_headers_is_dropped_at_the_bound() {
    let (addr, task) = serving().await;
    let (mut read, mut write) = tokio::net::TcpStream::connect(&addr)
        .await
        .unwrap()
        .into_split();
    write
        .write_all(b"GET /health HTTP/1.1\r\nHost: x\r\n")
        .await
        .unwrap();
    let dribble = tokio::spawn(async move {
        while write.write_all(b"x").await.is_ok() {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });
    assert!(
        closed_within_the_bound(&mut read).await,
        "a connection dribbling its headers was held past {:?}",
        nuthatch::serve::HEADER_READ_TIMEOUT + GRACE
    );
    dribble.abort();
    task.abort();
}

#[tokio::test]
async fn a_client_that_never_sends_a_header_is_dropped_at_the_bound() {
    let (addr, task) = serving().await;
    let (mut read, _write) = tokio::net::TcpStream::connect(&addr)
        .await
        .unwrap()
        .into_split();
    assert!(
        closed_within_the_bound(&mut read).await,
        "a silent connection was held past {:?}",
        nuthatch::serve::HEADER_READ_TIMEOUT + GRACE
    );
    task.abort();
}
