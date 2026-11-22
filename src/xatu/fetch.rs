//! The transport: one dataset file per GET.

use anyhow::{anyhow, Result};
use bytes::Bytes;
use std::time::Duration;

/// Fetches one dataset file. `Ok(None)` is "no such file" (a 404), which is how the dataset's head
/// shows; every other failure is an error. A trait so tests serve recorded files.
#[async_trait::async_trait]
pub trait Fetch: Send + Sync {
    async fn get(&self, url: &str) -> Result<Option<Bytes>>;
}

/// The real transport: plain HTTPS GETs of public files. Sends nothing about the operator.
pub struct HttpFetch(reqwest::Client);

impl HttpFetch {
    pub fn new() -> Result<Self> {
        Ok(Self(
            reqwest::Client::builder()
                // A host that accepts and then stalls must fail in seconds, not minutes: every
                // window behind this chunk waits on it. 30 s without a byte is a stall; 300 s in
                // all still lets a 50 MB file come down a slow line.
                .connect_timeout(Duration::from_secs(10))
                .read_timeout(Duration::from_secs(30))
                .timeout(Duration::from_secs(300))
                .user_agent("nuthatch")
                .build()?,
        ))
    }
}

#[async_trait::async_trait]
impl Fetch for HttpFetch {
    async fn get(&self, url: &str) -> Result<Option<Bytes>> {
        let mut last = anyhow!("no attempt made");
        for attempt in 0..3u64 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(500 * attempt)).await;
            }
            match self.0.get(url).send().await {
                Ok(r) if r.status() == reqwest::StatusCode::NOT_FOUND => return Ok(None),
                Ok(r) if r.status().is_success() => match r.bytes().await {
                    Ok(b) => return Ok(Some(b)),
                    Err(e) => last = anyhow!("reading {url}: {e}"),
                },
                // A 403 is how some object stores say "not there" for an anonymous reader.
                Ok(r) if r.status() == reqwest::StatusCode::FORBIDDEN => return Ok(None),
                Ok(r) => last = anyhow!("{url}: HTTP {}", r.status()),
                Err(e) => last = anyhow!("{url}: {e}"),
            }
        }
        Err(last)
    }
}
