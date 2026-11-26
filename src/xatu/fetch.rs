//! The transport: one dataset file per GET.

use anyhow::{anyhow, Result};
use bytes::Bytes;
use std::time::Duration;

/// Fetches one dataset file. `Ok(None)` is "no such file" (a 404), which is how the dataset's head
/// shows; every other failure is an error. A trait so tests serve recorded files.
#[async_trait::async_trait]
pub trait Fetch: Send + Sync {
    /// `max` caps the body in bytes; a longer one fails with [`TooLarge`] without being read whole.
    async fn get(&self, url: &str, max: usize) -> Result<Option<Bytes>>;
}

/// The body is, or says it is, longer than the cap. The host is misbehaving, not briefly down.
#[derive(Debug)]
pub struct TooLarge {
    pub url: String,
    pub max: usize,
}

impl std::fmt::Display for TooLarge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: body over the {} byte cap", self.url, self.max)
    }
}

impl std::error::Error for TooLarge {}

/// Reads `body` into memory, refusing past `max` bytes: a declared length over it is refused before
/// a byte is read, and the running total is checked per chunk (a host can omit or lie about it).
pub async fn read_capped<S, B, E>(
    url: &str,
    declared: Option<u64>,
    body: S,
    max: usize,
) -> Result<Bytes>
where
    S: futures::Stream<Item = std::result::Result<B, E>>,
    B: AsRef<[u8]>,
    E: std::fmt::Display,
{
    use futures::StreamExt;
    let over = || TooLarge {
        url: url.to_string(),
        max,
    };
    if declared.is_some_and(|n| n > max as u64) {
        return Err(over().into());
    }
    let mut buf = Vec::with_capacity(declared.map_or(0, |n| n as usize).min(max));
    futures::pin_mut!(body);
    while let Some(part) = body.next().await {
        let part = part.map_err(|e| anyhow!("reading {url}: {e}"))?;
        let part = part.as_ref();
        if part.len() > max - buf.len() {
            return Err(over().into());
        }
        buf.extend_from_slice(part);
    }
    Ok(buf.into())
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
    async fn get(&self, url: &str, max: usize) -> Result<Option<Bytes>> {
        let mut last = anyhow!("no attempt made");
        for attempt in 0..3u64 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(500 * attempt)).await;
            }
            match self.0.get(url).send().await {
                Ok(r) if r.status() == reqwest::StatusCode::NOT_FOUND => return Ok(None),
                Ok(r) if r.status().is_success() => {
                    let declared = r.content_length();
                    match read_capped(url, declared, r.bytes_stream(), max).await {
                        Ok(b) => return Ok(Some(b)),
                        // Retrying would only download it again.
                        Err(e) if e.is::<TooLarge>() => return Err(e),
                        Err(e) => last = e,
                    }
                }
                // A 403 is how some object stores say "not there" for an anonymous reader.
                Ok(r) if r.status() == reqwest::StatusCode::FORBIDDEN => return Ok(None),
                Ok(r) => last = anyhow!("{url}: HTTP {}", r.status()),
                Err(e) => last = anyhow!("{url}: {e}"),
            }
        }
        Err(last)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// An endless body of 1 MiB parts that counts how many were pulled.
    fn endless(pulled: Arc<AtomicUsize>) -> impl futures::Stream<Item = Result<Bytes, String>> {
        futures::stream::repeat_with(move || {
            pulled.fetch_add(1, Ordering::SeqCst);
            Ok(Bytes::from(vec![0u8; 1 << 20]))
        })
    }

    #[tokio::test]
    async fn an_endless_body_stops_at_the_cap_without_being_read_whole() {
        let pulled = Arc::new(AtomicUsize::new(0));
        let e = read_capped("u", None, endless(pulled.clone()), 4 << 20)
            .await
            .unwrap_err();
        assert!(e.is::<TooLarge>());
        // 4 parts fit, the 5th trips the cap: nothing near the whole (endless) body was pulled.
        assert_eq!(pulled.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn a_declared_length_over_the_cap_is_refused_before_any_read() {
        let pulled = Arc::new(AtomicUsize::new(0));
        let e = read_capped("u", Some(3 << 30), endless(pulled.clone()), 16 << 20)
            .await
            .unwrap_err();
        assert!(e.is::<TooLarge>());
        assert_eq!(pulled.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn a_body_within_the_cap_is_returned_whole() {
        let parts = futures::stream::iter(vec![
            Ok::<_, String>(Bytes::from_static(b"ab")),
            Ok(Bytes::from_static(b"cd")),
        ]);
        let b = read_capped("u", Some(4), parts, 4).await.unwrap();
        assert_eq!(&b[..], b"abcd");
    }
}
