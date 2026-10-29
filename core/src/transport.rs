//! The bytes under [`crate::rpc::RpcClient`] (RFC-0075 §5): one POST of a JSON body and the reply.
//! Every decision about what to send, where, and what a failure means stays in `rpc`.

use serde_json::Value;
use std::time::Duration;

/// A request that failed before it had a reply, or a reply whose body could not be read.
#[derive(Debug)]
pub struct TransportError {
    /// The deadline the transport was built with ran out.
    pub timed_out: bool,
    pub error: anyhow::Error,
}

#[async_trait::async_trait]
pub trait Reply: Send {
    fn status(&self) -> u16;
    /// The status as it reads in a log line, `429 Too Many Requests`.
    fn status_line(&self) -> String;
    fn retry_after(&self) -> Option<String>;
    fn content_length(&self) -> Option<u64>;
    async fn text(self) -> Result<String, TransportError>;
    async fn json(self) -> Result<Value, TransportError>;
    /// The next piece of the body, or `None` once it is all read.
    async fn chunk(&mut self) -> Result<Option<bytes::Bytes>, TransportError>;
}

#[async_trait::async_trait]
pub trait Transport: Send + Sync + Sized + 'static {
    type Reply: Reply;

    /// A transport whose requests, body included, give up after `timeout`.
    fn build(timeout: Duration) -> anyhow::Result<Self>;

    async fn post(&self, url: &str, body: &Value) -> Result<Self::Reply, TransportError>;

    /// Headers per batch where an endpoint in `urls` is known to take fewer than the client sends.
    fn header_batch_cap(_urls: &[String]) -> Option<usize> {
        None
    }

    // What the node counts into its process-wide series; a tab has nowhere to publish them.
    fn inc_rpc(&self) {}
    fn inc_rpc_method(&self, _method: &str) {}
    fn inc_rpc_methods(&self, _method: &str, _n: u64) {}
    fn observe_rpc(&self, _url: &str, _elapsed: Duration, _failed: bool, _retried: bool) {}
}
