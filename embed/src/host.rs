//! JSON-RPC through the host, and the executor that drives discovery's futures over it.
//!
//! A call does not block the guest. It queues its request and returns pending; when every future the
//! discovery can advance is waiting on a call, the executor hands the whole queue to the host in one
//! `rpc_batch`, which may run them concurrently, and wakes each caller with its answer. Discovery's own
//! concurrency (eight spans in flight per provider) therefore reaches the network as it does natively,
//! without threads or an async runtime in the guest.

use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use crate::address_discovery::{Rpc, LOGS_BODY_CAP};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Endpoint {
    Main,
    Trace,
}

#[derive(Debug, Serialize)]
pub struct Request {
    pub endpoint: Endpoint,
    pub method: String,
    pub params: Value,
}

/// The host's answer to one request: the HTTP body, or why there was none after its retries.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum Reply {
    Body { body: String },
    Failed { error: String },
}

thread_local! {
    static QUEUE: RefCell<Vec<(u64, Request, Waker)>> = const { RefCell::new(Vec::new()) };
    static DONE: RefCell<HashMap<u64, Reply>> = RefCell::new(HashMap::new());
    static NEXT: RefCell<u64> = const { RefCell::new(0) };
}

/// One endpoint the host reaches on the module's behalf.
#[derive(Clone, Copy)]
pub struct HostRpc(pub Endpoint);

impl Rpc for HostRpc {
    fn call(&self, method: &str, params: Value) -> impl Future<Output = Result<Value>> + Send {
        HostCall {
            state: State::Unsent(Some(Request {
                endpoint: self.0,
                method: method.to_string(),
                params,
            })),
        }
    }
}

enum State {
    Unsent(Option<Request>),
    Sent(u64, String),
}

struct HostCall {
    state: State,
}

impl Future for HostCall {
    type Output = Result<Value>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<Value>> {
        match &mut self.state {
            State::Unsent(req) => {
                let req = req.take().expect("a request is sent once");
                let id = NEXT.with(|n| {
                    let mut n = n.borrow_mut();
                    *n += 1;
                    *n
                });
                let method = req.method.clone();
                QUEUE.with(|q| q.borrow_mut().push((id, req, cx.waker().clone())));
                self.state = State::Sent(id, method);
                Poll::Pending
            }
            State::Sent(id, method) => match DONE.with(|d| d.borrow_mut().remove(id)) {
                Some(reply) => Poll::Ready(judge(method, reply)),
                None => {
                    let waker = cx.waker().clone();
                    QUEUE.with(|q| {
                        for (i, _, w) in q.borrow_mut().iter_mut() {
                            if i == id {
                                *w = waker.clone();
                            }
                        }
                    });
                    Poll::Pending
                }
            },
        }
    }
}

/// A JSON-RPC body as discovery expects a client to have judged it: the result, or an error whose
/// text carries the provider's message, which is what discovery's span narrowing reads.
fn judge(method: &str, reply: Reply) -> Result<Value> {
    let body = match reply {
        Reply::Body { body } => body,
        Reply::Failed { error } => bail!("{method}: {error}"),
    };
    if method == "eth_getLogs" && body.len() > LOGS_BODY_CAP {
        bail!(
            "{method}: response size {} exceeded the {LOGS_BODY_CAP}-byte cap",
            body.len()
        );
    }
    let v: Value =
        serde_json::from_str(&body).map_err(|e| anyhow!("{method}: unreadable body: {e}"))?;
    if let Some(err) = v.get("error") {
        let msg = err.get("message").and_then(Value::as_str).unwrap_or("");
        let code = err.get("code").map(Value::to_string).unwrap_or_default();
        bail!("{method}: {msg} (code {code})");
    }
    v.get("result")
        .cloned()
        .ok_or_else(|| anyhow!("{method}: a body with neither result nor error"))
}

struct Flag(AtomicBool);

impl Wake for Flag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// Run `fut` to completion, sending every round of queued calls through `transport`.
pub fn block_on<F: Future>(
    fut: F,
    transport: &mut dyn FnMut(&[Request]) -> Result<Vec<Reply>>,
) -> Result<F::Output> {
    let mut fut = std::pin::pin!(fut);
    let flag = Arc::new(Flag(AtomicBool::new(true)));
    let waker = Waker::from(flag.clone());
    let mut cx = Context::from_waker(&waker);
    loop {
        if flag.0.swap(false, Ordering::SeqCst) {
            if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
                QUEUE.with(|q| q.borrow_mut().clear());
                DONE.with(|d| d.borrow_mut().clear());
                return Ok(v);
            }
            continue;
        }
        let round: Vec<(u64, Request, Waker)> =
            QUEUE.with(|q| std::mem::take(&mut *q.borrow_mut()));
        if round.is_empty() {
            bail!("the module is waiting on nothing the host can answer");
        }
        let requests: Vec<Request> = round
            .iter()
            .map(|(_, r, _)| Request {
                endpoint: r.endpoint,
                method: r.method.clone(),
                params: r.params.clone(),
            })
            .collect();
        let replies = transport(&requests)?;
        if replies.len() != round.len() {
            bail!(
                "the host answered {} of {} requests",
                replies.len(),
                round.len()
            );
        }
        for ((id, _, w), reply) in round.into_iter().zip(replies) {
            DONE.with(|d| d.borrow_mut().insert(id, reply));
            w.wake();
        }
    }
}
