//! The `Host` rule every nest on a loopback bind applies (#3002). A reverse proxy and a page that
//! rebinds its DNS name to 127.0.0.1 both connect from loopback, so the socket cannot tell them apart;
//! the `Host` a browser sends is the page's own name, which it cannot choose.

use anyhow::{bail, Result};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use std::sync::Arc;

/// Host names the operator declared for a loopback bind (`--local-host`, `--public-host`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HostNames {
    /// Answered as this machine, as `localhost` is: a Docker host alias, say.
    pub local: Vec<String>,
    /// Answered as the public, behind the operator's proxy.
    pub public: Vec<String>,
}

/// What the `Host` of an admitted request named.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Seen {
    /// A loopback or declared local name, or no `Host` at all.
    Local,
    /// A declared public name.
    Public(String),
}

impl HostNames {
    /// The names given to `--local-host` and `--public-host`, refused when one could not do what it
    /// says: a loopback name is always this machine, so it cannot be limited to reads.
    pub fn declared(local: &[String], public: &[String]) -> Result<Self> {
        let name = |flag: &str, raw: &String| -> Result<String> {
            match authority_host(raw) {
                Some(n)
                    if raw
                        .trim_start_matches('[')
                        .trim_end_matches(']')
                        .eq_ignore_ascii_case(&n) =>
                {
                    Ok(n)
                }
                _ => bail!("{flag} {raw:?} is not a host name; give the name alone, with no port"),
            }
        };
        let local = local
            .iter()
            .map(|h| name("--local-host", h))
            .collect::<Result<Vec<_>>>()?;
        let public = public
            .iter()
            .map(|h| name("--public-host", h))
            .collect::<Result<Vec<_>>>()?;
        for p in &public {
            if is_loopback_name(p) {
                bail!(
                    "--public-host {p} would do nothing: a loopback name is always answered as this \
                     machine. Point the proxy at a name of its own, or have it add X-Forwarded-For"
                );
            }
            if local.contains(p) {
                bail!("{p} is given to both --local-host and --public-host; a name is one or the other");
            }
        }
        Ok(Self { local, public })
    }

    pub fn is_empty(&self) -> bool {
        self.local.is_empty() && self.public.is_empty()
    }

    fn is_local(&self, name: &str) -> bool {
        is_loopback_name(name) || declared(&self.local, name)
    }

    /// Judge a request to a loopback bind by its `Host`: refused, with why, unless it names loopback
    /// or a declared name.
    pub fn check(&self, headers: &HeaderMap) -> Result<Seen, String> {
        let mut all = headers.get_all(axum::http::header::HOST).iter();
        let host = all.next();
        if all.next().is_some() {
            return Err("this request carries two Host headers, and is refused".into());
        }
        let Some(raw) = host else {
            return Ok(Seen::Local);
        };
        let raw = String::from_utf8_lossy(raw.as_bytes()).into_owned();
        let Some(name) = authority_host(&raw) else {
            return Err(format!(
                "Host {raw:?} is not a host name and port, and is refused"
            ));
        };
        if self.is_local(&name) {
            return Ok(Seen::Local);
        }
        if declared(&self.public, &name) {
            return Ok(Seen::Public(name));
        }
        Err(format!(
            "this nest is bound to loopback and does not answer Host {name}: start it with \
             --public-host {name} to serve reads to callers behind a proxy, or --local-host \
             {name} if that name is this machine"
        ))
    }

    /// Whether an `Origin` names this machine, by the same names [`check`](Self::check) answers.
    pub fn is_local_origin(&self, origin: &str) -> bool {
        origin
            .split_once("://")
            .and_then(|(_, rest)| authority_host(rest))
            .is_some_and(|name| self.is_local(&name))
    }
}

fn declared(list: &[String], name: &str) -> bool {
    list.iter().any(|h| h.eq_ignore_ascii_case(name))
}

/// The host of an authority that is `host[:digits]` and nothing else, lowercased.
fn authority_host(authority: &str) -> Option<String> {
    let (name, rest) = match authority.strip_prefix('[') {
        Some(v6) => {
            let (v6, rest) = v6.split_once(']')?;
            v6.parse::<std::net::Ipv6Addr>().ok()?;
            (v6, rest)
        }
        None => authority
            .find(':')
            .map_or((authority, ""), |i| (&authority[..i], &authority[i..])),
    };
    let port_ok = rest.is_empty()
        || rest
            .strip_prefix(':')
            .is_some_and(|p| (1..=5).contains(&p.len()) && p.bytes().all(|b| b.is_ascii_digit()));
    let name_ok = !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_' | b':'));
    (port_ok && name_ok).then(|| name.to_ascii_lowercase())
}

fn is_loopback_name(name: &str) -> bool {
    name == "localhost"
        || name
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback() || ip.is_unspecified())
}

/// `app` behind the `Host` rule on every route when `loopback`; off loopback the operator exposed the
/// nest to whoever can reach the address, and every `Host` is answered.
pub fn guard(app: axum::Router, loopback: bool, names: HostNames) -> axum::Router {
    if !loopback {
        if !names.is_empty() {
            tracing::warn!(
                "--public-host and --local-host do nothing off a loopback bind: every Host is answered"
            );
        }
        return app;
    }
    let names = Arc::new(names);
    app.layer(axum::middleware::from_fn(
        move |req: axum::extract::Request, next: axum::middleware::Next| {
            let names = names.clone();
            async move {
                match names.check(req.headers()) {
                    Ok(_) => next.run(req).await,
                    Err(why) => (
                        StatusCode::FORBIDDEN,
                        axum::Json(serde_json::json!({ "error": why })),
                    )
                        .into_response(),
                }
            }
        },
    ))
}
