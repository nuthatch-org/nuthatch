//! RFC-0069 §5, G1: the last requests a nest served, for `GET /_admin/requests`.
//!
//! What is kept is the shape of a request and nothing a caller sent: the route template, the name
//! the template captures when it names a declared query or entity, status, time and size. No SQL
//! text, no query string, no client address, no path value that is data (an address, a key, an id).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use serde::Serialize;

pub const CAPACITY: usize = 256;

/// The template parameters whose values are names the nest's author declared, so they say which
/// query or table ran without saying what anyone asked of it.
const NAME_PARAMS: [&str; 2] = ["{name}", "{entity}"];

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Served {
    pub seq: u64,
    pub unix_ms: u64,
    pub route: Arc<str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub status: u16,
    pub ms: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
}

#[derive(Default)]
pub struct RequestRing {
    inner: Mutex<(u64, VecDeque<Served>)>,
}

impl RequestRing {
    /// Keep one served request, dropping the oldest past [`CAPACITY`].
    pub fn push(
        &self,
        unix_ms: u64,
        route: Arc<str>,
        name: Option<String>,
        status: u16,
        ms: f64,
        bytes: Option<u64>,
    ) {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let (next, ring) = &mut *guard;
        *next += 1;
        if ring.len() == CAPACITY {
            ring.pop_front();
        }
        ring.push_back(Served {
            seq: *next,
            unix_ms,
            route,
            name,
            status,
            ms,
            bytes,
        });
    }

    /// Everything after `since`, oldest first, and the sequence number of the newest kept.
    pub fn since(&self, since: u64) -> (Vec<Served>, u64) {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let (next, ring) = &*guard;
        (
            ring.iter().filter(|s| s.seq > since).cloned().collect(),
            *next,
        )
    }
}

/// Requests a monitor makes of a nest rather than a caller's: kept out of the ring so a watcher
/// reading `/ready` every second does not push every real request out of it.
pub fn is_monitoring(route: &str) -> bool {
    matches!(route, "/" | "/health" | "/ready" | "/metrics") || route.starts_with("/_admin")
}

/// The route template as the nest itself serves it, with a runtime's mount prefix taken off: the
/// matched template's last segments, as many as the path the nest's own router saw.
pub fn local_template<'a>(matched: &'a str, local_path: &str) -> &'a str {
    let depth = local_path.split('/').filter(|s| !s.is_empty()).count();
    if depth == 0 {
        return "/";
    }
    let mut cut = matched.len();
    for _ in 0..depth {
        match matched[..cut].rfind('/') {
            Some(at) => cut = at,
            None => return matched,
        }
    }
    &matched[cut..]
}

/// The value a declared-name parameter (`{name}`, `{entity}`) took in `path`, if the template has
/// one.
pub fn captured_name(template: &str, path: &str) -> Option<String> {
    let t: Vec<&str> = template.split('/').filter(|s| !s.is_empty()).collect();
    let p: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    if t.len() != p.len() {
        return None;
    }
    t.iter()
        .zip(&p)
        .find(|(t, _)| NAME_PARAMS.contains(t))
        .map(|(_, p)| percent_decode(p))
}

fn percent_decode(s: &str) -> String {
    let hex = |b: u8| (b as char).to_digit(16).map(|d| d as u8);
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(hi), Some(lo)) = (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                out.push(hi << 4 | lo);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_ring_keeps_the_newest_and_answers_after_a_sequence_number() {
        let ring = RequestRing::default();
        for i in 0..(CAPACITY as u64 + 10) {
            ring.push(i, Arc::from("/sql"), None, 200, 1.0, Some(10));
        }
        let (all, next) = ring.since(0);
        assert_eq!(all.len(), CAPACITY);
        assert_eq!(next, CAPACITY as u64 + 10);
        assert_eq!(all[0].seq, 11);
        let (tail, _) = ring.since(next - 3);
        assert_eq!(
            tail.iter().map(|s| s.seq).collect::<Vec<_>>(),
            vec![264, 265, 266]
        );
    }

    #[test]
    fn a_runtime_prefix_comes_off_the_template() {
        assert_eq!(local_template("/pulse/q/{name}", "/q/top"), "/q/{name}");
        assert_eq!(local_template("/q/{name}", "/q/top"), "/q/{name}");
        assert_eq!(local_template("/acme/pulse/sql", "/sql"), "/sql");
        assert_eq!(local_template("/pulse", "/"), "/");
    }

    #[test]
    fn only_declared_names_are_captured() {
        assert_eq!(
            captured_name("/q/{name}", "/q/top%20holders"),
            Some("top holders".into())
        );
        assert_eq!(
            captured_name("/derived/{entity}", "/derived/pools"),
            Some("pools".into())
        );
        assert_eq!(
            captured_name("/derived/{entity}/{key}", "/derived/pools/0xabc"),
            Some("pools".into())
        );
        assert_eq!(captured_name("/balance/{address}", "/balance/0xabc"), None);
        assert_eq!(captured_name("/entity/{id}", "/entity/000001-000002"), None);
        assert_eq!(captured_name("/sql", "/sql"), None);
    }
}
