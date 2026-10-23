//! RFC-0054's head-count receiver. Increments day-grained tallies and keeps nothing else: no IP,
//! no header, no body after the increment, no time finer than the UTC day. See README.md.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Deserialize;

const MAX_BODY: usize = 1024;
const MAX_KEYS_PER_DAY: usize = 512;

/// day -> key -> tally
pub type Store = BTreeMap<String, BTreeMap<String, u64>>;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Ping {
    v: u8,
    event: Event,
    version: String,
    os: String,
    arch: String,
    chain: Option<u64>,
    source: Option<Source>,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq)]
#[serde(rename_all = "lowercase")]
enum Event {
    Counted,
    Init,
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum Source {
    Addresses,
    From,
    Subgraph,
}

impl Source {
    fn name(self) -> &'static str {
        match self {
            Source::Addresses => "addresses",
            Source::From => "from",
            Source::Subgraph => "subgraph",
        }
    }
}

fn token(s: &str, max: usize, extra: &str) -> bool {
    !s.is_empty()
        && s.len() <= max
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || extra.contains(c))
}

/// The keys a valid ping increments, or `None` when it is not a v1 payload.
pub fn keys_for(body: &[u8]) -> Option<Vec<String>> {
    let p: Ping = serde_json::from_slice(body).ok()?;
    if p.v != 1
        || !token(&p.version, 40, ".-+")
        || !token(&p.os, 16, "_")
        || !token(&p.arch, 16, "_")
    {
        return None;
    }
    Some(match p.event {
        Event::Counted => vec!["counted".into()],
        Event::Init => {
            let (chain, source) = (p.chain?, p.source?);
            vec![
                "init".into(),
                format!("init.version.{}", p.version),
                format!("init.os_arch.{}/{}", p.os, p.arch),
                format!("init.chain.{chain}"),
                format!("init.source.{}", source.name()),
            ]
        }
    })
}

pub fn increment(store: &mut Store, day: &str, keys: &[String]) {
    let tallies = store.entry(day.to_string()).or_default();
    for key in keys {
        let key = if tallies.contains_key(key) || tallies.len() < MAX_KEYS_PER_DAY {
            key.clone()
        } else {
            // Fold to one key per field: a version holds dots, so cutting at the last one made a new key each time.
            let mut parts = key.splitn(3, '.');
            match (parts.next(), parts.next(), parts.next()) {
                (Some(event), Some(field), Some(_)) => format!("{event}.{field}.other"),
                _ => key.clone(),
            }
        };
        *tallies.entry(key).or_default() += 1;
    }
}

pub fn utc_day(unix_secs: u64) -> String {
    let z = (unix_secs / 86_400) as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}-{m:02}-{d:02}")
}

fn today() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    utc_day(secs)
}

fn save(store: &Store, path: &Path) -> std::io::Result<()> {
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(store)?)?;
    std::fs::rename(tmp, path)
}

fn load(path: &Path) -> Store {
    std::fs::read(path)
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_default()
}

/// Reads the request line, `Content-Length` and the body. Every other header is read past and
/// dropped where it stands.
fn read_request(stream: &mut TcpStream) -> Option<(String, String, Vec<u8>)> {
    let mut reader = BufReader::new(stream.try_clone().ok()?);
    let mut line = String::new();
    reader.read_line(&mut line).ok()?;
    let mut parts = line.split_whitespace();
    let (method, path) = (parts.next()?.to_string(), parts.next()?.to_string());
    let mut length = 0usize;
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header).ok()? == 0 || header.len() > 8192 {
            return None;
        }
        let header = header.trim_end();
        if header.is_empty() {
            break;
        }
        if let Some((name, value)) = header.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                length = value.trim().parse().ok()?;
            }
        }
    }
    if length > MAX_BODY {
        return None;
    }
    let mut body = vec![0; length];
    reader.read_exact(&mut body).ok()?;
    Some((method, path, body))
}

/// Any origin may read: the totals are public by design, and the site reads them from the browser.
fn respond(stream: &mut TcpStream, status: &str, body: &[u8], kind: &str) {
    let _ = write!(
        stream,
        "HTTP/1.1 {status}\r\ncontent-type: {kind}\r\naccess-control-allow-origin: *\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(body);
}

fn handle(mut stream: TcpStream, store: &Mutex<Store>, path: &Path) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let Some((method, target, body)) = read_request(&mut stream) else {
        return respond(&mut stream, "400 Bad Request", b"", "text/plain");
    };
    match (method.as_str(), target.as_str()) {
        ("POST", "/") => match keys_for(&body) {
            Some(keys) => {
                let mut store = store.lock().unwrap_or_else(|e| e.into_inner());
                increment(&mut store, &today(), &keys);
                if let Err(e) = save(&store, path) {
                    eprintln!("could not save tallies: {e}");
                }
                respond(&mut stream, "204 No Content", b"", "text/plain");
            }
            None => respond(&mut stream, "400 Bad Request", b"", "text/plain"),
        },
        ("GET", "/totals") => {
            let json = serde_json::to_vec_pretty(&*store.lock().unwrap_or_else(|e| e.into_inner()))
                .unwrap_or_default();
            respond(&mut stream, "200 OK", &json, "application/json");
        }
        _ => respond(&mut stream, "404 Not Found", b"", "text/plain"),
    }
}

fn arg(name: &str) -> Option<String> {
    let args: Vec<String> = std::env::args().collect();
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}

fn main() {
    let listen = arg("--listen").unwrap_or_else(|| "127.0.0.1:8290".into());
    let path = PathBuf::from(arg("--store").unwrap_or_else(|| "tallies.json".into()));
    let store = Arc::new(Mutex::new(load(&path)));
    let listener =
        TcpListener::bind(&listen).unwrap_or_else(|e| panic!("cannot bind {listen}: {e}"));
    eprintln!(
        "nuthatch-count-receiver listening on {listen}, store {}",
        path.display()
    );
    for stream in listener.incoming().flatten() {
        let (store, path) = (store.clone(), path.clone());
        std::thread::spawn(move || handle(stream, &store, &path));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const INIT: &str = r#"{"v":1,"event":"init","version":"4.14.0","os":"linux","arch":"x86_64","chain":42161,"source":"addresses"}"#;

    #[test]
    fn invented_versions_fold_to_one_key_past_the_cap() {
        let mut store = Store::new();
        for i in 0..(MAX_KEYS_PER_DAY * 4) {
            let ping = INIT.replace("4.14.0", &format!("x{i}.0"));
            increment(
                &mut store,
                "2026-10-23",
                &keys_for(ping.as_bytes()).unwrap(),
            );
        }
        let day = &store["2026-10-23"];
        assert!(day.len() <= MAX_KEYS_PER_DAY + 4, "{} keys", day.len());
        assert!(day.contains_key("init.version.other"));
    }

    #[test]
    fn an_init_increments_its_five_tallies() {
        assert_eq!(
            keys_for(INIT.as_bytes()).unwrap(),
            [
                "init",
                "init.version.4.14.0",
                "init.os_arch.linux/x86_64",
                "init.chain.42161",
                "init.source.addresses"
            ]
        );
    }

    #[test]
    fn a_counted_increments_only_its_total_whatever_else_it_carries() {
        let bare = r#"{"v":1,"event":"counted","version":"4.14.0","os":"macos","arch":"aarch64"}"#;
        let full = r#"{"v":1,"event":"counted","version":"4.14.0","os":"macos","arch":"aarch64","chain":1,"source":"from"}"#;
        assert_eq!(keys_for(bare.as_bytes()).unwrap(), ["counted"]);
        assert_eq!(keys_for(full.as_bytes()).unwrap(), ["counted"]);
    }

    #[test]
    fn anything_but_a_v1_payload_is_refused() {
        for bad in [
            INIT.replace(r#""v":1"#, r#""v":2"#),
            INIT.replace(r#""chain":42161,"#, ""),
            INIT.replace("addresses", "elsewhere"),
            INIT.replace("linux", "Linux"),
            INIT.replace("linux", "linux\nx"),
            INIT.replace("4.14.0", "4.14.0 <script>"),
            INIT.replace('}', r#","host":"me"}"#),
            "not json".into(),
        ] {
            assert!(keys_for(bad.as_bytes()).is_none(), "{bad}");
        }
    }

    #[test]
    fn a_flood_of_invented_keys_cannot_grow_a_day_past_the_cap() {
        let mut store = Store::new();
        for chain in 0..10_000u64 {
            increment(&mut store, "2026-10-08", &[format!("init.chain.{chain}")]);
        }
        let before = store["2026-10-08"].len();
        assert_eq!(before, MAX_KEYS_PER_DAY + 1);
        assert_eq!(
            store["2026-10-08"]["init.chain.other"],
            10_000 - MAX_KEYS_PER_DAY as u64
        );
        increment(&mut store, "2026-10-08", &["init.chain.7".into()]);
        assert_eq!(store["2026-10-08"].len(), before);
        assert_eq!(store["2026-10-08"]["init.chain.7"], 2);
    }

    #[test]
    fn days_are_utc_calendar_days() {
        assert_eq!(utc_day(0), "1970-01-01");
        assert_eq!(utc_day(951_782_400), "2000-02-29");
        assert_eq!(utc_day(1_791_417_599), "2026-10-07");
        assert_eq!(utc_day(1_791_417_600), "2026-10-08");
    }

    // A5: a thousand pings from a thousand addresses leave a store whose size is the distinct
    // (day, key) pairs alone, with no value that varies with who sent them.
    #[test]
    fn the_store_holds_tallies_and_nothing_about_the_sender() {
        let dir = std::env::temp_dir().join(format!("count-receiver-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("tallies.json");
        let store = Arc::new(Mutex::new(Store::new()));
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (s, p) = (store.clone(), path.clone());
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                handle(stream, &s, &p);
            }
        });
        for i in 0..1000u32 {
            let mut c = TcpStream::connect(addr).unwrap();
            write!(
                c,
                "POST / HTTP/1.1\r\nhost: count\r\nx-forwarded-for: 10.{}.{}.{}\r\nuser-agent: nuthatch-count/1\r\ncontent-length: {}\r\n\r\n{INIT}",
                i >> 16 & 255, i >> 8 & 255, i & 255, INIT.len()
            )
            .unwrap();
            let mut reply = String::new();
            c.read_to_string(&mut reply).unwrap();
            assert!(reply.starts_with("HTTP/1.1 204"), "{reply}");
        }
        let mut c = TcpStream::connect(addr).unwrap();
        write!(c, "GET /totals HTTP/1.1\r\nhost: count\r\n\r\n").unwrap();
        let mut reply = String::new();
        c.read_to_string(&mut reply).unwrap();
        assert!(reply.contains("access-control-allow-origin: *"), "{reply}");
        assert!(reply.contains("\"init\": 1000"), "{reply}");
        let saved = std::fs::read_to_string(&path).unwrap();
        assert!(!saved.contains("10."), "{saved}");
        assert!(
            !saved.contains("forwarded") && !saved.contains("user-agent"),
            "{saved}"
        );
        let saved: Store = serde_json::from_str(&saved).unwrap();
        let day = saved.values().next().unwrap();
        assert_eq!(day.len(), 5);
        assert!(day.values().all(|&n| n == 1000));
        std::fs::remove_dir_all(dir).unwrap();
    }
}
