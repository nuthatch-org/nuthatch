//! The RFC-0016 Tier-B runner and the RFC-0017 authoring runner, and what they share.
//!
//! A standalone crate, outside the root package, so nothing here reaches the shipped binary or the
//! default build. Reports are written byte-for-byte as the Python runners they replace wrote them.

pub mod authoring;
pub mod proc;
pub mod pyjson;
pub mod tier_b;

use std::path::PathBuf;

/// The repository root: the runners read `eval/` and `skills/` from the tree they were built in.
pub fn root() -> PathBuf {
    PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
        .canonicalize()
        .expect("the repository root exists")
}

/// The self-tests' `ok`/`FAIL` lines, with the names CI checks for.
#[derive(Default)]
pub struct Checks {
    pub results: Vec<(String, bool)>,
}

impl Checks {
    pub fn check(&mut self, name: &str, ok: bool, detail: &str) {
        if ok {
            println!("  ok   {name}");
        } else {
            println!("  FAIL {name} - {detail}");
        }
        self.results.push((name.to_string(), ok));
    }

    pub fn passed(&self) -> bool {
        self.results.iter().all(|(_, ok)| *ok)
    }

    pub fn finish(&self) -> bool {
        let failed: Vec<&str> = self
            .results
            .iter()
            .filter(|(_, ok)| !ok)
            .map(|(n, _)| n.as_str())
            .collect();
        if failed.is_empty() {
            println!("self-test: PASS");
        } else {
            println!("self-test: FAIL ({})", failed.join(", "));
        }
        failed.is_empty()
    }
}

/// Python's `float(str)`: surrounding whitespace and digit-separating underscores are accepted.
pub fn parse_py_float(s: &str) -> Option<f64> {
    let t = s.trim();
    if t.contains('_') {
        let b = t.as_bytes();
        for (i, &c) in b.iter().enumerate() {
            if c == b'_'
                && !(i > 0
                    && i + 1 < b.len()
                    && b[i - 1].is_ascii_digit()
                    && b[i + 1].is_ascii_digit())
            {
                return None;
            }
        }
        return t.replace('_', "").parse().ok();
    }
    t.parse().ok()
}

/// `urllib.parse.quote_plus`, which is what `urlencode` applies to each value.
pub fn quote_plus(s: &str) -> String {
    let mut out = String::new();
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'.' | b'-' | b'~' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// `str.format` with named fields only, which is all the templates use.
pub fn py_format(template: &str, fields: &[(&str, String)]) -> Result<String, String> {
    let mut out = String::new();
    let mut chars = template.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '{' if chars.peek() == Some(&'{') => {
                chars.next();
                out.push('{');
            }
            '}' if chars.peek() == Some(&'}') => {
                chars.next();
                out.push('}');
            }
            '{' => {
                let mut name = String::new();
                loop {
                    match chars.next() {
                        Some('}') => break,
                        Some(ch) => name.push(ch),
                        None => return Err("unclosed '{' in template".into()),
                    }
                }
                match fields.iter().find(|(k, _)| *k == name) {
                    Some((_, v)) => out.push_str(v),
                    None => return Err(format!("KeyError: '{name}'")),
                }
            }
            '}' => return Err("single '}' encountered in format string".into()),
            other => out.push(other),
        }
    }
    Ok(out)
}

/// The first `n` characters, as Python's `s[:n]`.
pub fn head(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}

/// The last `n` characters, as Python's `s[-n:]`.
pub fn tail(s: &str, n: usize) -> String {
    let count = s.chars().count();
    s.chars().skip(count.saturating_sub(n)).collect()
}

/// A Python `repr` of a list of strings, for messages that printed one.
pub fn py_list_repr(items: &[String]) -> String {
    let inner: Vec<String> = items
        .iter()
        .map(|s| format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'")))
        .collect();
    format!("[{}]", inner.join(", "))
}

/// `statistics.median`.
pub fn median(values: &[f64]) -> f64 {
    let mut v = values.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).expect("no NaN in a median"));
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

/// `date +%Y-%m-%d`, local or UTC, as the two runners each recorded it.
pub fn today(utc: bool) -> String {
    let mut cmd = std::process::Command::new("date");
    if utc {
        cmd.arg("-u");
    }
    let out = cmd.arg("+%Y-%m-%d").output().expect("run date");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn helpers_match_python() {
        assert_eq!(parse_py_float(" 3600 "), Some(3600.0));
        assert_eq!(parse_py_float("1_000"), Some(1000.0));
        assert_eq!(parse_py_float("1__0"), None);
        assert_eq!(parse_py_float("abc"), None);
        assert_eq!(quote_plus("select 1 \"x\"/é"), "select+1+%22x%22%2F%C3%A9");
        assert_eq!(
            py_format("{{a}} {b}", &[("b", "x".into())]).unwrap(),
            "{a} x"
        );
        assert!(py_format("{c}", &[]).is_err());
        assert_eq!(median(&[1.0, 0.0, 1.0]), 1.0);
        assert_eq!(median(&[1.0, 2.0]), 1.5);
        assert_eq!(py_list_repr(&["a".into(), "b".into()]), "['a', 'b']");
    }
}
