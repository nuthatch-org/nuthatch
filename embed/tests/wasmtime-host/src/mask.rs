//! `embed/mask.mjs`, ported: the keys `tests/e2e_tab_answers.rs` masks, replaced the same way, and the
//! one digest every host prints, so a line compares this host with Node, Chrome and the pin.

use serde_json::Value;
use sha2::{Digest, Sha256};

const MASKED: &[&str] = &[
    "fetch_window_blocks",
    "version",
    "last_poll_unixtime",
    "seconds_since_poll",
    "seconds_since_seal_progress",
    "seconds_since_progress",
    "nid",
];

pub fn mask(body: &str) -> String {
    let mut out = body.to_string();
    for key in MASKED {
        let needle = format!("\"{key}\":");
        let mut from = 0;
        while let Some(found) = out[from..].find(&needle) {
            let s = from + found + needle.len();
            let rest = &out[s..];
            let len = match rest.strip_prefix('"') {
                Some(quoted) => quoted.find('"').map_or(rest.len(), |i| i + 2),
                None => rest.find([',', '}']).unwrap_or(rest.len()),
            };
            out.replace_range(s..s + len, "\"<masked>\"");
            from = s;
        }
    }
    out
}

/// `answersDigest` in `mask.mjs`: `pinned` answers are masked already.
pub fn digest(answers: &[Value], pinned: bool) -> String {
    let mut h = Sha256::new();
    for a in answers {
        let body = a["body"].as_str().unwrap_or_default();
        let body = if pinned { body.to_string() } else { mask(body) };
        h.update(format!(
            "{} {}\n{}\n{}\n{body}\n",
            a["method"].as_str().unwrap_or_default(),
            a["path"].as_str().unwrap_or_default(),
            a["status"],
            a["content_type"].as_str().unwrap_or_default(),
        ));
    }
    format!("{:x}", h.finalize())
}

#[cfg(test)]
mod tests {
    use super::mask;

    #[test]
    fn masks_strings_numbers_and_every_occurrence_as_the_js_does() {
        assert_eq!(
            mask(
                r#"{"version":"5.14.1","nid":"x","seconds_since_poll":3,"a":{"seconds_since_progress":0}}"#
            ),
            r#"{"version":"<masked>","nid":"<masked>","seconds_since_poll":"<masked>","a":{"seconds_since_progress":"<masked>"}}"#
        );
        assert_eq!(
            mask(r#"{"fetch_window_blocks":20"#),
            r#"{"fetch_window_blocks":"<masked>""#
        );
    }
}
