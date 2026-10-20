//! `json.dumps` as Python writes it, so reports and details keep their bytes.
//!
//! Python's defaults differ from serde_json's in three ways that reach the output: `ensure_ascii`,
//! `", "`/`": "` separators in compact mode, and float `repr` (`1e+16`, `1.5e-05`).

use serde_json::{Number, Value};

pub enum Style {
    /// `json.dumps(v)`: `", "` and `": "`.
    Default,
    /// `json.dumps(v, separators=(",", ":"))`.
    Compact,
    /// `json.dumps(v, indent=2)`.
    Indent2,
}

pub fn dumps(value: &Value, style: Style) -> String {
    let mut out = String::new();
    write(&mut out, value, &style, 0);
    out
}

fn write(out: &mut String, value: &Value, style: &Style, depth: usize) {
    let (item_sep, key_sep) = match style {
        Style::Default => (", ", ": "),
        Style::Compact => (",", ":"),
        Style::Indent2 => (",", ": "),
    };
    let newline = |out: &mut String, depth: usize| {
        if let Style::Indent2 = style {
            out.push('\n');
            out.push_str(&"  ".repeat(depth));
        }
    };
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => out.push_str(&number(n)),
        Value::String(s) => string(out, s),
        Value::Array(items) => {
            if items.is_empty() {
                out.push_str("[]");
                return;
            }
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push_str(item_sep);
                }
                newline(out, depth + 1);
                write(out, item, style, depth + 1);
            }
            newline(out, depth);
            out.push(']');
        }
        Value::Object(map) => {
            if map.is_empty() {
                out.push_str("{}");
                return;
            }
            out.push('{');
            for (i, (k, v)) in map.iter().enumerate() {
                if i > 0 {
                    out.push_str(item_sep);
                }
                newline(out, depth + 1);
                string(out, k);
                out.push_str(key_sep);
                write(out, v, style, depth + 1);
            }
            newline(out, depth);
            out.push('}');
        }
    }
}

/// An integer stays exact, as a Python `int` does; anything else is a Python `float`.
fn number(n: &Number) -> String {
    let raw = n.to_string();
    if !raw.contains(['.', 'e', 'E']) {
        return raw.trim_start_matches('+').to_string();
    }
    float_repr(raw.parse().expect("a JSON number parses as f64"))
}

/// Python's `repr(float)`: shortest round-trip digits, fixed between 1e-4 and 1e16.
pub fn float_repr(x: f64) -> String {
    if x.is_nan() {
        return "NaN".into();
    }
    if x.is_infinite() {
        return if x > 0.0 { "Infinity" } else { "-Infinity" }.into();
    }
    let sci = format!("{x:e}");
    let (mantissa, exp) = sci.split_once('e').expect("{:e} has an exponent");
    let exp: i32 = exp.parse().expect("exponent");
    let (sign, mantissa) = match mantissa.strip_prefix('-') {
        Some(m) => ("-", m),
        None => ("", mantissa),
    };
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    if (-4..16).contains(&exp) {
        let point = exp + 1;
        let body = if point <= 0 {
            format!("0.{}{}", "0".repeat((-point) as usize), digits)
        } else if point as usize >= digits.len() {
            format!("{}{}.0", digits, "0".repeat(point as usize - digits.len()))
        } else {
            let (a, b) = digits.split_at(point as usize);
            format!("{a}.{b}")
        };
        format!("{sign}{body}")
    } else {
        let (first, rest) = digits.split_at(1);
        let m = if rest.is_empty() {
            first.to_string()
        } else {
            format!("{first}.{rest}")
        };
        let e = if exp < 0 {
            format!("-{:02}", -exp)
        } else {
            format!("+{exp:02}")
        };
        format!("{sign}{m}e{e}")
    }
}

fn string(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 || (c as u32) > 0x7e => {
                let mut buf = [0u16; 2];
                for unit in c.encode_utf16(&mut buf) {
                    out.push_str(&format!("\\u{unit:04x}"));
                }
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// A float as a JSON value, for report fields Python computed as floats.
pub fn float(x: f64) -> Value {
    Value::Number(Number::from_f64(x).expect("a finite float"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floats_are_python_reprs() {
        for (x, want) in [
            (0.0, "0.0"),
            (1.0, "1.0"),
            (-0.0, "-0.0"),
            (3600.0, "3600.0"),
            (1.0 / 3.0, "0.3333333333333333"),
            (2.0 / 3.0, "0.6666666666666666"),
            (1.267, "1.267"),
            (1e16, "1e+16"),
            (1.5e-5, "1.5e-05"),
            (0.0001, "0.0001"),
            (123456789012345.6, "123456789012345.6"),
        ] {
            assert_eq!(float_repr(x), want, "{x}");
        }
    }

    #[test]
    fn dumps_matches_python() {
        let v: Value = serde_json::from_str(
            r#"{"a": [1, 2.50, "é\u0001"], "b": {}, "c": [], "d": 123456789012345678901234567890}"#,
        )
        .unwrap();
        assert_eq!(
            dumps(&v, Style::Default),
            r#"{"a": [1, 2.5, "\u00e9\u0001"], "b": {}, "c": [], "d": 123456789012345678901234567890}"#
        );
        assert_eq!(
            dumps(&v, Style::Compact),
            r#"{"a":[1,2.5,"\u00e9\u0001"],"b":{},"c":[],"d":123456789012345678901234567890}"#
        );
        assert_eq!(
            dumps(&v, Style::Indent2),
            "{\n  \"a\": [\n    1,\n    2.5,\n    \"\\u00e9\\u0001\"\n  ],\n  \"b\": {},\n  \"c\": [],\n  \"d\": 123456789012345678901234567890\n}"
        );
        assert_eq!(
            dumps(&Value::String("😀".into()), Style::Default),
            r#""\ud83d\ude00""#
        );
    }
}
