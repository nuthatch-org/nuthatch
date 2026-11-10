//! The audit surface (RFC-0008 C6). `audit report` summarises the threshold flags in a range with
//! their block bounds, for a human record; `audit sealed` lives in `sealed_audit`.

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use std::path::Path;

/// `nuthatch audit report --from --to` - a summary of the flags in a range and their block bounds.
/// Returned as JSON; the CLI can render markdown from it. `flags_on` is whether the nest's config
/// sets a threshold: without one, no count it could give would mean anything.
pub fn report(dir: &Path, flags_on: bool, from: u64, to: u64) -> Result<Value> {
    if !flags_on {
        bail!("this nest raises no threshold flags: nuthatch.toml sets no [flags] threshold");
    }
    // The table exists only once a flag has been sealed, so its absence is a true zero.
    let probe = crate::analytics::query(
        dir,
        "SELECT count(*) AS n FROM information_schema.tables WHERE table_name = 'threshold_flag'",
    )
    .context("looking for threshold_flag")?;
    let sealed = probe
        .first()
        .and_then(|r| r.get("n"))
        .and_then(Value::as_u64)
        .unwrap_or(0)
        > 0;
    let flags = if sealed {
        crate::analytics::query(
            dir,
            &format!(
                "SELECT count(*) AS n, min(block_number) AS lo, max(block_number) AS hi \
                 FROM threshold_flag WHERE block_number BETWEEN {from} AND {to}"
            ),
        )
        .context("counting threshold_flag")?
    } else {
        Vec::new()
    };

    let flag_row = flags.first().cloned().unwrap_or(json!({}));
    Ok(json!({
        "range": { "from": from, "to": to },
        "threshold_flags": {
            "count": flag_row.get("n").cloned().unwrap_or(json!(0)),
            "block_bounds": [flag_row.get("lo").cloned().unwrap_or(json!(null)), flag_row.get("hi").cloned().unwrap_or(json!(null))],
        },
    }))
}

/// Render a report JSON as a compact markdown record for a human audit file.
pub fn report_markdown(r: &Value) -> String {
    let tf = &r["threshold_flags"];
    format!(
        "# Compliance audit report\n\n\
         - Range: blocks {}-{}\n\
         - Threshold flags: {}\n",
        r["range"]["from"], r["range"]["to"], tf["count"],
    )
}

/// CLI entry: report/sealed dispatch.
pub async fn run(args: crate::cli::AuditArgs) -> Result<()> {
    use std::path::PathBuf;
    match args.what {
        crate::cli::AuditWhat::Sealed(a) => crate::sealed_audit::run_cli(a).await,
        crate::cli::AuditWhat::Report(a) => {
            let dir = PathBuf::from(&a.dir);
            let flags_on = crate::config::Config::load(&dir)?
                .flags
                .threshold_amount()
                .is_some();
            let r = report(&dir, flags_on, a.from, a.to)?;
            if a.json {
                println!("{}", serde_json::to_string_pretty(&r)?);
            } else {
                print!("{}", report_markdown(&r));
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_counts_sealed_flags_in_range_only() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        let flag = |block: u64| {
            format!(
                r#"{{"table":"threshold_flag","kind":"threshold_flag","address":"0xaa","value":"500","block_number":{block},"log_index":0}}"#
            )
        };
        crate::seal::seal_range(d, &[flag(10), flag(12), flag(30)], 10, 30).unwrap();

        let rep = report(d, true, 10, 20).unwrap();
        assert_eq!(rep["threshold_flags"]["count"], Value::from(2u64));
        assert_eq!(
            rep["threshold_flags"]["block_bounds"][0],
            Value::from(10u64)
        );
        assert_eq!(
            rep["threshold_flags"]["block_bounds"][1],
            Value::from(12u64)
        );
        assert!(report_markdown(&rep).contains("Threshold flags: 2"));
    }

    /// #2719: an unreadable catalogue is an error, not zero flags, and so is a nest without flags.
    #[test]
    fn a_report_that_cannot_be_answered_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        let other = r#"{"table":"t__transfer","from":"0xaa","to":"0xbb","value":"1","block_number":10,"log_index":0}"#;
        crate::seal::seal_range(d, &[other.to_string()], 10, 20).unwrap();

        let none_yet = report(d, true, 10, 20).unwrap();
        assert_eq!(none_yet["threshold_flags"]["count"], Value::from(0u64));
        let off = report(d, false, 10, 20).unwrap_err();
        assert!(format!("{off:#}").contains("no threshold flags"), "{off:#}");

        let flag = r#"{"table":"threshold_flag","kind":"threshold_flag","address":"0xaa","value":"500","block_number":12,"log_index":0}"#;
        crate::seal::seal_range(d, &[flag.to_string()], 21, 30).unwrap();
        let manifest = d
            .join(crate::seal::SEGMENTS_DIR)
            .join(crate::seal::MANIFEST_FILE);
        std::fs::write(&manifest, b"not a manifest").unwrap();
        let broken = report(d, true, 10, 30).unwrap_err();
        assert!(
            format!("{broken:#}").contains("corrupt segments manifest"),
            "{broken:#}"
        );
    }
}
