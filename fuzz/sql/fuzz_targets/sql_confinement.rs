#![no_main]

//! RFC-0073 FR8: `/sql`'s read-only enforcement and confinement under fuzzed statements.
//!
//! Two datasets sit side by side under one runtime directory, as a shared runtime lays them out.
//! B's sealed rows, hot rows and config carry a secret drawn at start-up; every statement runs on A,
//! shared and strict, through the same `query_hot_cold` `/sql` calls. A finding is any of:
//!
//! - an answer or an error carrying B's secret, when the statement did not spell it out itself;
//! - any file under the runtime directory created, removed or changed by a statement.
//!
//! An `Err` is always an acceptable outcome. Statements are built from fragments of every escape
//! this surface has had or been audited for (quoted names, replacement scans, file URLs, stacked
//! statements, the catalogue), so libFuzzer spends its time recombining them rather than finding
//! `SELECT` by mutation.

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use nuthatch::analytics::{isolated, query_hot_cold, HotRows, QueryGuard};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

struct Fixture {
    _root: tempfile::TempDir,
    root: PathBuf,
    a: PathBuf,
    secret: String,
    /// Every path a fragment may name, so a statement can aim at B.
    paths: Vec<String>,
    files: Vec<(PathBuf, u64, std::time::SystemTime)>,
}

fn files_under(root: &Path) -> Vec<(PathBuf, u64, std::time::SystemTime)> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for e in std::fs::read_dir(&dir).unwrap().flatten() {
            let p = e.path();
            let m = std::fs::symlink_metadata(&p).unwrap();
            if m.is_dir() {
                stack.push(p.clone());
            }
            out.push((p, m.len(), m.modified().unwrap()));
        }
    }
    out.sort();
    out
}

fn fixture() -> &'static Fixture {
    static F: OnceLock<Fixture> = OnceLock::new();
    F.get_or_init(|| {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let secret = format!("B{:x}{:x}", nanos, std::process::id());
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let a = root.join(nuthatch::runtime::DATA_DIR).join("aaaa");
        let b = root.join(nuthatch::runtime::DATA_DIR).join("bbbb");
        for (d, x) in [(&a, "mine"), (&b, secret.as_str())] {
            std::fs::create_dir_all(d.join("views")).unwrap();
            let row = serde_json::json!({"table": "t__e", "block_number": 10, "log_index": 0, "x": x});
            nuthatch::seal::seal_range(d, &[row.to_string()], 10, 10).unwrap();
        }
        std::fs::write(b.join("nuthatch.toml"), &secret).unwrap();
        std::fs::write(
            b.join("views").join("b.sql"),
            format!("CREATE VIEW b_only AS SELECT '{secret}' AS x FROM t__e;\n"),
        )
        .unwrap();
        std::fs::write(
            a.join("views").join("v.sql"),
            "CREATE VIEW v AS SELECT x FROM t__e;\n",
        )
        .unwrap();
        // B's session is cached first, holding its hot rows, so one reused across datasets shows.
        let mut hot = HotRows::new();
        hot.insert(
            "t__e".into(),
            vec![serde_json::json!({"table": "t__e", "block_number": 11, "log_index": 0, "x": secret})],
        );
        for tenant in [None, Some(Arc::<str>::from("a"))] {
            isolated(tenant.as_ref(), || {
                query_hot_cold(&b, "SELECT * FROM t__e", guard(), &hot, 10, &[]).unwrap()
            });
        }
        let mut paths = vec![
            b.display().to_string(),
            b.join("nuthatch.toml").display().to_string(),
            root.display().to_string(),
            "/etc/hosts".to_string(),
        ];
        for (p, _, _) in files_under(&root) {
            if p.extension().is_some_and(|x| x == "parquet") {
                paths.push(p.display().to_string());
            }
        }
        let files = files_under(&root);
        Fixture {
            _root: dir,
            root,
            a,
            secret,
            paths,
            files,
        }
    })
}

fn guard() -> QueryGuard {
    QueryGuard {
        timeout: Duration::from_secs(2),
        max_rows: 100,
    }
}

const WORDS: &[&str] = &[
    "SELECT",
    "WITH",
    "FROM",
    "JOIN",
    "WHERE",
    "UNION ALL",
    "AS",
    "ON",
    "TABLE",
    "VALUES",
    "COPY",
    "TO",
    "ATTACH",
    "INSERT INTO",
    "CREATE VIEW",
    "CREATE TABLE",
    "CREATE EXTERNAL TABLE",
    "STORED AS PARQUET LOCATION",
    "DROP VIEW",
    "SET",
    "PRAGMA",
    "EXPLAIN",
    "SHOW TABLES",
    "LATERAL",
    "t__e",
    "v",
    "b_only",
    "t__e__raw",
    "t__e__hot",
    "t__e__union",
    "bbbb",
    "aaaa",
    "main",
    "public",
    "datafusion",
    "information_schema",
    "tables",
    "columns",
    "views",
    "df_settings",
    "read_parquet",
    "read_csv",
    "read_text",
    "read_json_auto",
    "glob",
    "parquet_scan",
    "input_file_name",
    "version",
    "arrow_metadata",
    "range",
    "generate_series",
    "unnest",
    "list_transform",
    "x",
    "*",
    "count(*)",
    "1",
];

const PUNCT: &[&str] = &[
    "(",
    ")",
    ",",
    ";",
    ".",
    "\"",
    "'",
    "/**/",
    "--\n",
    "/*",
    "*/",
    "[",
    "]",
    " FOR q IN ",
    "->",
    "::",
    "=",
    "||",
];

#[derive(Arbitrary, Debug)]
enum Piece {
    Word(u8),
    Punct(u8),
    /// A path to B, the runtime, or the host, quoted one of three ways.
    Path(u8, u8),
    Text(String),
}

#[derive(Arbitrary, Debug)]
struct Input {
    strict: bool,
    pieces: Vec<Piece>,
}

fn render(f: &Fixture, input: &Input) -> String {
    let mut sql = String::new();
    for piece in &input.pieces {
        match piece {
            Piece::Word(i) => sql.push_str(WORDS[*i as usize % WORDS.len()]),
            Piece::Punct(i) => sql.push_str(PUNCT[*i as usize % PUNCT.len()]),
            Piece::Path(i, q) => {
                let p = &f.paths[*i as usize % f.paths.len()];
                match q % 3 {
                    0 => sql.push_str(&format!("'{p}'")),
                    1 => sql.push_str(&format!("\"{p}\"")),
                    _ => sql.push_str(&format!("'file://{p}'")),
                }
            }
            Piece::Text(t) => sql.push_str(t),
        }
        sql.push(' ');
    }
    sql
}

fuzz_target!(|input: Input| {
    let f = fixture();
    let sql = render(f, &input);
    if sql.len() > 16 * 1024 {
        return;
    }
    let tenant = input.strict.then(|| Arc::<str>::from("a"));
    let got = isolated(tenant.as_ref(), || {
        query_hot_cold(&f.a, &sql, guard(), &HotRows::new(), 10, &[])
    });
    let text = match &got {
        Ok(out) => serde_json::to_string(&out.rows).unwrap(),
        Err(e) => format!("{e:#}"),
    };
    if !sql.contains(&f.secret) {
        assert!(
            !text.contains(&f.secret),
            "B's secret reached A ({}): {sql}\n{text}",
            if input.strict { "strict" } else { "shared" }
        );
    }
    let now = files_under(&f.root);
    assert!(
        now == f.files,
        "a statement changed the runtime directory: {sql}\nbefore {:?}\nafter {:?}",
        f.files,
        now
    );
});
