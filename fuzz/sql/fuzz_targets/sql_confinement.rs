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
//! A bundle is the tenant's too, so an input may also carry A's `views/zz.sql`, built from the same
//! fragments, which the statement can then name as `z`.
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
        // libFuzzer's hook aborts before unwinding, which `catch_unwind` below could never see. A
        // failed assertion still aborts: libfuzzer-sys catches the unwind out of the target.
        let _ = std::panic::take_hook();
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
        ..Default::default()
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
    "DROP TABLE",
    "CREATE TABLE z AS",
    "SET",
    "PRAGMA",
    "EXPLAIN",
    "SHOW TABLES",
    "LATERAL",
    "t__e",
    "v",
    "z",
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
    base: u8,
    pieces: Vec<Piece>,
    /// The body of an authored `CREATE VIEW z AS …`, or its whole file when `raw`.
    view: Option<(bool, Vec<Piece>)>,
}

const VIEW_FILE: &str = "zz.sql";

/// Statements that already answer, so the fuzzer spends its time on what follows them rather than on
/// assembling a first `SELECT`. The empty one leaves the pieces on their own.
const BASES: &[&str] = &[
    "",
    "SELECT * FROM t__e",
    "SELECT x FROM v",
    "SELECT * FROM information_schema.tables",
    "WITH c AS (SELECT * FROM t__e) SELECT * FROM c",
    "SELECT * FROM t__e WHERE x IN (SELECT x FROM t__e)",
];

fn render(f: &Fixture, input: &Input) -> String {
    let sql = format!("{} ", BASES[input.base as usize % BASES.len()]);
    render_pieces(f, sql, &input.pieces)
}

fn render_pieces(f: &Fixture, mut sql: String, pieces: &[Piece]) -> String {
    for piece in pieces {
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
    let view = input.view.as_ref().map(|(raw, pieces)| {
        let head = if *raw { "" } else { "CREATE VIEW z AS " };
        render_pieces(f, head.to_string(), pieces)
    });
    if sql.len() + view.as_ref().map_or(0, String::len) > 16 * 1024 {
        return;
    }
    let view_file = f.a.join("views").join(VIEW_FILE);
    match &view {
        Some(text) => std::fs::write(&view_file, text).unwrap(),
        None => {
            let _ = std::fs::remove_file(&view_file);
        }
    }
    let tenant = input.strict.then(|| Arc::<str>::from("a"));
    // `/sql` confines a panic to its request, so one is an outcome here, not a finding; the checks
    // below still run after it.
    let got = std::panic::catch_unwind(|| {
        isolated(tenant.as_ref(), || {
            query_hot_cold(&f.a, &sql, guard(), &HotRows::new(), 10, &[])
        })
    });
    let text = match &got {
        Ok(Ok(out)) => serde_json::to_string(&out.rows).unwrap(),
        Ok(Err(e)) => format!("{e:#}"),
        Err(_) => String::new(),
    };
    let spelled = sql.contains(&f.secret) || view.as_ref().is_some_and(|v| v.contains(&f.secret));
    if !spelled {
        assert!(
            !text.contains(&f.secret),
            "B's secret reached A ({}): {sql}\nview: {view:?}\n{text}",
            if input.strict { "strict" } else { "shared" }
        );
    }
    // Writing the view file moves its directory's mtime; that is the harness, not the statement.
    let authored = |p: &PathBuf| *p == view_file || Some(p.as_path()) == view_file.parent();
    let now: Vec<_> = files_under(&f.root)
        .into_iter()
        .filter(|(p, _, _)| !authored(p))
        .collect();
    let before: Vec<_> = f
        .files
        .iter()
        .filter(|(p, _, _)| !authored(p))
        .cloned()
        .collect();
    assert!(
        now == before,
        "a statement changed the runtime directory: {sql}\nview: {view:?}\nbefore {:?}\nafter {:?}",
        before,
        now
    );
});
