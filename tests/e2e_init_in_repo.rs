//! `init` inside someone's repository (#3025): their `AGENTS.md` and `.gitignore` are kept and given
//! a delimited nuthatch section, the nest goes to its own directory, and `--json` names what was
//! skipped. Offline, like e2e_cli_json: `--abi`, `--chain` and `--start-block` leave nothing to fetch.

use std::path::Path;
use std::process::{Command, Stdio};

use serde_json::Value;

const WETH: &str = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2";
const ERC20: &str = r#"[{"type":"event","name":"Transfer","anonymous":false,"inputs":[
    {"name":"from","type":"address","indexed":true},
    {"name":"to","type":"address","indexed":true},
    {"name":"value","type":"uint256","indexed":false}]}]"#;
const THEIR_AGENTS: &str = "# My repo\n\nUse tabs.\n";
const THEIR_IGNORE: &str = "target/\n.env\n";

fn init(cwd: &Path, extra: &[&str]) -> (i32, Value, String) {
    let abi = cwd.join("erc20.json");
    std::fs::write(&abi, ERC20).unwrap();
    let abi = abi.to_string_lossy().into_owned();
    let mut args = vec![
        "init",
        WETH,
        "--chain",
        "mainnet",
        "--rpc",
        "http://127.0.0.1:1",
        "--abi",
        &abi,
        "--start-block",
        "100",
        "--alias",
        "weth",
        "--json",
    ];
    args.extend_from_slice(extra);
    let out = Command::new(env!("CARGO_BIN_EXE_nuthatch"))
        .args(&args)
        .current_dir(cwd)
        .stdin(Stdio::null())
        .output()
        .expect("run nuthatch");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let json = serde_json::from_str(stdout.trim()).unwrap_or(Value::Null);
    (
        out.status.code().unwrap_or(-1),
        json,
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

fn repo() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join(".git")).unwrap();
    std::fs::write(dir.path().join("AGENTS.md"), THEIR_AGENTS).unwrap();
    std::fs::write(dir.path().join(".gitignore"), THEIR_IGNORE).unwrap();
    dir
}

fn read(p: &Path) -> String {
    std::fs::read_to_string(p).unwrap()
}

fn strings(v: &Value) -> Vec<String> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|s| s.as_str().unwrap().to_string())
        .collect()
}

#[test]
fn a_repo_with_both_files_keeps_them_and_the_nest_goes_to_its_own_directory() {
    let dir = repo();
    let (code, v, stderr) = init(dir.path(), &[]);
    assert_eq!(code, 0, "{stderr}");
    assert_eq!(v["dir_defaulted"], true, "{v}");
    assert!(stderr.contains("./nest"), "says where it went: {stderr}");
    assert!(dir.path().join("nest/nuthatch.toml").exists());
    assert!(
        !dir.path().join("nuthatch.toml").exists(),
        "nothing at the root"
    );
    assert!(v["next"][0].as_str().unwrap().contains("--dir nest"), "{v}");

    let agents = read(&dir.path().join("AGENTS.md"));
    assert!(
        agents.starts_with(THEIR_AGENTS),
        "their content first and intact:\n{agents}"
    );
    assert!(
        agents.contains("nuthatch:begin") && agents.contains("nest/AGENTS.md"),
        "{agents}"
    );
    let ignore = read(&dir.path().join(".gitignore"));
    assert!(ignore.starts_with(THEIR_IGNORE), "{ignore}");
    for rule in ["nest/nuthatch.redb", "nest/segments/", "nest/.admin-token"] {
        assert!(
            ignore.lines().any(|l| l == rule),
            "{rule} missing:\n{ignore}"
        );
    }
    // The nest's own files are fresh, not appended to.
    assert!(read(&dir.path().join("nest/AGENTS.md")).starts_with("<!-- Written by nuthatch"));
    assert_eq!(strings(&v["files_appended"]).len(), 2, "{v}");
}

#[test]
fn a_second_run_does_not_repeat_the_sections() {
    let dir = repo();
    assert_eq!(init(dir.path(), &["--dir", "."]).0, 0);
    let (agents, ignore) = (
        read(&dir.path().join("AGENTS.md")),
        read(&dir.path().join(".gitignore")),
    );
    // A nest already there is refused, so take its config away to run `init` over the same files.
    std::fs::remove_file(dir.path().join("nuthatch.toml")).unwrap();
    let (code, v, stderr) = init(dir.path(), &["--dir", "."]);
    assert_eq!(code, 0, "{stderr}");
    assert_eq!(read(&dir.path().join("AGENTS.md")), agents);
    assert_eq!(read(&dir.path().join(".gitignore")), ignore);
    assert_eq!(agents.matches("nuthatch:begin").count(), 1);
    assert_eq!(ignore.matches("nuthatch:begin").count(), 1);
    assert!(strings(&v["files_appended"]).is_empty(), "{v}");
}

#[test]
fn in_place_init_appends_to_the_files_there_and_json_lists_what_it_skipped() {
    let dir = repo();
    let (code, v, stderr) = init(dir.path(), &["--dir", "."]);
    assert_eq!(code, 0, "{stderr}");
    assert_eq!(v["dir_defaulted"], false, "{v}");
    assert!(
        dir.path().join("nuthatch.toml").exists(),
        "--dir . scaffolds in place"
    );
    let skipped = strings(&v["files_skipped"]);
    for f in ["AGENTS.md", ".gitignore"] {
        assert!(skipped.contains(&f.to_string()), "{f} not in {skipped:?}");
    }
    let appended = strings(&v["files_appended"]);
    assert_eq!(appended.len(), 2, "{v}");
    let agents = read(&dir.path().join("AGENTS.md"));
    assert!(
        agents.starts_with(THEIR_AGENTS) && agents.contains("weth__transfer"),
        "{agents}"
    );
    let ignore = read(&dir.path().join(".gitignore"));
    assert!(ignore.starts_with(THEIR_IGNORE), "{ignore}");
    assert!(ignore.lines().any(|l| l == "nuthatch.redb"), "{ignore}");
}

#[test]
fn a_repo_without_those_files_is_scaffolded_in_place() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join(".git")).unwrap();
    let (code, v, stderr) = init(dir.path(), &[]);
    assert_eq!(code, 0, "{stderr}");
    assert_eq!(v["dir_defaulted"], false, "{v}");
    assert!(dir.path().join("nuthatch.toml").exists());
    assert!(strings(&v["files_skipped"]).is_empty(), "{v}");
}

#[test]
fn a_second_run_refreshes_the_section_when_the_generated_text_changed() {
    let dir = repo();
    assert_eq!(init(dir.path(), &["--dir", "."]).0, 0);
    let agents_path = dir.path().join("AGENTS.md");
    let first = read(&agents_path);
    assert!(first.contains("weth__transfer"), "{first}");

    // Stale text inside the block, as if the tables or the generator had changed since.
    let stale = first.replace("weth__transfer", "weth__stale_table");
    assert_ne!(stale, first);
    std::fs::write(&agents_path, format!("{stale}\nTheir footer.\n")).unwrap();
    std::fs::remove_file(dir.path().join("nuthatch.toml")).unwrap();

    let (code, v, stderr) = init(dir.path(), &["--dir", "."]);
    assert_eq!(code, 0, "{stderr}");
    let agents = read(&agents_path);
    assert!(agents.contains("weth__transfer"), "{agents}");
    assert!(!agents.contains("stale_table"), "{agents}");
    assert!(agents.starts_with(THEIR_AGENTS), "{agents}");
    assert!(agents.ends_with("\nTheir footer.\n"), "{agents}");
    assert_eq!(agents.matches("nuthatch:begin").count(), 1, "{agents}");
    assert_eq!(agents.matches("nuthatch:end").count(), 1, "{agents}");
    let updated = strings(&v["files_updated"]);
    assert_eq!(updated.len(), 1, "{v}");
    assert!(updated[0].ends_with("AGENTS.md"), "{v}");
    assert!(strings(&v["files_appended"]).is_empty(), "{v}");
}

#[test]
fn rerunning_after_the_nest_is_deleted_refreshes_the_host_section() {
    let dir = repo();
    assert_eq!(init(dir.path(), &[]).0, 0);
    let agents_path = dir.path().join("AGENTS.md");
    let first = read(&agents_path);
    std::fs::write(&agents_path, first.replace("`nest/`", "`gone/`")).unwrap();
    std::fs::remove_dir_all(dir.path().join("nest")).unwrap();

    let (code, v, stderr) = init(dir.path(), &[]);
    assert_eq!(code, 0, "{stderr}");
    assert_eq!(
        read(&agents_path),
        first,
        "block regenerated, not duplicated"
    );
    assert_eq!(strings(&v["files_updated"]).len(), 1, "{v}");
}
