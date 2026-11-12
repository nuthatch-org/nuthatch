//! The compose files' default image is the release they ship in. An operator who sets no
//! `NUTHATCH_IMAGE` runs that default, and CI always sets one, so nothing else would notice a stale
//! tag: it sat at 1.0.2 from August to 5.19, past every security fix in between.

use std::path::PathBuf;

const IMAGE: &str = "ghcr.io/nuthatch-org/nuthatch:";

#[test]
fn every_compose_default_image_is_this_release() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let version = env!("CARGO_PKG_VERSION");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&root)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("docker-compose") && n.ends_with(".yml"))
        })
        .collect();
    files.sort();
    assert!(
        !files.is_empty(),
        "no docker-compose*.yml at {}",
        root.display()
    );
    let mut stale = Vec::new();
    let mut seen = 0;
    for file in &files {
        let text = std::fs::read_to_string(file).unwrap();
        for (n, line) in text.lines().enumerate() {
            if line.trim_start().starts_with('#') {
                continue;
            }
            for (at, _) in line.match_indices(IMAGE) {
                seen += 1;
                let tag: String = line[at + IMAGE.len()..]
                    .chars()
                    .take_while(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'))
                    .collect();
                let current = tag
                    .strip_prefix(version)
                    .is_some_and(|rest| rest.is_empty() || rest.starts_with('-'));
                if !current {
                    stale.push(format!("{}:{}: {tag}", file.display(), n + 1));
                }
            }
        }
    }
    assert!(seen > 0, "no {IMAGE} image named in {files:?}");
    assert!(
        stale.is_empty(),
        "compose default images that are not {version}; bump them with Cargo.toml's version:\n{}",
        stale.join("\n")
    );
}
