//! The admin credential a localhost runtime makes for itself when `NUTHATCH_ADMIN_TOKEN` is unset
//! (#2354). A loopback bind cannot tell a local browser from a reverse proxy, so no request is
//! admitted on where it appears to come from: the token is kept beside the nest, readable by its
//! owner alone, and the CLI reads it from the same `--dir`.

use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};

/// Left out of every nest's inputs (`blob::EXCLUDE`), so it moves no NID and is never bundled.
pub const FILE: &str = ".admin-token";

/// The token, or the private name it is written under before it is linked into place (#2530).
pub fn is_own(name: &str) -> bool {
    name.strip_prefix(FILE)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with('.'))
}

pub fn path(dir: &Path) -> PathBuf {
    dir.join(FILE)
}

/// The token in `dir`, if one has been made there.
pub fn read(dir: &Path) -> Option<String> {
    std::fs::read_to_string(path(dir))
        .ok()
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty())
}

/// The token in `dir`, made on first use: 32 bytes from the OS, hex, owner-only.
pub fn load_or_create(dir: &Path) -> Result<String> {
    if let Some(token) = read(dir) {
        return Ok(token);
    }
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).map_err(|e| anyhow!("OS randomness unavailable: {e}"))?;
    let token = hex::encode(bytes);
    // Written whole under a private name, then linked into place: a reader never sees a partial
    // file, and a second process starting at the same moment keeps the first one's token.
    let tmp = dir.join(format!("{FILE}.{}", std::process::id()));
    let _ = std::fs::remove_file(&tmp);
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut opts, 0o600);
    let written = opts.open(&tmp).and_then(|mut f| {
        f.write_all(token.as_bytes())?;
        f.sync_all()
    });
    let linked = written.and_then(|()| std::fs::hard_link(&tmp, path(dir)));
    let _ = std::fs::remove_file(&tmp);
    match linked {
        Ok(()) => Ok(token),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => match read(dir) {
            Some(token) => Ok(token),
            None => bail!("{} exists but is empty", path(dir).display()),
        },
        Err(e) => Err(e).with_context(|| {
            format!(
                "cannot write the admin token to {}; set NUTHATCH_ADMIN_TOKEN, or pass --no-admin",
                path(dir).display()
            )
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_token_is_made_once_and_kept() {
        let dir = tempfile::tempdir().unwrap();
        let made = load_or_create(dir.path()).unwrap();
        assert_eq!(made.len(), 64);
        assert!(made.bytes().all(|b| b.is_ascii_hexdigit()));
        assert_eq!(load_or_create(dir.path()).unwrap(), made);
        assert_eq!(read(dir.path()).as_deref(), Some(made.as_str()));
        let other = tempfile::tempdir().unwrap();
        assert_ne!(load_or_create(other.path()).unwrap(), made);
        let names: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names, [FILE], "no temporary file is left behind");
    }

    #[cfg(unix)]
    #[test]
    fn the_token_file_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        load_or_create(dir.path()).unwrap();
        let mode = std::fs::metadata(path(dir.path()))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }
}
