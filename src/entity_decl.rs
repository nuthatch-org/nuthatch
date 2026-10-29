//! `entities.toml` as declared (RFC-0041 §3). Kept apart from [`crate::entities`] so a tab can read the
//! same declarations without the binary (RFC-0075 S4).

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub(crate) struct EntityFile {
    #[serde(default)]
    pub(crate) entities: Vec<EntityDecl>,
}

/// One author-declared maintained relation. Parsing is centralised here so startup, `check`, and a
/// future serving surface all act on exactly the file whose bytes form the nest identity.
#[derive(Debug, Clone, Deserialize)]
pub struct EntityDecl {
    pub name: String,
    pub sql: String,
    pub key: Vec<String>,
    pub max_rows: usize,
}

/// The declarations in an `entities.toml`'s text.
pub fn parse(raw: &str) -> Result<Vec<EntityDecl>> {
    Ok(toml::from_str::<EntityFile>(raw)?.entities)
}

impl EntityDecl {
    /// The `entities/<name>.sql` this declaration names, or why it names no such file.
    pub fn sql_path(&self) -> Result<&Path> {
        let rel = Path::new(&self.sql);
        if rel.components().count() != 2
            || rel.parent() != Some(Path::new("entities"))
            || rel.extension().and_then(|x| x.to_str()) != Some("sql")
        {
            bail!(
                "sql must name one entities/<name>.sql file; move the SQL into entities/{}.sql and set sql = \"entities/{}.sql\"",
                self.name, self.name
            );
        }
        if rel.file_stem().and_then(|s| s.to_str()) != Some(self.name.as_str()) {
            bail!("entity name must match the declared SQL filename");
        }
        Ok(rel)
    }

    /// Resolve the same authored SQL file for validation and runtime startup.
    pub fn read_sql(&self, dir: &Path) -> Result<String> {
        let rel = self.sql_path()?;
        std::fs::read_to_string(dir.join(rel)).with_context(|| format!("cannot read {}", self.sql))
    }
}
