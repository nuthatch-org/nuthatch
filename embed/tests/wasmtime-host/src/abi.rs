//! The pinned interface. Each published `abi` version's shapes, every export and import as wasmtime
//! types them, are held here; a module whose shapes differ from those of the version it reports
//! fails `check`. A shape change therefore needs a new version, and a version's table, once
//! published, is frozen by its hash (`published_versions_are_frozen`).

use anyhow::{bail, Result};
use wasmtime::{ExternType, Module, Mutability};

/// `abi: 1`. Linker leftovers (getrandom's hooks, zstd's shims, `__data_end`, `__heap_base`) are
/// pinned with the rest: a host can see them, so they are shape.
pub const V1: &[&str] = &[
    "export func __getrandom_custom(i32, i32) -> (i32)",
    "export func __getrandom_v03_custom(i32, i32) -> (i32)",
    "export func nh_abi() -> (i32)",
    "export func nh_alloc(i32) -> (i32)",
    "export func nh_export() -> (i32)",
    "export func nh_free(i32, i32) -> ()",
    "export func nh_memory_bytes() -> (f64)",
    "export func nh_open(i32, i32) -> (i32)",
    "export func nh_output(i32) -> ()",
    "export func nh_report() -> (i32)",
    "export func nh_request(i32, i32, i32, i32, i32, i32) -> (i32)",
    "export func nh_resume(i32, i32) -> (i32)",
    "export func nh_step() -> (i32)",
    "export func rust_zstd_wasm_shim_calloc(i32, i32) -> (i32)",
    "export func rust_zstd_wasm_shim_free(i32) -> ()",
    "export func rust_zstd_wasm_shim_malloc(i32) -> (i32)",
    "export func rust_zstd_wasm_shim_memcmp(i32, i32, i32) -> (i32)",
    "export func rust_zstd_wasm_shim_memcpy(i32, i32, i32) -> (i32)",
    "export func rust_zstd_wasm_shim_memmove(i32, i32, i32) -> (i32)",
    "export func rust_zstd_wasm_shim_memset(i32, i32, i32) -> (i32)",
    "export func rust_zstd_wasm_shim_qsort(i32, i32, i32, i32) -> ()",
    "export global __data_end i32 const",
    "export global __heap_base i32 const",
    "export memory memory i32 max Some(65536) shared false",
    "import func nuthatch.log(i32, i32, i32) -> ()",
    "import func nuthatch.now_ms() -> (f64)",
    "import func nuthatch.random_fill(i32, i32) -> ()",
    "import func nuthatch.store_close(i32) -> ()",
    "import func nuthatch.store_len(i32) -> (i64)",
    "import func nuthatch.store_read(i32, i64, i32, i32) -> (i32)",
    "import func nuthatch.store_set_len(i32, i64) -> (i32)",
    "import func nuthatch.store_sync(i32) -> (i32)",
    "import func nuthatch.store_write(i32, i64, i32, i32) -> (i32)",
];

/// sha256 of each version's table joined by newlines. Editing a published table fails the build's
/// tests; a new shape is a new version.
#[cfg(test)]
const FROZEN: &[(u32, &str)] = &[(
    1,
    "59363eb2af5b66f81e8d0cd6eef06786ffcf80045e821c69997add03f9a11071",
)];

pub fn table(version: u64) -> Option<&'static [&'static str]> {
    match version {
        1 => Some(V1),
        _ => None,
    }
}

fn types(ts: impl Iterator<Item = wasmtime::ValType>) -> String {
    ts.map(|t| t.to_string()).collect::<Vec<_>>().join(", ")
}

fn render(name: String, ty: ExternType) -> String {
    match ty {
        ExternType::Func(f) => format!(
            "func {name}({}) -> ({})",
            types(f.params()),
            types(f.results())
        ),
        ExternType::Memory(m) => format!(
            "memory {name} {} max {:?} shared {}",
            if m.is_64() { "i64" } else { "i32" },
            m.maximum(),
            m.is_shared()
        ),
        ExternType::Global(g) => format!(
            "global {name} {} {}",
            g.content(),
            match g.mutability() {
                Mutability::Const => "const",
                Mutability::Var => "var",
            }
        ),
        ExternType::Table(t) => format!("table {name} {}", t.element()),
        ExternType::Tag(_) => format!("tag {name}"),
    }
}

/// Every export and import of `module`, one line each, sorted.
pub fn shapes(module: &Module) -> Vec<String> {
    let mut out: Vec<String> = module
        .exports()
        .map(|e| format!("export {}", render(e.name().to_string(), e.ty())))
        .chain(module.imports().map(|i| {
            format!(
                "import {}",
                render(format!("{}.{}", i.module(), i.name()), i.ty())
            )
        }))
        .collect();
    out.sort();
    out
}

/// `actual` against the table of the version the module reports.
pub fn check(actual: &[String], version: u64) -> Result<()> {
    let Some(pinned) = table(version) else {
        bail!("the module reports abi {version}, which has no pinned table");
    };
    let missing: Vec<&str> = pinned
        .iter()
        .copied()
        .filter(|p| !actual.iter().any(|a| a == p))
        .collect();
    let extra: Vec<&String> = actual
        .iter()
        .filter(|a| !pinned.contains(&a.as_str()))
        .collect();
    if !missing.is_empty() || !extra.is_empty() {
        bail!(
            "the module's shapes differ from abi {version}'s, so it needs a new version\n  pinned, not in the module: {missing:#?}\n  in the module, not pinned: {extra:#?}"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    fn v1() -> Vec<String> {
        V1.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn the_pinned_shapes_pass() {
        check(&v1(), 1).unwrap();
    }

    #[test]
    fn a_changed_signature_without_a_bump_fails() {
        let mut shapes = v1();
        let at = shapes
            .iter()
            .position(|s| s.starts_with("export func nh_step("))
            .unwrap();
        shapes[at] = "export func nh_step(i32) -> (i32)".into();
        let e = check(&shapes, 1).unwrap_err().to_string();
        assert!(e.contains("nh_step"), "{e}");
    }

    #[test]
    fn an_added_or_removed_export_or_import_fails() {
        let mut more = v1();
        more.push("export func nh_new() -> (i32)".into());
        assert!(check(&more, 1).is_err());
        let fewer: Vec<String> = v1()
            .into_iter()
            .filter(|s| !s.starts_with("import func nuthatch.store_close("))
            .collect();
        assert!(check(&fewer, 1).is_err());
    }

    #[test]
    fn an_unpublished_version_fails() {
        assert!(check(&v1(), 2).is_err());
    }

    #[test]
    fn published_versions_are_frozen() {
        for (version, want) in FROZEN {
            let got = format!(
                "{:x}",
                Sha256::digest(table(u64::from(*version)).unwrap().join("\n"))
            );
            assert_eq!(
                &got, want,
                "abi {version}'s table changed after publication"
            );
        }
        assert_eq!(FROZEN.len(), 1, "every published version is frozen");
    }

    #[test]
    fn the_renderer_reads_a_module_as_the_table_writes_it() {
        let engine = wasmtime::Engine::default();
        let module = Module::new(
            &engine,
            r#"(module
                 (import "nuthatch" "store_read" (func (param i32 i64 i32 i32) (result i32)))
                 (memory (export "memory") 1 65536)
                 (global (export "__heap_base") i32 (i32.const 0))
                 (func (export "nh_memory_bytes") (result f64) f64.const 0))"#,
        )
        .unwrap();
        assert_eq!(
            shapes(&module),
            [
                "export func nh_memory_bytes() -> (f64)",
                "export global __heap_base i32 const",
                "export memory memory i32 max Some(65536) shared false",
                "import func nuthatch.store_read(i32, i64, i32, i32) -> (i32)",
            ]
        );
    }

    #[test]
    fn the_readme_names_every_export_and_import() {
        let readme = include_str!("../../../README.md");
        for line in V1 {
            let name = line
                .split_whitespace()
                .nth(2)
                .unwrap()
                .split('(')
                .next()
                .unwrap();
            let name = name.strip_prefix("nuthatch.").unwrap_or(name);
            assert!(
                readme.contains(&format!("`{name}")),
                "embed/README.md does not name `{name}`"
            );
        }
    }
}
