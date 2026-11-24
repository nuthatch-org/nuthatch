//! `nuthatch-sbom --manifest-path <Cargo.toml> --target <triple> --out <file.cdx.json>
//!   [--artifact <released file>] [--deny <deny.toml>] [-- <cargo feature args>]`
//!
//! Writes the CycloneDX SBOM of one release artifact (src/sbom.rs). With `--deny`, refuses to write
//! one that names a licence deny.toml does not allow: cargo-deny in CI checks the default feature
//! set, and this is where the scaled, graph and flight builds' licences meet the same list.

mod sbom;

use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    match real_main() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("nuthatch-sbom: {e}");
            ExitCode::FAILURE
        }
    }
}

fn real_main() -> Result<(), String> {
    let mut args = std::env::args().skip(1);
    let (mut manifest, mut target, mut out, mut artifact, mut deny) =
        (None, None, None, None, None);
    let mut cargo_args = Vec::new();
    while let Some(a) = args.next() {
        let mut value = || args.next().ok_or(format!("{a} needs a value"));
        match a.as_str() {
            "--manifest-path" => manifest = Some(PathBuf::from(value()?)),
            "--target" => target = Some(value()?),
            "--out" => out = Some(PathBuf::from(value()?)),
            "--artifact" => artifact = Some(value()?),
            "--deny" => deny = Some(PathBuf::from(value()?)),
            "--" => {
                cargo_args.extend(args.by_ref());
            }
            other => return Err(format!("unknown argument `{other}`")),
        }
    }
    let manifest = manifest.ok_or("--manifest-path is required")?;
    let target = target.ok_or("--target is required")?;
    let out = out.ok_or("--out is required")?;
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());

    let bom = sbom::generate(&sbom::Request {
        cargo: &cargo,
        manifest: &manifest,
        target: &target,
        cargo_args: &cargo_args,
        artifact: artifact.as_deref(),
    })?;
    if let Some(deny) = deny {
        let text = std::fs::read_to_string(&deny)
            .map_err(|e| format!("cannot read {}: {e}", deny.display()))?;
        let refused = sbom::refused(&bom, &sbom::deny_allow_list(&text)?);
        if !refused.is_empty() {
            return Err(format!(
                "{} component(s) carry a licence {} does not allow:\n  {}",
                refused.len(),
                deny.display(),
                refused.join("\n  ")
            ));
        }
    }
    std::fs::write(&out, sbom::to_json(&bom))
        .map_err(|e| format!("cannot write {}: {e}", out.display()))?;
    eprintln!(
        "{}: {} components ({} linked) for {target}",
        out.display(),
        bom.components.len(),
        bom.components.values().filter(|c| c.runtime).count()
    );
    Ok(())
}
