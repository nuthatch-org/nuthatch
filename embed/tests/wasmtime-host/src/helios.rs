//! RFC-0081 S5: `verified_finalized` for this host, from Helios's beacon light client. Its consensus
//! core verifies a bootstrap against a trusted checkpoint, then each sync-committee update and the
//! finality update, as Helios's own client does in `sync`; the finalized header it ends on names the
//! execution block. The beacon endpoint can withhold an update but cannot forge one.
//!
//! A light client run is the answers it verified: `{checkpoint, bootstrap, updates, finality_update}`,
//! the beacon API's `data` for each. `fetch` asks a live endpoint for them; a recording of the same
//! shape is verified the same way, so CI needs no beacon endpoint.

use anyhow::{anyhow, ensure, Context, Result};
use helios_consensus_core::consensus_spec::MainnetConsensusSpec;
use helios_consensus_core::types::{Bootstrap, FinalityUpdate, Forks, LightClientStore, Update};
use helios_consensus_core::{
    apply_bootstrap, apply_finality_update, apply_update, calc_sync_period, verify_bootstrap,
    verify_finality_update, verify_update,
};
use serde_json::{json, Value};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

type Spec = MainnetConsensusSpec;

const GENESIS_TIME: u64 = 1_606_824_023;
const GENESIS_ROOT: &str = "0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95";
/// Helios's own limit for one `updates` request.
const MAX_UPDATES: u64 = 128;

/// Mainnet's forks, as Helios's `networks::mainnet` gives them.
fn forks() -> Result<Forks> {
    let fork = |epoch: u64, v: &str| json!({"epoch": epoch, "fork_version": v});
    Ok(serde_json::from_value(json!({
        "genesis": fork(0, "0x00000000"),
        "altair": fork(74_240, "0x01000000"),
        "bellatrix": fork(144_896, "0x02000000"),
        "capella": fork(194_048, "0x03000000"),
        "deneb": fork(269_568, "0x04000000"),
        "electra": fork(364_032, "0x05000000"),
        "fulu": fork(411_392, "0x06000000"),
        "gloas": fork(u64::MAX, "0x07000000"),
    }))?)
}

fn now_slot() -> u64 {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    secs.saturating_sub(GENESIS_TIME) / 12
}

/// The finalized execution block `{number, hash}` a light client run verifies, or why it does not.
pub fn verify(run: &Value) -> Result<Value> {
    let forks = forks()?;
    let genesis_root = serde_json::from_value(json!(GENESIS_ROOT))?;
    let checkpoint = serde_json::from_value(run["checkpoint"].clone()).context("checkpoint")?;
    let bootstrap: Bootstrap<Spec> =
        serde_json::from_value(run["bootstrap"].clone()).context("bootstrap")?;
    verify_bootstrap(&bootstrap, checkpoint, &forks).map_err(|e| anyhow!("bootstrap: {e}"))?;
    let mut store = LightClientStore::<Spec>::default();
    apply_bootstrap(&mut store, &bootstrap);
    // Helios checks each against the wall clock's slot; a recording is older than now, never newer.
    let slot = now_slot();
    for (i, u) in run["updates"].as_array().into_iter().flatten().enumerate() {
        let update: Update<Spec> = serde_json::from_value(u.clone()).context("update")?;
        verify_update(&update, slot, &store, genesis_root, &forks)
            .map_err(|e| anyhow!("update {i}: {e}"))?;
        apply_update(&mut store, &update);
    }
    if !run["finality_update"].is_null() {
        let f: FinalityUpdate<Spec> =
            serde_json::from_value(run["finality_update"].clone()).context("finality update")?;
        verify_finality_update(&f, slot, &store, genesis_root, &forks)
            .map_err(|e| anyhow!("finality update: {e}"))?;
        apply_finality_update(&mut store, &f);
    }
    let header = serde_json::to_value(&store.finalized_header)?;
    let execution = &header["execution"];
    let number = execution["block_number"]
        .as_str()
        .and_then(|n| n.parse::<u64>().ok())
        .context("the finalized header carries no execution block")?;
    let hash = execution["block_hash"]
        .as_str()
        .context("no execution block hash")?;
    Ok(json!({"number": number, "hash": hash, "slot": header["beacon"]["slot"]}))
}

/// The run with the last finalized execution block it names replaced, as a consensus endpoint that
/// lied about which block was finalized would send it.
pub fn forge(run: &mut Value) -> Result<()> {
    let last = if run["finality_update"].is_null() {
        run["updates"]
            .as_array_mut()
            .and_then(|u| u.last_mut())
            .context("a run with nothing to forge")?
    } else {
        &mut run["finality_update"]
    };
    last["finalized_header"]["execution"]["block_hash"] = json!(format!("0x{}", "77".repeat(32)));
    Ok(())
}

fn get(agent: &ureq::Agent, url: &str) -> Result<Value> {
    let mut res = agent.get(url).call().with_context(|| url.to_string())?;
    let status = res.status();
    let body = res
        .body_mut()
        .with_config()
        .limit(64 << 20)
        .read_to_string()?;
    ensure!(status == 200, "{url}: {status}: {body:.300}");
    Ok(serde_json::from_str(&body)?)
}

/// A light client run from `beacon`: the bootstrap for `checkpoint`, every update from its period
/// through `until_period` (or the current one), and, without `until_period`, the finality update.
pub fn fetch(beacon: &str, checkpoint: &str, until_period: Option<u64>) -> Result<Value> {
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(60)))
        .build()
        .into();
    let base = beacon.trim_end_matches('/');
    let bootstrap = get(
        &agent,
        &format!("{base}/eth/v1/beacon/light_client/bootstrap/{checkpoint}"),
    )?["data"]
        .take();
    let slot: u64 = bootstrap["header"]["beacon"]["slot"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .context("the bootstrap has no slot")?;
    let last = until_period.unwrap_or_else(|| calc_sync_period::<Spec>(now_slot()));
    let mut updates = Vec::new();
    let mut period = calc_sync_period::<Spec>(slot);
    while period <= last {
        let count = (last - period + 1).min(MAX_UPDATES);
        let got = get(
            &agent,
            &format!(
                "{base}/eth/v1/beacon/light_client/updates?start_period={period}&count={count}"
            ),
        )?;
        let got = got.as_array().context("updates: not an array")?;
        ensure!(
            !got.is_empty(),
            "the endpoint has no update for period {period}"
        );
        updates.extend(got.iter().map(|u| u["data"].clone()));
        period += count;
    }
    let finality_update = if until_period.is_none() {
        get(
            &agent,
            &format!("{base}/eth/v1/beacon/light_client/finality_update"),
        )?["data"]
            .take()
    } else {
        Value::Null
    };
    Ok(json!({
        "consensus": base, "checkpoint": checkpoint, "bootstrap": bootstrap,
        "updates": updates, "finality_update": finality_update,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn recording() -> Value {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../tests/fixtures/tab-parity/helios.json.gz"
        );
        let mut s = String::new();
        std::io::Read::read_to_string(
            &mut flate2::read::GzDecoder::new(std::fs::File::open(path).unwrap()),
            &mut s,
        )
        .unwrap();
        serde_json::from_str(&s).unwrap()
    }

    #[test]
    fn the_recorded_run_verifies_to_its_finalized_block() {
        let head = verify(&recording()).unwrap();
        assert_eq!(head["number"], 26_243_710);
        assert_eq!(
            head["hash"],
            "0x24be5c10f90ee1c234813f02eeb4afef605a34017be11536d75886b5e785196c"
        );
    }

    #[test]
    fn a_finalized_block_the_committee_did_not_sign_is_refused() {
        let mut run = recording();
        let last = run["updates"].as_array_mut().unwrap().last_mut().unwrap();
        last["finalized_header"]["execution"]["block_hash"] =
            json!(format!("0x{}", "77".repeat(32)));
        let e = verify(&run).unwrap_err().to_string();
        assert!(e.starts_with("update 1:"), "{e}");
    }

    #[test]
    fn a_signature_from_outside_the_committee_is_refused() {
        let mut run = recording();
        let last = run["updates"].as_array_mut().unwrap().last_mut().unwrap();
        let sig = last["sync_aggregate"]["sync_committee_signature"]
            .as_str()
            .unwrap()
            .to_string();
        let first = run["updates"][0]["sync_aggregate"]["sync_committee_signature"].clone();
        assert_ne!(first, json!(sig));
        run["updates"][1]["sync_aggregate"]["sync_committee_signature"] = first;
        let e = verify(&run).unwrap_err().to_string();
        assert!(e.starts_with("update 1:"), "{e}");
    }

    #[test]
    fn a_bootstrap_for_another_checkpoint_is_refused() {
        let mut run = recording();
        run["checkpoint"] = json!(format!("0x{}", "11".repeat(32)));
        let e = verify(&run).unwrap_err().to_string();
        assert!(e.starts_with("bootstrap:"), "{e}");
    }
}
