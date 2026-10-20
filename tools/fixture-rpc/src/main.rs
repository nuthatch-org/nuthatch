use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use nuthatch_fixture_rpc::footprint::Footprint;
use nuthatch_fixture_rpc::multinest::{self, Multinest};
use nuthatch_fixture_rpc::scripted::Scripted;
use nuthatch_fixture_rpc::{serve, Fixture};

#[derive(Parser)]
#[command(about = "Deterministic JSON-RPC chains for nuthatch's tests and CI gates")]
struct Cli {
    #[command(subcommand)]
    chain: Chain,
}

#[derive(Subcommand)]
enum Chain {
    /// One Transfer a block over blocks 1..=8, switchable through /control/mode to zero logs
    Scripted {
        #[arg(long, default_value_t = 8945)]
        port: u16,
        #[arg(long)]
        contract: String,
        /// Initial eth_blockNumber and "latest"
        #[arg(long)]
        tip: Option<u64>,
        /// Initial finalized block
        #[arg(long)]
        finalized: Option<u64>,
        /// Address to listen on; 0.0.0.0 to be reachable from a container
        #[arg(long, default_value = "127.0.0.1")]
        bind: String,
    },
    /// One contract, four Transfers a block, tip 20,000
    Footprint {
        #[arg(default_value_t = 8545)]
        port: u16,
        /// Advance the tip one block per fixture_advance call, recording when each was first seen
        #[arg(long)]
        moving: bool,
    },
    /// Many contracts emitting every event of an ABI, the tip moving once the backfill drains
    Multinest {
        #[arg(long, default_value_t = 8546)]
        port: u16,
        /// The ABI the harness wrote; topic0s come from it
        #[arg(long)]
        abi: PathBuf,
        /// One per nest on the cursor
        #[arg(long, default_value_t = 8)]
        contracts: u64,
        /// Per contract, per block
        #[arg(long, default_value_t = 10)]
        logs_per_block: u64,
        #[arg(long, default_value_t = 20_000)]
        initial_tip: u64,
        #[arg(long, default_value_t = 20_240)]
        final_tip: u64,
        /// Blocks released per eth_blockNumber
        #[arg(long, default_value_t = 8)]
        tip_step: u64,
    },
}

fn main() {
    match Cli::parse().chain {
        Chain::Scripted {
            port,
            contract,
            tip,
            finalized,
            bind,
        } => {
            let fx = Scripted::new(&contract).pinned(tip, finalized);
            let (tip, finalized) = fx.tip_and_finalized();
            let banner = format!(
                "fixture-rpc listening on http://{bind}:{port}/ contract={} tip={tip} finalized={finalized}",
                fx.contract()
            );
            run(&bind, port, fx, Some(banner));
        }
        Chain::Footprint { port, moving } => run("127.0.0.1", port, Footprint::new(moving), None),
        Chain::Multinest {
            port,
            abi,
            contracts,
            logs_per_block,
            initial_tip,
            final_tip,
            tip_step,
        } => {
            let events = std::fs::read_to_string(&abi)
                .map_err(|e| e.to_string())
                .and_then(|s| serde_json::from_str(&s).map_err(|e| e.to_string()))
                .and_then(|v| multinest::load_events(&v))
                .unwrap_or_else(|e| die(&format!("{}: {e}", abi.display())));
            eprintln!(
                "fixture-rpc multinest: {} events, {contracts} contracts, {logs_per_block} \
                 logs/block/contract, tip {initial_tip}->{final_tip} step {tip_step}",
                events.len()
            );
            let cfg = multinest::Config {
                contracts,
                logs_per_block,
                initial_tip,
                final_tip,
                tip_step,
            };
            run("127.0.0.1", port, Multinest::new(events, cfg), None);
        }
    }
}

fn run<F: Fixture>(bind: &str, port: u16, fixture: F, banner: Option<String>) {
    let runtime = tokio::runtime::Runtime::new().unwrap_or_else(|e| die(&e.to_string()));
    runtime.block_on(async {
        let listener = tokio::net::TcpListener::bind((bind, port))
            .await
            .unwrap_or_else(|e| die(&format!("bind {bind}:{port}: {e}")));
        if let Some(b) = banner {
            println!("{b}");
        }
        serve(listener, Arc::new(fixture)).await;
    });
}

fn die(msg: &str) -> ! {
    eprintln!("fixture-rpc: {msg}");
    std::process::exit(1);
}
