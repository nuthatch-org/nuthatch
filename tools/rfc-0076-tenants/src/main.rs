//! RFC-0076 S1's bench: one chain's SQL gate with 1,000 tenants mounted and some of them querying in
//! a closed loop, under the ledger in `src/fair_share.rs` and under 5.13.0's (`baseline/`, which
//! `run.sh` extracts). Time is tokio's paused clock, so a run is deterministic and a statement's
//! service time is a sleep: this measures admission, not the engine.

use std::future::Future;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// What `fair_share.rs` reaches outside itself, as small as it needs.
mod runtime {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct TenantShare {
        pub weight: u32,
        pub max_permits: Option<usize>,
        pub max_response_mib: Option<u64>,
    }

    impl Default for TenantShare {
        fn default() -> Self {
            Self {
                weight: 1,
                max_permits: None,
                max_response_mib: None,
            }
        }
    }
}

mod metrics {
    #[derive(Clone, Copy)]
    pub enum SqlRejection {
        Busy,
        TenantShare,
    }
}

mod serve {
    pub const SQL_MAX_HOT_INFLIGHT_BYTES: u64 = 128 << 20;
}

#[path = "../../../src/fair_share.rs"]
#[allow(dead_code)]
mod current;

#[path = "../baseline/fair_share.rs"]
#[allow(dead_code)]
mod baseline;

/// `/sql`'s admission wait, `SQL_ADMISSION_WAIT`.
const WAIT: Duration = Duration::from_millis(250);
const MOUNTED: usize = 1000;
const RUN: Duration = Duration::from_secs(120);

trait Ledger: Clone + Send + Sync + 'static {
    type Permit: Send + 'static;
    type Seat: 'static;
    fn new(permits: usize) -> Self;
    fn seat(&self, tenant: &str, dir: &Path) -> Self::Seat;
    fn admit(&self, tenant: String) -> impl Future<Output = Option<Self::Permit>> + Send;
}

macro_rules! ledger {
    ($name:ident, $m:ident) => {
        #[derive(Clone)]
        struct $name($m::SqlGate, Arc<$m::ByteShares>);

        impl Ledger for $name {
            type Permit = $m::GatePermit;
            type Seat = $m::Seat;
            fn new(permits: usize) -> Self {
                Self(
                    $m::SqlGate::new(permits),
                    Arc::new($m::ByteShares::new(64 << 10)),
                )
            }
            fn seat(&self, tenant: &str, dir: &Path) -> $m::Seat {
                $m::Seat::new(tenant, dir, &self.0, &self.1, Default::default())
            }
            fn admit(&self, tenant: String) -> impl Future<Output = Option<Self::Permit>> + Send {
                let gate = self.0.clone();
                async move {
                    match gate.try_admit(Some(&tenant)) {
                        Ok(p) => Some(p),
                        Err(_) => gate.wait_admit(Some(&tenant), WAIT).await.ok(),
                    }
                }
            }
        }
    };
}

ledger!(Current, current);
ledger!(Baseline, baseline);

#[derive(Clone, Copy)]
struct Class {
    label: &'static str,
    tenants: usize,
    clients: usize,
    service_ms: f64,
    think_ms: f64,
}

struct Profile {
    name: &'static str,
    permits: usize,
    classes: Vec<Class>,
}

#[derive(Default, Clone)]
struct Tally {
    waits_ms: Vec<f64>,
    refused: usize,
    busy_ms: f64,
}

/// xorshift64*, seeded per client so a run is the same every time.
struct Rng(u64);

impl Rng {
    fn exp(&mut self, mean: f64) -> f64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        let u = (self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 11) as f64 / (1u64 << 53) as f64;
        -mean * (1.0 - u).max(f64::MIN_POSITIVE).ln()
    }
}

fn run<L: Ledger>(profile: &Profile) -> (Vec<(Class, Tally)>, Duration) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();
    let wall = std::time::Instant::now();
    let out = rt.block_on(async {
        let gate = L::new(profile.permits);
        let _seats: Vec<_> = (0..MOUNTED)
            .map(|i| gate.seat(&format!("t{i}"), Path::new(&format!("/data/t{i}"))))
            .collect();
        let end = tokio::time::Instant::now() + RUN;
        let mut first = 0;
        let mut handles = Vec::new();
        let mut tallies = Vec::new();
        for class in &profile.classes {
            let tally = Arc::new(Mutex::new(Tally::default()));
            for t in first..first + class.tenants {
                for c in 0..class.clients {
                    let (gate, tally, class) = (gate.clone(), tally.clone(), *class);
                    let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ ((t as u64) << 16 | c as u64));
                    handles.push(tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_secs_f64(
                            rng.exp(class.think_ms.max(1.0)) / 1e3,
                        ))
                        .await;
                        while tokio::time::Instant::now() < end {
                            let asked = tokio::time::Instant::now();
                            let permit = gate.admit(format!("t{t}")).await;
                            let ok = permit.is_some();
                            let waited = asked.elapsed().as_secs_f64() * 1e3;
                            let service = rng.exp(class.service_ms);
                            {
                                let mut tally = tally.lock().unwrap();
                                tally.waits_ms.push(waited);
                                if ok {
                                    tally.busy_ms += service;
                                } else {
                                    tally.refused += 1;
                                }
                            }
                            if ok {
                                tokio::time::sleep(Duration::from_secs_f64(service / 1e3)).await;
                            }
                            drop(permit);
                            if class.think_ms > 0.0 {
                                tokio::time::sleep(Duration::from_secs_f64(
                                    rng.exp(class.think_ms) / 1e3,
                                ))
                                .await;
                            }
                        }
                    }));
                }
            }
            first += class.tenants;
            tallies.push((*class, tally));
        }
        for h in handles {
            h.await.unwrap();
        }
        tallies
            .into_iter()
            .map(|(c, tally)| (c, tally.lock().unwrap().clone()))
            .collect::<Vec<_>>()
    });
    (out, wall.elapsed())
}

fn quantile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let i = ((sorted.len() as f64 * q).ceil() as usize).clamp(1, sorted.len()) - 1;
    sorted[i]
}

fn report(ledger: &str, profile: &Profile, out: &[(Class, Tally)], wall: Duration) {
    let secs = RUN.as_secs_f64();
    for (class, tally) in out {
        let mut w = tally.waits_ms.clone();
        w.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let asked = w.len();
        let admitted = asked - tally.refused;
        println!(
            "| {} | {} | {} | {} x {} | {:.2} | {:.2} | {:.1}% | {:.0} | {:.2} | {:.1} |",
            profile.name,
            ledger,
            class.label,
            class.tenants,
            class.clients,
            quantile(&w, 0.5),
            quantile(&w, 0.99),
            100.0 * tally.refused as f64 / asked.max(1) as f64,
            admitted as f64 / secs,
            tally.busy_ms / 1e3 / secs,
            wall.as_secs_f64(),
        );
    }
}

fn main() {
    let uniform = Class {
        label: "uniform",
        tenants: 200,
        clients: 1,
        service_ms: 20.0,
        think_ms: 0.0,
    };
    let heavy = Class {
        label: "heavy",
        tenants: 8,
        clients: 16,
        service_ms: 20.0,
        think_ms: 0.0,
    };
    let light = Class {
        label: "light",
        tenants: 192,
        clients: 1,
        service_ms: 20.0,
        think_ms: 1000.0,
    };
    let profiles = [
        Profile {
            name: "closed-2",
            permits: 2,
            classes: vec![uniform],
        },
        Profile {
            name: "closed-16",
            permits: 16,
            classes: vec![uniform],
        },
        Profile {
            name: "mixed-2",
            permits: 2,
            classes: vec![heavy, light],
        },
        Profile {
            name: "mixed-16",
            permits: 16,
            classes: vec![heavy, light],
        },
        Profile {
            name: "mixed-64",
            permits: 64,
            classes: vec![heavy, light],
        },
    ];
    println!("| profile | ledger | class | tenants x clients | wait p50 ms | wait p99 ms | refused | admitted/s | permits busy | wall s |");
    println!("|---|---|---|---|---|---|---|---|---|---|");
    for p in &profiles {
        let (out, wall) = run::<Baseline>(p);
        report("5.13.0", p, &out, wall);
        let (out, wall) = run::<Current>(p);
        report("S1", p, &out, wall);
    }
}
