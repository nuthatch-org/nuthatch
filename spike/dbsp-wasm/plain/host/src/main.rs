//! RFC-0077 S0: the plain module in a non-browser host. wasmtime from Rust, the RPC over ureq, the
//! clock and randomness from the OS: the same imports `run-node.mjs` answers in Node.
//!
//!   plain-host compile <module.wasm> <out.cwasm> [--serial] [--winch]
//!   plain-host run <module.wasm|module.cwasm> <rpc-url> <tab-parity-dir> [--winch]
//!
//! Each prints one JSON line of what it measured; `run` exits non-zero unless the segment hashes and
//! the 1,000 balances equal RFC-0075 S3's pinned `expected.json`.

use anyhow::{bail, ensure, Context, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::Read;
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use wasmtime::{
    Caller, Config, Engine, Instance, Linker, Memory, Module, Store, Strategy, TypedFunc,
};

/// This process's peak resident set, in MiB.
fn peak_rss_mib() -> f64 {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    // Bytes on macOS, KiB on Linux.
    let bytes = if cfg!(target_os = "macos") {
        ru.ru_maxrss as f64
    } else {
        ru.ru_maxrss as f64 * 1024.0
    };
    bytes / (1 << 20) as f64
}

fn mib(n: f64) -> f64 {
    (n * 10.0).round() / 10.0
}

/// `--serial` compiles on one thread; `--winch` uses wasmtime's baseline compiler, not Cranelift.
#[derive(Clone, Copy)]
struct Opts {
    serial: bool,
    winch: bool,
}

impl Opts {
    fn parse(flags: &[&str]) -> Result<Opts> {
        let mut o = Opts {
            serial: false,
            winch: false,
        };
        for f in flags {
            match *f {
                "--serial" => o.serial = true,
                "--winch" => o.winch = true,
                _ => bail!("unknown flag {f}"),
            }
        }
        Ok(o)
    }

    fn compiler(self) -> &'static str {
        if self.winch {
            "winch"
        } else {
            "cranelift"
        }
    }
}

fn engine(o: Opts) -> Result<Engine> {
    let mut config = Config::new();
    config.parallel_compilation(!o.serial);
    if o.winch {
        config.strategy(Strategy::Winch);
    }
    Ok(Engine::new(&config)?)
}

fn compile(wasm: &Path, out: &Path, o: Opts) -> Result<()> {
    let engine = engine(o)?;
    let bytes = std::fs::read(wasm)?;
    let before = peak_rss_mib();
    let t = Instant::now();
    let module = Module::new(&engine, &bytes)?;
    let ms = t.elapsed().as_millis();
    let peak = peak_rss_mib();
    let cwasm = module.serialize()?;
    std::fs::write(out, &cwasm)?;
    println!(
        "{}",
        json!({
            "wasmtime": "48",
            "compiler": o.compiler(),
            "parallel": !o.serial,
            "threads": std::thread::available_parallelism().map_or(1, |n| n.get()),
            "module_bytes": bytes.len(),
            "ms_compile": ms,
            "rss_before_compile_mib": mib(before),
            "peak_rss_mib": mib(peak),
            "cwasm_bytes": cwasm.len(),
        })
    );
    Ok(())
}

struct Guest {
    store: Store<()>,
    memory: Memory,
    alloc: TypedFunc<i32, i32>,
    free: TypedFunc<(i32, i32), ()>,
    output: TypedFunc<i32, ()>,
    instance: Instance,
}

impl Guest {
    fn put(&mut self, data: &[u8]) -> Result<(i32, i32)> {
        let ptr = self.alloc.call(&mut self.store, data.len() as i32)?;
        self.memory.write(&mut self.store, ptr as usize, data)?;
        Ok((ptr, data.len() as i32))
    }

    fn take(&mut self, n: i32) -> Result<String> {
        let len = n.unsigned_abs() as i32;
        let ptr = self.alloc.call(&mut self.store, len)?;
        self.output.call(&mut self.store, ptr)?;
        let mut buf = vec![0u8; len as usize];
        self.memory.read(&self.store, ptr as usize, &mut buf)?;
        self.free.call(&mut self.store, (ptr, len))?;
        let s = String::from_utf8(buf)?;
        if n < 0 {
            bail!("{s}");
        }
        Ok(s)
    }

    fn call0(&mut self, name: &str) -> Result<Value> {
        let f = self
            .instance
            .get_typed_func::<(), i32>(&mut self.store, name)?;
        let n = f.call(&mut self.store, ())?;
        Ok(serde_json::from_str(&self.take(n)?)?)
    }

    fn call1(&mut self, name: &str, arg: &[u8]) -> Result<Value> {
        let (p, l) = self.put(arg)?;
        let f = self
            .instance
            .get_typed_func::<(i32, i32), i32>(&mut self.store, name)?;
        let n = f.call(&mut self.store, (p, l))?;
        Ok(serde_json::from_str(&self.take(n)?)?)
    }

    fn request(&mut self, method: &str, target: &str, body: &str) -> Result<Value> {
        let (m, ml) = self.put(method.as_bytes())?;
        let (t, tl) = self.put(target.as_bytes())?;
        let (b, bl) = self.put(body.as_bytes())?;
        let f = self
            .instance
            .get_typed_func::<(i32, i32, i32, i32, i32, i32), i32>(&mut self.store, "nh_request")?;
        let n = f.call(&mut self.store, (m, ml, t, tl, b, bl))?;
        Ok(serde_json::from_str(&self.take(n)?)?)
    }
}

fn memory_of(caller: &mut Caller<'_, ()>) -> Memory {
    caller
        .get_export("memory")
        .and_then(|e| e.into_memory())
        .expect("the module exports its memory")
}

fn link(engine: &Engine) -> Result<Linker<()>> {
    let mut linker = Linker::new(engine);
    linker.func_wrap("nuthatch", "now_ms", || -> f64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0.0, |d| d.as_secs_f64() * 1000.0)
    })?;
    linker.func_wrap(
        "nuthatch",
        "random_fill",
        |mut caller: Caller<'_, ()>, ptr: i32, len: i32| {
            let memory = memory_of(&mut caller);
            let dest = &mut memory.data_mut(&mut caller)[ptr as usize..][..len as usize];
            std::fs::File::open("/dev/urandom")
                .and_then(|mut f| f.read_exact(dest))
                .expect("/dev/urandom");
        },
    )?;
    linker.func_wrap(
        "nuthatch",
        "log",
        |mut caller: Caller<'_, ()>, level: i32, ptr: i32, len: i32| {
            let memory = memory_of(&mut caller);
            let line = &memory.data(&caller)[ptr as usize..][..len as usize];
            eprintln!("guest[{level}]: {}", String::from_utf8_lossy(line));
        },
    )?;
    Ok(linker)
}

/// One request as the host performs it, answered in the shape `nh_resume` takes.
fn perform(
    agents: &mut HashMap<u64, ureq::Agent>,
    endpoints: &HashMap<&str, &str>,
    r: &Value,
) -> Value {
    let deadline = r["deadline_ms"].as_u64().unwrap_or(20_000);
    let agent = agents.entry(deadline).or_insert_with(|| {
        ureq::Agent::config_builder()
            .http_status_as_error(false)
            .timeout_global(Some(Duration::from_millis(deadline)))
            .build()
            .into()
    });
    let Some(url) = endpoints.get(r["endpoint"].as_str().unwrap_or_default()) else {
        return json!({"error": format!("no endpoint named {}", r["endpoint"])});
    };
    let sent = agent
        .post(*url)
        .header("content-type", "application/json")
        .send(r["body"].as_str().unwrap_or_default());
    match sent {
        Ok(mut res) => {
            let status = res.status();
            let retry_after = res
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            match res
                .body_mut()
                .with_config()
                .limit(u64::MAX)
                .read_to_string()
            {
                Ok(body) => json!({
                    "status": status.as_u16(),
                    "status_text": status.canonical_reason().unwrap_or(""),
                    "retry_after": retry_after,
                    "body": body,
                }),
                Err(e) => {
                    json!({"error": e.to_string(), "timed_out": matches!(e, ureq::Error::Timeout(_))})
                }
            }
        }
        Err(e) => {
            json!({"error": e.to_string(), "timed_out": matches!(e, ureq::Error::Timeout(_))})
        }
    }
}

fn run(module_path: &Path, rpc: &str, dir: &Path, o: Opts) -> Result<()> {
    let mut gz = String::new();
    flate2::read::GzDecoder::new(std::fs::File::open(dir.join("chain.json.gz"))?)
        .read_to_string(&mut gz)?;
    let chain: Value = serde_json::from_str(&gz)?;
    drop(gz);
    let expected: Value = serde_json::from_slice(&std::fs::read(dir.join("expected.json"))?)?;
    let requests: Value =
        serde_json::from_slice(&std::fs::read(dir.join("s5").join("requests.json"))?)?;
    let first = chain["headers"][0][0]
        .as_str()
        .context("chain.headers[0][0]")?;
    let start = u64::from_str_radix(first.trim_start_matches("0x"), 16)? + 1;

    let engine = engine(o)?;
    let rss_start = peak_rss_mib();
    let t = Instant::now();
    let precompiled = module_path.extension().is_some_and(|e| e == "cwasm");
    let module = if precompiled {
        // Only ever a file `compile` wrote, with this same wasmtime.
        unsafe { Module::deserialize_file(&engine, module_path)? }
    } else {
        Module::from_file(&engine, module_path)?
    };
    let ms_load = t.elapsed().as_millis();
    let rss_loaded = peak_rss_mib();
    let mut store = Store::new(&engine, ());
    let t = Instant::now();
    let instance = link(&engine)?.instantiate(&mut store, &module)?;
    let ms_instantiate = t.elapsed().as_millis();
    let memory = instance
        .get_memory(&mut store, "memory")
        .context("no exported memory")?;
    let mut g = Guest {
        alloc: instance.get_typed_func(&mut store, "nh_alloc")?,
        free: instance.get_typed_func(&mut store, "nh_free")?,
        output: instance.get_typed_func(&mut store, "nh_output")?,
        memory,
        instance,
        store,
    };

    eprintln!("{}", g.call0("nh_abi")?);
    let config = json!({"nid": "tab-parity", "contract": chain["contract"], "start": start, "rpc": ["main"]});
    g.call1("nh_open", config.to_string().as_bytes())?;

    let endpoints = HashMap::from([("main", rpc)]);
    let mut agents = HashMap::new();
    let (mut steps, mut waits, mut posts) = (0u64, 0u64, 0u64);
    let t = Instant::now();
    loop {
        steps += 1;
        let s = g.call0("nh_step")?;
        if s["idle"] == true {
            break;
        }
        if let Some(reqs) = s["requests"].as_array() {
            let answers: Vec<Value> = reqs
                .iter()
                .map(|r| {
                    posts += 1;
                    perform(&mut agents, &endpoints, r)
                })
                .collect();
            g.call1("nh_resume", Value::Array(answers).to_string().as_bytes())?;
        } else if let Some(ms) = s["wait_ms"].as_u64() {
            waits += 1;
            std::thread::sleep(Duration::from_millis(ms));
        } else {
            bail!("unexpected step: {s}");
        }
    }
    let ms_follow = t.elapsed().as_millis();
    let got = g.call0("nh_report")?;

    let mut digest = Sha256::new();
    let mut answered = 0;
    for req in requests.as_array().context("requests.json")? {
        let path = req["path"].as_str().unwrap_or_default();
        if path.starts_with("/ready") {
            continue;
        }
        let method = req["method"].as_str().unwrap_or_default();
        let a = g.request(method, path, req["body"].as_str().unwrap_or_default())?;
        digest.update(format!(
            "{method} {path}\n{}\n{}\n{}\n",
            a["status"],
            a["content_type"].as_str().unwrap_or_default(),
            a["body"].as_str().unwrap_or_default()
        ));
        answered += 1;
    }
    let memory_bytes = g
        .instance
        .get_typed_func::<(), f64>(&mut g.store, "nh_memory_bytes")?
        .call(&mut g.store, ())?;

    let segs: Vec<&Value> = got["segments"]
        .as_object()
        .map(|t| {
            t.values()
                .flat_map(|v| v.as_array().into_iter().flatten())
                .collect()
        })
        .unwrap_or_default();
    println!(
        "{}",
        json!({
            "host": "wasmtime",
            "compiler": o.compiler(),
            "precompiled": precompiled,
            "ms_load": ms_load,
            "ms_instantiate": ms_instantiate,
            "ms_follow": ms_follow,
            "rss_before_load_mib": mib(rss_start),
            "rss_after_load_mib": mib(rss_loaded),
            "peak_rss_mib": mib(peak_rss_mib()),
            "wasm_memory_mib": mib(memory_bytes / (1 << 20) as f64),
            "steps": steps, "waits": waits, "rpc_posts": posts, "rpc_requests": got["rpc_requests"],
            "segments": segs.len(), "hot_rows": got["hot_rows"], "holders": got["balances"]["holders"],
            "answers": answered,
            "answers_sha256": format!("{:x}", digest.finalize()),
        })
    );

    let misnamed = segs.iter().filter(|s| s["hash"] != s["bytes_hash"]).count();
    let mismatched: Vec<&String> = expected
        .as_object()
        .context("expected.json")?
        .iter()
        .filter(|(k, v)| got[k.as_str()] != **v)
        .map(|(k, _)| k)
        .collect();
    ensure!(
        mismatched.is_empty() && misnamed == 0,
        "MISMATCH in {mismatched:?} ({misnamed} misnamed segments)\n got: {got}"
    );
    println!(
        "gate: {} segment hashes and {} balances equal the node's",
        segs.len(),
        got["balances"]["count"]
    );
    Ok(())
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["compile", wasm, out, flags @ ..] => {
            compile(Path::new(wasm), Path::new(out), Opts::parse(flags)?)
        }
        ["run", module, rpc, dir, flags @ ..] => {
            run(Path::new(module), rpc, Path::new(dir), Opts::parse(flags)?)
        }
        _ => bail!("usage: plain-host compile <in.wasm> <out.cwasm> | run <module> <rpc> <dir>, then [--serial] [--winch]"),
    }
}
