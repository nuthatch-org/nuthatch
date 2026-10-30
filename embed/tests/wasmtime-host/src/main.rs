//! RFC-0077 S2: the reference non-browser host for `abi: 1` (`embed/README.md`), and the test
//! harness that holds the plain module to it. wasmtime from Rust, the RPC over ureq, storage mode
//! `host` over files, the clock and randomness from the OS.
//!
//!   wasmtime-host abi <module.wasm> [--cache <dir>]
//!   wasmtime-host run <module.wasm> <rpc-url> <tab-parity-dir> <s3|s5> [--storage bytes|host]
//!                     [--cache <dir>] [--winch]
//!   wasmtime-host compile <module.wasm> <out.cwasm> [--serial] [--winch]
//!
//! `abi` fails a module whose shapes differ from its version's pinned table. `run` is the gate
//! `hosts/node/run.mjs` runs in Node, and prints the same fields. `compile` measures a cold compile.
//! Each prints one JSON line of what it measured.

mod abi;
mod mask;

use anyhow::{bail, ensure, Context, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::Read;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
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
    mib(bytes)
}

fn mib(bytes: f64) -> f64 {
    (bytes / (1 << 20) as f64 * 10.0).round() / 10.0
}

#[derive(Default)]
struct Opts {
    serial: bool,
    winch: bool,
    storage: Option<String>,
    cache: Option<PathBuf>,
    restart: bool,
    /// The child's half of `--restart`: the directory to follow into, then die.
    die_mid_follow: Option<PathBuf>,
}

impl Opts {
    fn parse(flags: &[&str]) -> Result<Opts> {
        let mut o = Opts::default();
        let mut it = flags.iter();
        while let Some(f) = it.next() {
            match *f {
                "--serial" => o.serial = true,
                "--winch" => o.winch = true,
                "--storage" => o.storage = it.next().map(|s| s.to_string()),
                "--cache" => o.cache = it.next().map(PathBuf::from),
                "--restart" => o.restart = true,
                "--die-mid-follow" => o.die_mid_follow = it.next().map(PathBuf::from),
                _ => bail!("unknown flag {f}"),
            }
        }
        Ok(o)
    }

    fn compiler(&self) -> &'static str {
        if self.winch {
            "winch"
        } else {
            "cranelift"
        }
    }

    fn cache(&self) -> PathBuf {
        self.cache
            .clone()
            .unwrap_or_else(|| std::env::temp_dir().join("nuthatch-embed-cwasm"))
    }
}

fn engine(o: &Opts) -> Result<Engine> {
    let mut config = Config::new();
    config.parallel_compilation(!o.serial);
    if o.winch {
        config.strategy(Strategy::Winch);
    }
    Ok(Engine::new(&config)?)
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// `wasm` compiled for `engine`, from the cache when this module was compiled by this wasmtime with
/// these settings before. The event build's compile peaks at 1.3 to 1.8 GiB (RFC-0077 §12); a load
/// from the cache stays near 120 MiB.
fn load(engine: &Engine, wasm: &[u8], cache: &Path) -> Result<(Module, &'static str)> {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    engine.precompile_compatibility_hash().hash(&mut h);
    let path = cache.join(format!("{}-{:016x}.cwasm", sha256_hex(wasm), h.finish()));
    if path.exists() {
        // Only ever a file this function wrote, named for its source and this engine's settings.
        return Ok((unsafe { Module::deserialize_file(engine, &path)? }, "hit"));
    }
    let module = Module::new(engine, wasm)?;
    std::fs::create_dir_all(cache)?;
    let partial = path.with_extension(format!("{}.partial", std::process::id()));
    std::fs::write(&partial, module.serialize()?)?;
    std::fs::rename(&partial, &path)?;
    Ok((module, "miss"))
}

/// Storage mode `host`: the nest's files under `root`, and the ones the guest holds open.
struct Host {
    root: PathBuf,
    files: HashMap<i32, File>,
    next: i32,
}

impl Host {
    fn path(&self, name: &str) -> Result<PathBuf> {
        ensure!(
            !name
                .split('/')
                .any(|p| p.is_empty() || p == "." || p == ".."),
            "refused storage name {name:?}"
        );
        Ok(self.root.join(name))
    }

    fn open(&mut self, name: &str) -> Result<i32> {
        let path = self.path(name)?;
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        self.next += 1;
        self.files.insert(self.next, file);
        Ok(self.next)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut names = Vec::new();
        let mut dirs = vec![self.root.clone()];
        while let Some(dir) = dirs.pop() {
            for e in std::fs::read_dir(&dir)? {
                let p = e?.path();
                if p.is_dir() {
                    dirs.push(p);
                } else {
                    let rel = p.strip_prefix(&self.root)?.to_string_lossy().into_owned();
                    if rel.starts_with(prefix) {
                        names.push(rel);
                    }
                }
            }
        }
        names.sort();
        Ok(names)
    }

    fn remove(&self, name: &str) -> Result<()> {
        let path = self.path(name)?;
        match std::fs::remove_file(&path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            r => Ok(r?),
        }
    }
}

struct Guest {
    store: Store<Host>,
    memory: Memory,
    alloc: TypedFunc<i32, i32>,
    free: TypedFunc<(i32, i32), ()>,
    output: TypedFunc<i32, ()>,
    instance: Instance,
}

impl Guest {
    fn put(&mut self, data: &[u8]) -> Result<(i32, i32)> {
        let ptr = self.alloc.call(&mut self.store, data.len() as i32)?;
        self.memory
            .write(&mut self.store, ptr as u32 as usize, data)?;
        Ok((ptr, data.len() as i32))
    }

    fn take(&mut self, n: i32) -> Result<Vec<u8>> {
        let len = n.unsigned_abs() as i32;
        let ptr = self.alloc.call(&mut self.store, len)?;
        self.output.call(&mut self.store, ptr)?;
        let mut buf = vec![0u8; len as usize];
        self.memory
            .read(&self.store, ptr as u32 as usize, &mut buf)?;
        self.free.call(&mut self.store, (ptr, len))?;
        if n < 0 {
            let f: Value = serde_json::from_slice(&buf)?;
            bail!("guest fault {}: {}", f["fault"], f["message"]);
        }
        Ok(buf)
    }

    fn json(&mut self, n: i32) -> Result<Value> {
        Ok(serde_json::from_slice(&self.take(n)?)?)
    }

    fn call0(&mut self, name: &str) -> Result<Value> {
        let f = self
            .instance
            .get_typed_func::<(), i32>(&mut self.store, name)?;
        let n = f.call(&mut self.store, ())?;
        self.json(n)
    }

    fn call1(&mut self, name: &str, arg: &[u8]) -> Result<Value> {
        let (p, l) = self.put(arg)?;
        let f = self
            .instance
            .get_typed_func::<(i32, i32), i32>(&mut self.store, name)?;
        let n = f.call(&mut self.store, (p, l))?;
        self.json(n)
    }

    fn request(&mut self, method: &str, target: &str, body: &str) -> Result<Value> {
        let (m, ml) = self.put(method.as_bytes())?;
        let (t, tl) = self.put(target.as_bytes())?;
        let (b, bl) = self.put(body.as_bytes())?;
        let f = self
            .instance
            .get_typed_func::<(i32, i32, i32, i32, i32, i32), i32>(&mut self.store, "nh_request")?;
        let n = f.call(&mut self.store, (m, ml, t, tl, b, bl))?;
        self.json(n)
    }

    fn export(&mut self) -> Result<Vec<u8>> {
        let f = self
            .instance
            .get_typed_func::<(), i32>(&mut self.store, "nh_export")?;
        let n = f.call(&mut self.store, ())?;
        self.take(n)
    }
}

fn memory_of(caller: &mut Caller<'_, Host>) -> Memory {
    caller
        .get_export("memory")
        .and_then(|e| e.into_memory())
        .expect("the module exports its memory")
}

/// `len` bytes of guest memory at `ptr`, and the host state beside them.
fn guest_bytes<'a>(
    caller: &'a mut Caller<'_, Host>,
    ptr: i32,
    len: i32,
) -> Option<(&'a mut [u8], &'a mut Host)> {
    let memory = memory_of(caller);
    let (data, host) = memory.data_and_store_mut(caller);
    let at = ptr as u32 as usize;
    let bytes = data.get_mut(at..at.checked_add(len as u32 as usize)?)?;
    Some((bytes, host))
}

/// A storage import's answer: the value, or -1 with the reason on stderr.
fn or_failed<T: From<i8>>(call: &str, r: Result<T>) -> T {
    r.unwrap_or_else(|e| {
        eprintln!("host storage: {call}: {e:#}");
        T::from(-1)
    })
}

fn link(engine: &Engine) -> Result<Linker<Host>> {
    let mut linker = Linker::new(engine);
    linker.func_wrap("nuthatch", "now_ms", || -> f64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0.0, |d| d.as_secs_f64() * 1000.0)
    })?;
    linker.func_wrap(
        "nuthatch",
        "random_fill",
        |mut caller: Caller<'_, Host>, ptr: i32, len: i32| {
            let (dest, _) = guest_bytes(&mut caller, ptr, len).expect("random_fill in bounds");
            File::open("/dev/urandom")
                .and_then(|mut f| f.read_exact(dest))
                .expect("/dev/urandom");
        },
    )?;
    linker.func_wrap(
        "nuthatch",
        "log",
        |mut caller: Caller<'_, Host>, level: i32, ptr: i32, len: i32| {
            if let Some((line, _)) = guest_bytes(&mut caller, ptr, len) {
                eprintln!("guest[{level}]: {}", String::from_utf8_lossy(line));
            }
        },
    )?;
    linker.func_wrap(
        "nuthatch",
        "store_read",
        |mut caller: Caller<'_, Host>, h: i32, at: i64, ptr: i32, len: i32| -> i32 {
            or_failed(
                "store_read",
                (|| {
                    let (buf, host) = guest_bytes(&mut caller, ptr, len).context("bounds")?;
                    let f = host.files.get(&h).context("no such handle")?;
                    Ok(f.read_at(buf, u64::try_from(at)?)? as i32)
                })(),
            )
        },
    )?;
    linker.func_wrap(
        "nuthatch",
        "store_write",
        |mut caller: Caller<'_, Host>, h: i32, at: i64, ptr: i32, len: i32| -> i32 {
            or_failed(
                "store_write",
                (|| {
                    let (buf, host) = guest_bytes(&mut caller, ptr, len).context("bounds")?;
                    let f = host.files.get(&h).context("no such handle")?;
                    Ok(f.write_at(buf, u64::try_from(at)?)? as i32)
                })(),
            )
        },
    )?;
    linker.func_wrap(
        "nuthatch",
        "store_len",
        |caller: Caller<'_, Host>, h: i32| -> i64 {
            or_failed(
                "store_len",
                (|| {
                    let f = caller.data().files.get(&h).context("no such handle")?;
                    Ok(f.metadata()?.len() as i64)
                })(),
            )
        },
    )?;
    linker.func_wrap(
        "nuthatch",
        "store_set_len",
        |caller: Caller<'_, Host>, h: i32, len: i64| -> i32 {
            or_failed(
                "store_set_len",
                (|| {
                    let f = caller.data().files.get(&h).context("no such handle")?;
                    f.set_len(u64::try_from(len)?)?;
                    Ok(0)
                })(),
            )
        },
    )?;
    linker.func_wrap(
        "nuthatch",
        "store_sync",
        |caller: Caller<'_, Host>, h: i32| -> i32 {
            or_failed(
                "store_sync",
                (|| {
                    let f = caller.data().files.get(&h).context("no such handle")?;
                    f.sync_data()?;
                    Ok(0)
                })(),
            )
        },
    )?;
    linker.func_wrap(
        "nuthatch",
        "store_close",
        |mut caller: Caller<'_, Host>, h: i32| {
            caller.data_mut().files.remove(&h);
        },
    )?;
    Ok(linker)
}

/// One `rpc` request as the host performs it, answered in the shape `nh_resume` takes.
fn rpc(
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
    let timed_out = |e: &ureq::Error| matches!(e, ureq::Error::Timeout(_));
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
                Err(e) => json!({"error": e.to_string(), "timed_out": timed_out(&e)}),
            }
        }
        Err(e) => json!({"error": e.to_string(), "timed_out": timed_out(&e)}),
    }
}

/// Every request of one `nh_step`, performed in order.
fn perform(
    host: &mut Host,
    agents: &mut HashMap<u64, ureq::Agent>,
    endpoints: &HashMap<&str, &str>,
    r: &Value,
) -> Value {
    let name = r["name"].as_str().unwrap_or_default();
    let failed = |e: anyhow::Error| json!({"error": format!("{e:#}")});
    match r["kind"].as_str().unwrap_or_default() {
        "rpc" => rpc(agents, endpoints, r),
        "open" => host.open(name).map_or_else(
            |e| json!({"error": format!("{e:#}"), "locked": false}),
            |h| json!({"handle": h}),
        ),
        "remove" => host.remove(name).map_or_else(failed, |()| json!({})),
        "list" => host
            .list(r["prefix"].as_str().unwrap_or_default())
            .map_or_else(failed, |names| json!({"names": names})),
        kind => json!({"error": format!("unsupported request kind: {kind}")}),
    }
}

fn instantiate(engine: &Engine, module: &Module, root: PathBuf) -> Result<Guest> {
    std::fs::create_dir_all(&root)?;
    let mut store = Store::new(
        engine,
        Host {
            root,
            files: HashMap::new(),
            next: 0,
        },
    );
    let instance = link(engine)?.instantiate(&mut store, module)?;
    Ok(Guest {
        memory: instance
            .get_memory(&mut store, "memory")
            .context("no exported memory")?,
        alloc: instance.get_typed_func(&mut store, "nh_alloc")?,
        free: instance.get_typed_func(&mut store, "nh_free")?,
        output: instance.get_typed_func(&mut store, "nh_output")?,
        instance,
        store,
    })
}

fn check_abi(wasm_path: &Path, o: &Opts) -> Result<()> {
    let engine = engine(o)?;
    let wasm = std::fs::read(wasm_path)?;
    let (module, cache) = load(&engine, &wasm, &o.cache())?;
    let shapes = abi::shapes(&module);
    let mut store = Store::new(&engine, ());
    let mut linker = Linker::new(&engine);
    // Read the version before judging the imports, so a changed import is reported, not a link error.
    linker.define_unknown_imports_as_traps(&module)?;
    let instance = linker.instantiate(&mut store, &module)?;
    let memory = instance
        .get_memory(&mut store, "memory")
        .context("no exported memory")?;
    let n = instance
        .get_typed_func::<(), i32>(&mut store, "nh_abi")?
        .call(&mut store, ())?;
    ensure!(n > 0, "nh_abi faulted");
    let ptr = instance
        .get_typed_func::<i32, i32>(&mut store, "nh_alloc")?
        .call(&mut store, n)?;
    instance
        .get_typed_func::<i32, ()>(&mut store, "nh_output")?
        .call(&mut store, ptr)?;
    let mut buf = vec![0u8; n as usize];
    memory.read(&store, ptr as u32 as usize, &mut buf)?;
    let reported: Value = serde_json::from_slice(&buf)?;
    let version = reported["abi"].as_u64().context("nh_abi has no `abi`")?;
    println!(
        "{}",
        json!({"module_sha256": sha256_hex(&wasm), "cache": cache, "nh_abi": reported, "shapes": shapes})
    );
    abi::check(&shapes, version)?;
    println!(
        "gate: {} exports and imports equal abi {version}'s pinned table",
        shapes.len()
    );
    Ok(())
}

fn compile(wasm: &Path, out: &Path, o: &Opts) -> Result<()> {
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
            "rss_before_compile_mib": before,
            "peak_rss_mib": peak,
            "cwasm_bytes": cwasm.len(),
        })
    );
    Ok(())
}

fn nest_files(dir: &Path) -> Result<serde_json::Map<String, Value>> {
    let nest = dir.join("s4");
    let mut files = serde_json::Map::new();
    files.insert(
        "entities.toml".into(),
        std::fs::read_to_string(nest.join("entities.toml"))?.into(),
    );
    for sub in ["entities", "views"] {
        let mut names: Vec<String> = std::fs::read_dir(nest.join(sub))?
            .map(|e| Ok(e?.file_name().to_string_lossy().into_owned()))
            .collect::<Result<_>>()?;
        names.sort();
        for f in names {
            files.insert(
                format!("{sub}/{f}"),
                std::fs::read_to_string(nest.join(sub).join(&f))?.into(),
            );
        }
    }
    Ok(files)
}

fn run(wasm_path: &Path, rpc_url: &str, dir: &Path, gate: &str, o: &Opts) -> Result<()> {
    ensure!(gate == "s3" || gate == "s5", "the gate is s3 or s5");
    let storage = o.storage.as_deref().unwrap_or("host");
    ensure!(
        storage == "bytes" || storage == "host",
        "storage is bytes or host"
    );
    ensure!(
        storage == "host" || !o.restart,
        "--restart reopens files: storage host"
    );
    let mut gz = String::new();
    flate2::read::GzDecoder::new(File::open(dir.join("chain.json.gz"))?).read_to_string(&mut gz)?;
    let chain: Value = serde_json::from_str(&gz)?;
    drop(gz);
    let first = chain["headers"][0][0]
        .as_str()
        .context("chain.headers[0][0]")?;
    let start = u64::from_str_radix(first.trim_start_matches("0x"), 16)? + 1;

    let engine = engine(o)?;
    let wasm = std::fs::read(wasm_path)?;
    let rss_start = peak_rss_mib();
    let t = Instant::now();
    let (module, cache) = load(&engine, &wasm, &o.cache())?;
    let ms_load = t.elapsed().as_millis();
    let rss_loaded = peak_rss_mib();
    let root = o.die_mid_follow.clone().unwrap_or_else(|| {
        std::env::temp_dir().join(format!("nest-wasmtime-host-{}", std::process::id()))
    });
    let killed = if o.restart {
        Some(kill_mid_follow(wasm_path, rpc_url, dir, gate, &root, o)?)
    } else {
        None
    };
    let t = Instant::now();
    let mut g = instantiate(&engine, &module, root.clone())?;
    let ms_instantiate = t.elapsed().as_millis();

    eprintln!("{}", g.call0("nh_abi")?);
    let mut config = json!({
        "nid": if gate == "s5" { "tab-answers" } else { "tab-parity" },
        "contract": chain["contract"], "start": start, "rpc": ["main"], "storage": storage,
    });
    if storage == "host" {
        config["fresh"] = (!o.restart).into();
    }
    if gate == "s5" {
        config["poll_interval_secs"] = 1.into();
        config["files"] = nest_files(dir)?.into();
    }
    g.call1("nh_open", config.to_string().as_bytes())?;

    let endpoints = HashMap::from([("main", rpc_url)]);
    let mut agents = HashMap::new();
    let (mut steps, mut waits, mut requests) = (0u64, 0u64, 0u64);
    let t = Instant::now();
    loop {
        steps += 1;
        let s = g.call0("nh_step")?;
        if s["idle"] == true {
            break;
        }
        if let Some(reqs) = s["requests"].as_array() {
            if o.die_mid_follow.is_some() && steps % 20 == 0 {
                let r = g.call0("nh_report")?;
                if r["segments"].as_object().is_some_and(|t| !t.is_empty()) {
                    // With these requests outstanding, as a crash would leave them.
                    let line = json!({"last_block": r["last_block"], "sealed_through": r["sealed_through"], "steps": steps});
                    std::io::Write::write_all(&mut std::io::stdout(), format!("{line}\n").as_bytes())?;
                    unsafe { libc::kill(libc::getpid(), libc::SIGKILL) };
                }
            }
            let answers: Vec<Value> = reqs
                .iter()
                .map(|r| {
                    requests += 1;
                    perform(g.store.data_mut(), &mut agents, &endpoints, r)
                })
                .collect();
            g.call1("nh_resume", Value::Array(answers).to_string().as_bytes())?;
        } else if let Some(ms) = s["wait_ms"].as_u64() {
            waits += 1;
            std::thread::sleep(Duration::from_millis(ms));
        } else {
            bail!("nh_step answered {s}");
        }
    }
    ensure!(
        o.die_mid_follow.is_none(),
        "the follow reached the tip before anything sealed"
    );
    let ms_follow = t.elapsed().as_millis();
    let got = g.call0("nh_report")?;
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
    let mut out = json!({
        "host": "wasmtime", "gate": gate, "storage": storage,
        "module_sha256": sha256_hex(&wasm),
        "compiler": o.compiler(), "cache": cache,
        "ms_load": ms_load, "ms_instantiate": ms_instantiate, "ms_follow": ms_follow,
        "rss_before_load_mib": rss_start, "rss_after_load_mib": rss_loaded,
        "wasm_memory_mib": mib(memory_bytes),
        "steps": steps, "waits": waits, "requests": requests, "rpc_requests": got["rpc_requests"],
        "segments": segs.len(), "hot_rows": got["hot_rows"],
        "segments_sha256": mask::segments_digest(&got["segments"]),
    });
    if let Some(k) = &killed {
        out["restart"] = true.into();
        out["killed_at"] = k.clone();
    }
    if storage == "bytes" {
        out["export_bytes"] = g.export()?.len().into();
    }

    let mut failures = Vec::new();
    if gate == "s3" {
        let expected: Value = serde_json::from_slice(&std::fs::read(dir.join("expected.json"))?)?;
        for (k, v) in expected.as_object().context("expected.json")? {
            if got[k.as_str()] != *v {
                failures.push(format!(
                    "report's {k}\n got: {}\nwant: {v}",
                    got[k.as_str()]
                ));
            }
        }
        let misnamed = segs.iter().filter(|s| s["hash"] != s["bytes_hash"]).count();
        if misnamed > 0 {
            failures.push(format!("{misnamed} segments named for other bytes"));
        }
    } else {
        let requests: Vec<Value> =
            serde_json::from_slice(&std::fs::read(dir.join("s5").join("requests.json"))?)?;
        let expected: Vec<Value> =
            serde_json::from_slice(&std::fs::read(dir.join("s5-expected.json"))?)?;
        let mut answers = Vec::new();
        for req in &requests {
            let (method, path) = (req["method"].as_str(), req["path"].as_str());
            let (method, path) = (method.unwrap_or_default(), path.unwrap_or_default());
            let a = g.request(method, path, req["body"].as_str().unwrap_or_default())?;
            answers.push(json!({
                "method": method, "path": path, "status": a["status"],
                "content_type": a["content_type"], "body": a["body"],
            }));
        }
        if answers.len() != expected.len() {
            failures.push(format!(
                "{} answers for {} pinned",
                answers.len(),
                expected.len()
            ));
        }
        for (a, want) in answers.iter().zip(&expected) {
            let masked = mask::mask(a["body"].as_str().unwrap_or_default());
            let same = ["method", "path", "status", "content_type"]
                .iter()
                .all(|k| a[*k] == want[*k])
                && want["body"] == masked.as_str();
            if !same {
                failures.push(format!(
                    "{} {}\n got: {} {masked}\nwant: {} {}",
                    want["method"], want["path"], a["status"], want["status"], want["body"]
                ));
            }
        }
        out["answers"] = answers.len().into();
        out["answers_sha256"] = mask::digest(&answers, false).into();
        out["pin_sha256"] = mask::digest(&expected, true).into();
    }
    out["peak_rss_mib"] = peak_rss_mib().into();
    println!("{out}");
    drop(g);
    let _ = std::fs::remove_dir_all(&root);
    if !failures.is_empty() {
        for f in &failures {
            eprintln!("MISMATCH {f}");
        }
        bail!("{} mismatches", failures.len());
    }
    let after = killed.map_or(String::new(), |k| {
        format!(
            " after a kill at block {}, sealed through {}",
            k["last_block"], k["sealed_through"]
        )
    });
    if gate == "s3" {
        println!(
            "gate: {} segment hashes and {} balances equal the node's{after}",
            segs.len(),
            got["balances"]["count"]
        );
    } else {
        println!(
            "gate: all {} answers equal the node's pin ({}){after}",
            out["answers"],
            out["answers_sha256"].as_str().unwrap_or_default()
        );
    }
    Ok(())
}

/// RFC-0077 S3: this host run again as a child that follows into `root` from an empty store until
/// something has sealed, and is killed with requests outstanding. Answers where it died.
fn kill_mid_follow(
    wasm: &Path,
    rpc: &str,
    dir: &Path,
    gate: &str,
    root: &Path,
    o: &Opts,
) -> Result<Value> {
    use std::os::unix::process::ExitStatusExt;
    let mut cmd = std::process::Command::new(std::env::current_exe()?);
    cmd.arg("run").arg(wasm).arg(rpc).arg(dir).arg(gate);
    cmd.arg("--die-mid-follow").arg(root).arg("--cache").arg(o.cache());
    if o.winch {
        cmd.arg("--winch");
    }
    let child = cmd.stderr(std::process::Stdio::inherit()).output()?;
    ensure!(
        child.status.signal() == Some(libc::SIGKILL),
        "the follow to kill ended {}, not by SIGKILL",
        child.status
    );
    let stdout = String::from_utf8_lossy(&child.stdout);
    let line = stdout.lines().last().context("the killed follow said nothing")?;
    Ok(serde_json::from_str(line)?)
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    match args.as_slice() {
        ["abi", wasm, flags @ ..] => check_abi(Path::new(wasm), &Opts::parse(flags)?),
        ["run", wasm, rpc, dir, gate, flags @ ..] => run(
            Path::new(wasm),
            rpc,
            Path::new(dir),
            gate,
            &Opts::parse(flags)?,
        ),
        ["compile", wasm, out, flags @ ..] => {
            compile(Path::new(wasm), Path::new(out), &Opts::parse(flags)?)
        }
        _ => bail!(
            "usage: wasmtime-host abi <module.wasm> | run <module.wasm> <rpc> <dir> <s3|s5> | compile <in.wasm> <out.cwasm>, then flags"
        ),
    }
}
