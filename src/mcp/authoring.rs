//! `nuthatch mcp --authoring`: the tools that take an agent from an empty directory to a served nest.
//! Each runs this binary's own `init` and `dev`, exactly as a person would type them, so nothing an
//! agent sends reaches the data path except as those commands' arguments.

use anyhow::{anyhow, bail, Context, Result};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// `init` resolves an ABI and probes for the deployment block, which a slow public RPC can drag out.
const INIT_TIMEOUT: Duration = Duration::from_secs(300);

/// How long `start_indexing` waits for the API to answer before reporting that it has not yet.
const SERVING_WAIT: Duration = Duration::from_secs(30);

/// How long `stop_indexing` gives `dev` to shut down on SIGTERM.
const STOP_WAIT: Duration = Duration::from_secs(10);

/// The most of a command's output or log a tool answer carries.
const OUTPUT_TAIL: usize = 8 * 1024;

const TOOLS: &[&str] = &[
    "init_nest",
    "start_indexing",
    "index_status",
    "stop_indexing",
];

pub fn is_tool(name: &str) -> bool {
    TOOLS.contains(&name)
}

pub fn tool_specs() -> Vec<Value> {
    vec![
        json!({ "name": "init_nest", "title": "Create a nest",
          "description": "Create a nuthatch nest that indexes one EVM contract's onchain events into SQL tables: resolves the contract's ABI (Sourcify, then Etherscan-class explorers) and its deployment block, and writes the nest into the directory this server was started on. Call it once, then `start_indexing`.",
          "inputSchema": { "type": "object", "properties": {
            "address": { "type": "string", "description": "The contract address, 0x and 40 hex digits." },
            "chain": { "type": "string", "description": "The chain, e.g. mainnet, base, arbitrum-one. Omit to have nuthatch find the chain the contract is deployed on." },
            "alias": { "type": "string", "description": "The table prefix, e.g. `usdc` gives `usdc__transfer`. Defaults to the contract's name." },
            "rpc": { "type": "string", "description": "An RPC URL to use instead of the chain's public endpoints." },
            "abi": { "type": "string", "description": "A local ABI JSON file to use instead of resolving one, e.g. a proxy's implementation ABI." },
            "start_block": { "type": "integer", "description": "The deployment block, to skip probing for it." } },
            "required": ["address"] },
          "annotations": { "title": "Create a nest", "readOnlyHint": false, "destructiveHint": false, "idempotentHint": false, "openWorldHint": true } }),
        json!({ "name": "start_indexing", "title": "Start indexing",
          "description": "Start indexing the EVM contract events of the nest `init_nest` created: runs `nuthatch dev` on it, serving the query tools at this server's URL, for as long as this server runs. Returns once the API answers; follow progress with `index_status`.",
          "inputSchema": { "type": "object", "properties": {
            "backfill": { "type": "integer", "description": "Index only this many blocks back from the tip, instead of from deployment." },
            "rpc": { "type": "string", "description": "An RPC URL to index from instead of the nest's own." },
            "poll_interval": { "type": "integer", "description": "Seconds between polls for new blocks once caught up (default 300)." } } },
          "annotations": { "title": "Start indexing", "readOnlyHint": false, "destructiveHint": false, "idempotentHint": true, "openWorldHint": true } }),
        json!({ "name": "index_status", "title": "Indexing progress",
          "description": "Whether the local nuthatch indexer of EVM contract events is serving and caught up: its `/ready` answer (tip, last indexed and sealed block, lag), and the indexer's state and recent log when `start_indexing` started it.",
          "inputSchema": { "type": "object", "properties": {} },
          "annotations": { "title": "Indexing progress", "readOnlyHint": true, "openWorldHint": false } }),
        json!({ "name": "stop_indexing", "title": "Stop indexing",
          "description": "Stop indexing EVM contract events into this directory's nest: stops the nuthatch indexer `start_indexing` started, including one left running by an earlier server. The nest and its indexed data stay; `start_indexing` resumes where it stopped.",
          "inputSchema": { "type": "object", "properties": {} },
          "annotations": { "title": "Stop indexing", "readOnlyHint": false, "destructiveHint": false, "idempotentHint": true, "openWorldHint": false } }),
    ]
}

/// The nest directory the tools act on and the indexer they supervise.
pub struct Authoring {
    dir: PathBuf,
    exe: PathBuf,
    base: String,
    listen: String,
    log: PathBuf,
    /// The indexer's pid and nest, outside the nest (every file in it counts toward its NID), so a
    /// server started after this one dies can find and stop what it left running.
    pidfile: PathBuf,
    client: reqwest::Client,
    indexer: tokio::sync::Mutex<Option<tokio::process::Child>>,
}

impl Authoring {
    /// `base` must be on this machine: `start_indexing` serves the nest there.
    pub fn new(dir: PathBuf, base: &str) -> Result<Self> {
        let listen = loopback_listen(base)?;
        let exe = std::env::current_exe().context("finding this nuthatch binary")?;
        Ok(Self::with_exe(dir, exe, base, listen))
    }

    fn with_exe(dir: PathBuf, exe: PathBuf, base: &str, listen: String) -> Self {
        let log = std::env::temp_dir().join(format!("nuthatch-mcp-{}-dev.log", std::process::id()));
        let key: String = listen
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect();
        let pidfile = std::env::temp_dir().join(format!("nuthatch-mcp-dev-{key}.pid"));
        Authoring {
            dir,
            exe,
            base: base.trim_end_matches('/').to_string(),
            listen,
            log,
            pidfile,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("a client with only a timeout builds"),
            indexer: tokio::sync::Mutex::new(None),
        }
    }

    pub async fn call(&self, name: &str, args: &Value) -> Result<String> {
        match name {
            "init_nest" => self.init_nest(args).await,
            "start_indexing" => self.start_indexing(args).await,
            "index_status" => self.index_status().await,
            "stop_indexing" => self.stop_indexing().await,
            other => bail!("unknown tool `{other}`"),
        }
    }

    async fn init_nest(&self, args: &Value) -> Result<String> {
        if self.dir.join("nuthatch.toml").exists() {
            bail!(
                "{} already holds a nest; call `start_indexing` to index it",
                self.dir.display()
            );
        }
        let address = args["address"]
            .as_str()
            .ok_or_else(|| anyhow!("`address` is required"))?;
        if !is_address(address) {
            bail!("`address` must be 0x followed by 40 hex digits, not {address:?}");
        }
        let mut cmd = tokio::process::Command::new(&self.exe);
        cmd.arg("init").arg(address).arg(flag("dir", &self.dir));
        if let Some(chain) = args["chain"].as_str() {
            if !chain
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            {
                bail!("`chain` is a chain name such as mainnet or base, not {chain:?}");
            }
            cmd.arg(format!("--chain={chain}"));
        }
        if let Some(alias) = args["alias"].as_str() {
            if alias.is_empty()
                || !alias
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_')
            {
                bail!("`alias` is letters, digits and underscores, not {alias:?}");
            }
            cmd.arg(format!("--alias={alias}"));
        }
        if let Some(rpc) = args["rpc"].as_str() {
            cmd.arg(format!("--rpc={}", rpc_url(rpc)?));
        }
        if let Some(abi) = args["abi"].as_str() {
            // Checked here, so a path to some other file is refused without `init` quoting it back.
            let is_abi = std::fs::read(abi)
                .ok()
                .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
                .is_some_and(|v| v.as_array().is_some_and(|a| a.iter().all(Value::is_object)));
            if !is_abi {
                bail!("`abi` must be a readable JSON ABI file: an array of ABI entries");
            }
            cmd.arg(flag("abi", Path::new(abi)));
        }
        if let Some(block) = args["start_block"].as_u64() {
            cmd.arg(format!("--start-block={block}"));
        }
        std::fs::create_dir_all(&self.dir)
            .with_context(|| format!("creating {}", self.dir.display()))?;
        cmd.stdin(std::process::Stdio::null()).kill_on_drop(true);
        let out = tokio::time::timeout(INIT_TIMEOUT, cmd.output())
            .await
            .map_err(|_| anyhow!("`nuthatch init` took longer than {INIT_TIMEOUT:?}"))?
            .context("running `nuthatch init`")?;
        let said = tail(&format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        ));
        if !out.status.success() {
            bail!("`nuthatch init` failed ({}):\n{said}", out.status);
        }
        Ok(format!(
            "Created the nest in {}.\n{said}\nNext: call `start_indexing`.",
            self.dir.display()
        ))
    }

    async fn start_indexing(&self, args: &Value) -> Result<String> {
        let mut indexer = self.indexer.lock().await;
        if let Some(child) = indexer.as_mut() {
            if child.try_wait()?.is_none() {
                return Ok(format!(
                    "Already indexing, serving at {}. Call `index_status` for progress.",
                    self.base
                ));
            }
        }
        if !self.dir.join("nuthatch.toml").exists() {
            bail!(
                "{} holds no nest yet; call `init_nest` first",
                self.dir.display()
            );
        }
        if self.serving().await {
            if let Some(pid) = self.left_running() {
                return Ok(format!(
                    "Already indexing, serving at {}: an earlier server started it (pid {pid}). \
                     Call `index_status` for progress, or `stop_indexing` to stop it.",
                    self.base
                ));
            }
            bail!(
                "something already answers at {}; stop it, or start this server with --url \
                 pointed at it and use the query tools",
                self.base
            );
        }
        let mut cmd = tokio::process::Command::new(&self.exe);
        cmd.arg("dev")
            .arg(flag("dir", &self.dir))
            .arg(format!("--listen={}", self.listen));
        if let Some(n) = args["backfill"].as_u64() {
            cmd.arg(format!("--backfill={n}"));
        }
        if let Some(rpc) = args["rpc"].as_str() {
            cmd.arg(format!("--rpc={}", rpc_url(rpc)?));
        }
        if let Some(secs) = args["poll_interval"].as_u64() {
            cmd.arg(format!("--poll-interval={}", secs.max(1)));
        }
        let log = std::fs::File::create(&self.log)
            .with_context(|| format!("creating {}", self.log.display()))?;
        // stdout is this server's protocol channel, so the indexer must never write to it.
        cmd.stdin(std::process::Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log);
        let mut child = cmd.spawn().context("starting `nuthatch dev`")?;
        if let Some(pid) = child.id() {
            let _ = std::fs::write(&self.pidfile, format!("{pid}\n{}\n", self.dir.display()));
        }
        let deadline = tokio::time::Instant::now() + SERVING_WAIT;
        loop {
            if let Some(status) = child.try_wait()? {
                bail!(
                    "`nuthatch dev` exited ({status}):\n{}",
                    tail(&std::fs::read_to_string(&self.log).unwrap_or_default())
                );
            }
            if self.serving().await {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                *indexer = Some(child);
                return Ok(format!(
                    "Indexing started, but {} has not answered within {SERVING_WAIT:?}. Call \
                     `index_status` to follow it; its log is {}.",
                    self.base,
                    self.log.display()
                ));
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        *indexer = Some(child);
        Ok(format!(
            "Indexing, serving at {}; the query tools answer from it now and fill in as the \
             backfill runs. Call `index_status` for progress. Its log is {}.",
            self.base,
            self.log.display()
        ))
    }

    async fn index_status(&self) -> Result<String> {
        let indexer = {
            let mut guard = self.indexer.lock().await;
            match guard.as_mut() {
                None => match self.left_running() {
                    Some(pid) => {
                        json!({ "running": true, "pid": pid, "started_by": "an earlier server" })
                    }
                    None => json!("not started by this server"),
                },
                Some(child) => match child.try_wait()? {
                    None => json!({ "running": true, "pid": child.id() }),
                    Some(status) => json!({
                        "running": false,
                        "exit": status.to_string(),
                        "log": tail(&std::fs::read_to_string(&self.log).unwrap_or_default()),
                    }),
                },
            }
        };
        let url = format!("{}/ready", self.base);
        let ready = match self.client.get(&url).send().await {
            Ok(resp) => {
                let body = resp.text().await.unwrap_or_default();
                serde_json::from_str::<Value>(&body).unwrap_or(Value::String(body))
            }
            Err(_) => json!(format!(
                "nothing answers at {}; call `start_indexing`",
                self.base
            )),
        };
        // Lifted from `/ready` so an agent reads "serving but empty" without digging (#3022).
        let has_rows = ready.get("has_rows").cloned().unwrap_or(Value::Null);
        let first_block_indexed = ready
            .get("first_block_indexed")
            .cloned()
            .unwrap_or(Value::Null);
        Ok(json!({
            "url": self.base,
            "ready": ready,
            "has_rows": has_rows,
            "first_block_indexed": first_block_indexed,
            "indexer": indexer,
        })
        .to_string())
    }

    async fn stop_indexing(&self) -> Result<String> {
        let mut indexer = self.indexer.lock().await;
        if let Some(mut child) = indexer.take() {
            if child.try_wait()?.is_none() {
                // `dev` shuts down cleanly on SIGTERM; SIGKILL only if it will not.
                if let Some(pid) = child.id() {
                    // SAFETY: a plain signal to the child this server spawned and still holds.
                    unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
                }
                if tokio::time::timeout(STOP_WAIT, child.wait()).await.is_err() {
                    child.kill().await.context("stopping `nuthatch dev`")?;
                }
                let _ = std::fs::remove_file(&self.pidfile);
                return Ok(
                    "Stopped the indexer. `start_indexing` resumes where it stopped.".into(),
                );
            }
        }
        // A pid outlives its process: after a crash or a reboot it may name something else, so it
        // is only signalled while the nest it was recorded for is still being served.
        let pid = match self.left_running() {
            Some(pid) if self.serving().await => pid,
            _ => {
                let _ = std::fs::remove_file(&self.pidfile);
                return Ok("No indexer started for this directory is running.".into());
            }
        };
        // SAFETY: a plain signal to the pid recorded for this directory, whose nest still answers.
        unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
        let deadline = tokio::time::Instant::now() + STOP_WAIT;
        while self.serving().await && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let _ = std::fs::remove_file(&self.pidfile);
        Ok(format!(
            "Stopped the indexer an earlier server left running (pid {pid}). `start_indexing` \
             resumes where it stopped."
        ))
    }

    /// The pid recorded for this directory, if that process is still alive.
    fn left_running(&self) -> Option<u32> {
        let recorded = std::fs::read_to_string(&self.pidfile).ok()?;
        let mut lines = recorded.lines();
        let pid: u32 = lines.next()?.trim().parse().ok()?;
        (lines.next()? == self.dir.display().to_string()).then_some(())?;
        // SAFETY: signal 0 only asks whether the pid exists and may be signalled.
        (unsafe { libc::kill(pid as libc::pid_t, 0) } == 0).then_some(pid)
    }

    async fn serving(&self) -> bool {
        self.client
            .get(format!("{}/health", self.base))
            .send()
            .await
            .is_ok_and(|r| r.status().is_success())
    }
}

/// The indexer stops with a server that exits normally, by SIGTERM so `dev` shuts down cleanly. A
/// server killed outright runs no drop; the pidfile is what lets the next one find the indexer.
impl Drop for Authoring {
    fn drop(&mut self) {
        if let Some(child) = self.indexer.get_mut().as_mut() {
            if let (Ok(None), Some(pid)) = (child.try_wait(), child.id()) {
                // SAFETY: a plain signal to the child this server spawned and still holds.
                unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
                let _ = std::fs::remove_file(&self.pidfile);
            }
        }
    }
}

/// `--name=value`, so a value beginning with `-` is never read as a flag.
fn flag(name: &str, value: &Path) -> std::ffi::OsString {
    let mut s = std::ffi::OsString::from(format!("--{name}="));
    s.push(value);
    s
}

fn is_address(s: &str) -> bool {
    s.strip_prefix("0x")
        .is_some_and(|h| h.len() == 40 && h.bytes().all(|b| b.is_ascii_hexdigit()))
}

fn rpc_url(s: &str) -> Result<&str> {
    let ok =
        reqwest::Url::parse(s).is_ok_and(|u| matches!(u.scheme(), "http" | "https" | "ws" | "wss"));
    if !ok {
        bail!("`rpc` must be an http(s) or ws(s) URL");
    }
    Ok(s)
}

/// The `--listen` address for `base`, which must be a loopback URL with no path: the indexer this
/// server starts is served on this machine, at the root.
fn loopback_listen(base: &str) -> Result<String> {
    let url = reqwest::Url::parse(base).with_context(|| format!("--url {base}"))?;
    let name = url.host_str().unwrap_or_default();
    let ip = name
        .trim_matches(|c| c == '[' || c == ']')
        .parse::<std::net::IpAddr>();
    let host = match ip {
        _ if name == "localhost" => "127.0.0.1".to_string(),
        Ok(std::net::IpAddr::V4(ip)) if ip.is_loopback() => ip.to_string(),
        Ok(std::net::IpAddr::V6(ip)) if ip.is_loopback() => format!("[{ip}]"),
        _ => bail!(
            "--authoring runs `nuthatch dev` on this machine, so --url must be a loopback address \
             such as http://127.0.0.1:8288, not {base}"
        ),
    };
    if url.scheme() != "http" || !matches!(url.path(), "" | "/") {
        bail!("--authoring serves one nest at the root of --url, so it must be http://host:port, not {base}");
    }
    let port = url.port_or_known_default().unwrap_or(8288);
    Ok(format!("{host}:{port}"))
}

/// The last `OUTPUT_TAIL` bytes of `s`, cut on a character boundary.
fn tail(s: &str) -> String {
    let s = s.trim();
    if s.len() <= OUTPUT_TAIL {
        return s.to_string();
    }
    let mut at = s.len() - OUTPUT_TAIL;
    while !s.is_char_boundary(at) {
        at += 1;
    }
    format!("…{}", &s[at..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authoring_needs_a_loopback_root_url() {
        assert_eq!(
            loopback_listen("http://127.0.0.1:8288").unwrap(),
            "127.0.0.1:8288"
        );
        assert_eq!(
            loopback_listen("http://localhost:9000/").unwrap(),
            "127.0.0.1:9000"
        );
        assert_eq!(loopback_listen("http://[::1]:8288").unwrap(), "[::1]:8288");
        for refused in [
            "http://10.0.0.5:8288",
            "https://nest.example.com",
            "http://127.0.0.1:8288/usdc",
            "https://127.0.0.1:8288",
        ] {
            assert!(loopback_listen(refused).is_err(), "{refused}");
        }
    }

    #[test]
    fn an_argument_cannot_pose_as_a_flag() {
        assert!(!is_address("--from=https://evil.example"));
        assert!(!is_address("0x123"));
        assert!(is_address("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"));
        assert!(rpc_url("--state-rpc=x").is_err());
        assert_eq!(flag("abi", Path::new("-x.json")), "--abi=-x.json");
    }

    #[tokio::test]
    async fn the_tools_refuse_to_run_out_of_order() {
        let dir = tempfile::tempdir().unwrap();
        let a = Authoring::with_exe(
            dir.path().to_path_buf(),
            PathBuf::from("/nonexistent/nuthatch"),
            "http://127.0.0.1:1",
            "127.0.0.1:1".into(),
        );
        let err = a.call("start_indexing", &json!({})).await.unwrap_err();
        assert!(err.to_string().contains("call `init_nest` first"), "{err}");
        let status: Value =
            serde_json::from_str(&a.call("index_status", &json!({})).await.unwrap()).unwrap();
        assert_eq!(status["indexer"], "not started by this server", "{status}");
        assert!(
            status["ready"].as_str().unwrap().contains("start_indexing"),
            "{status}"
        );

        std::fs::write(dir.path().join("nuthatch.toml"), "").unwrap();
        let err = a
            .call(
                "init_nest",
                &json!({ "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48" }),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("already holds a nest"), "{err}");
    }

    /// A path to a file that is not an ABI is refused here, so `init` never gets to quote it back.
    #[tokio::test]
    async fn an_abi_that_is_not_one_is_refused_without_echoing_it() {
        let dir = tempfile::tempdir().unwrap();
        let secret = dir.path().join("secret.txt");
        std::fs::write(&secret, "hunter2 is the password").unwrap();
        let a = Authoring::with_exe(
            dir.path().join("nest"),
            PathBuf::from("/nonexistent/nuthatch"),
            "http://127.0.0.1:1",
            "127.0.0.1:1".into(),
        );
        let err = a
            .call(
                "init_nest",
                &json!({ "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                         "abi": secret.to_str().unwrap() }),
            )
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("must be a readable JSON ABI"), "{err}");
        assert!(!err.contains("hunter2"), "{err}");
    }

    /// A server that died leaves its indexer running; the next one adopts it, and can stop it. A
    /// recorded pid whose nest no longer answers is never signalled: it may be another process now.
    #[tokio::test]
    async fn an_indexer_an_earlier_server_left_running_is_adopted_and_stopped() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("nuthatch.toml"), "").unwrap();
        let orphan = std::process::Command::new("sleep")
            .arg("60")
            .spawn()
            .unwrap();
        let pid = orphan.id();
        let gone = std::sync::Arc::new(AtomicBool::new(false));
        let (tx, exited) = std::sync::mpsc::channel();
        {
            let gone = gone.clone();
            let mut orphan = orphan;
            std::thread::spawn(move || {
                let status = orphan.wait().unwrap();
                gone.store(true, Ordering::SeqCst);
                let _ = tx.send(status);
            });
        }
        let at = |port: u16| {
            Authoring::with_exe(
                dir.path().to_path_buf(),
                PathBuf::from("/nonexistent/nuthatch"),
                &format!("http://127.0.0.1:{port}"),
                format!("127.0.0.1:{port}"),
            )
        };
        let record = |a: &Authoring| {
            std::fs::write(&a.pidfile, format!("{pid}\n{}\n", dir.path().display())).unwrap()
        };

        // Nothing listens on a port freed at once, so the nest is not served.
        let dead = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let stale = at(dead.local_addr().unwrap().port());
        drop(dead);
        record(&stale);
        let said = stale.call("stop_indexing", &json!({})).await.unwrap();
        assert!(said.contains("No indexer"), "{said}");
        assert!(
            !gone.load(Ordering::SeqCst),
            "a pid whose nest is not served is not signalled"
        );
        assert!(!stale.pidfile.exists(), "the stale record is cleared");

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a = at(listener.local_addr().unwrap().port());

        let health = axum::Router::new().route(
            "/health",
            axum::routing::get(move || {
                let up = !gone.load(Ordering::SeqCst);
                async move {
                    if up {
                        axum::http::StatusCode::OK
                    } else {
                        axum::http::StatusCode::SERVICE_UNAVAILABLE
                    }
                }
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, health).await });
        record(&a);
        let said = a.call("start_indexing", &json!({})).await.unwrap();
        assert!(said.contains("an earlier server started it"), "{said}");
        let status: Value =
            serde_json::from_str(&a.call("index_status", &json!({})).await.unwrap()).unwrap();
        assert_eq!(status["indexer"]["pid"], pid, "{status}");

        let said = a.call("stop_indexing", &json!({})).await.unwrap();
        server.abort();
        assert!(said.contains("earlier server left running"), "{said}");
        let status = exited.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(
            !status.success(),
            "the orphan was signalled, not left to finish"
        );
        assert!(!a.pidfile.exists());
    }
}
