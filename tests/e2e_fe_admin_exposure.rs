//! #292: the query-FE role must not publish an unauthenticated admin UI.
//!
//! `serve` is the one role an operator deliberately puts on a network - it is the query-FE half of the
//! plane split, and `docker-compose.scaled.yml` starts it as `serve --dir /nest --listen 0.0.0.0:8288`.
//! It was also the one role that derived its admin credential by reading `NUTHATCH_ADMIN_TOKEN`
//! directly instead of going through `indexer::admin_enabled` / `admin_required_token`. Unset, that
//! yields `admin_token: None`, and `serve::admin_authorized` reads `None` as *"localhost bind, open"* -
//! so `--admin` on a public bind served `/_admin/` to anyone, with a 200.
//!
//! The unit tests in `serve.rs` could not catch it: they build an `AppState` and set `admin_token`
//! themselves, which asserts the handler enforces a token it was handed, not that any role hands it
//! one. This suite therefore drives **`indexer::serve_role`** - the production entry point - and asks
//! the socket, because the wiring is the thing that was wrong.
//!
//! **The bind, `[0:0:0:0:0:0:0:1]`, is a deliberately unusual spelling of `::1` - not a different
//! address.** `serve::is_localhost` matches the literal host string (`127.0.0.1`, `::1`, `localhost`,
//! `[::1]`), so this fully-expanded form is off-localhost by the code's own definition while binding
//! the real IPv6 loopback: the off-localhost path gets exercised without this suite ever opening a
//! port to the network. An earlier revision used `127.0.0.2`, relying on Linux routing the whole
//! `127.0.0.0/8` block through `lo` - macOS aliases only `127.0.0.1` on `lo0`, so that bind failed
//! there with `EADDRNOTAVAIL` (#650). `::1` needs no aliasing on either platform, and Rust's
//! `SocketAddr` parser resolves the bracketed literal directly with no DNS/`getaddrinfo` involved, so
//! the trick is not OS-resolver-dependent either.

use std::time::{Duration, Instant};

use nuthatch::cli::ServeArgs;

mod common;

use common::tape::{scaffold_nest, USDC};

const POLL_TIMEOUT: Duration = Duration::from_secs(20);

/// `NUTHATCH_ADMIN_TOKEN` is process-wide and all four tests below depend on its value, so they take
/// turns rather than racing. Held across each test body - which spans awaits - hence the async mutex.
static ENV: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// RAII guard for `NUTHATCH_ADMIN_TOKEN`: clears it on drop, including when the drop is unwinding past
/// a panicking assertion. `tokio::sync::Mutex` does not poison, so a test that left the variable set on
/// panic used to hand it, live, to whichever test acquired [`ENV`] next (#436). Every test below builds
/// one immediately after locking `ENV` and holds it for the rest of the body, so the value is correct by
/// construction rather than by remembering to clear it before every `return`.
struct EnvGuard;

impl EnvGuard {
    fn set(value: &str) -> Self {
        std::env::set_var("NUTHATCH_ADMIN_TOKEN", value);
        EnvGuard
    }

    fn unset() -> Self {
        std::env::remove_var("NUTHATCH_ADMIN_TOKEN");
        EnvGuard
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        std::env::remove_var("NUTHATCH_ADMIN_TOKEN");
    }
}

/// #436, made concrete rather than left as a hypothetical "a fifth test would inherit s3cret": a panic
/// while an `EnvGuard` is alive must still clear `NUTHATCH_ADMIN_TOKEN`, or the next test to acquire
/// [`ENV`] starts with a stale credential already set. `std::panic::catch_unwind` is what makes the
/// unwind checkable in-process without failing this test itself.
#[tokio::test]
async fn env_guard_clears_the_token_even_when_a_panic_unwinds_through_it() {
    let _env = ENV.lock().await;
    let result = std::panic::catch_unwind(|| {
        let _token = EnvGuard::set("s3cret");
        panic!("simulated assertion failure, to prove EnvGuard's drop still fires");
    });
    assert!(
        result.is_err(),
        "premise: the closure must actually have panicked"
    );
    assert!(
        std::env::var("NUTHATCH_ADMIN_TOKEN").is_err(),
        "EnvGuard must clear the token on an unwinding drop, not just a normal return"
    );
}

/// A nest `serve_role` can load: `scaffold_nest` writes `rpc_urls = []`, and the FE constructs an
/// `RpcClient` it never polls, which refuses an empty URL list.
///
/// Also plants the local redb (#520): `serve` without `--hot-store` opens it non-creating now, so a
/// nest with no store indexed yet - what `scaffold_nest` alone leaves behind - is refused at startup
/// rather than served empty. A real deployment gets this from `dev` having run first; here it is a
/// bare `Store::open` (create semantics), standing in for that indexing.
fn scaffold_fe_nest(dir: &std::path::Path) -> nuthatch::config::Config {
    scaffold_nest(dir, "usdc", USDC);
    let toml_path = dir.join("nuthatch.toml");
    let toml = std::fs::read_to_string(&toml_path).unwrap();
    // Never dialled - the FE owns no cursor. It just has to parse.
    std::fs::write(
        &toml_path,
        toml.replace("rpc_urls = []", r#"rpc_urls = ["http://127.0.0.1:1"]"#),
    )
    .unwrap();
    nuthatch::store::Store::open(&dir.join(nuthatch::config::DB_FILE)).unwrap();
    nuthatch::config::Config::load(dir).unwrap()
}

/// A free port on `[0:0:0:0:0:0:0:1]` (real `::1`, spelled so `is_localhost` misses it - see the
/// module doc), released before we hand it to the FE.
async fn free_port() -> u16 {
    let probe = tokio::net::TcpListener::bind("[0:0:0:0:0:0:0:1]:0")
        .await
        .unwrap();
    probe.local_addr().unwrap().port()
}

/// Start `serve_role` on `[0:0:0:0:0:0:0:1]:<port>` with the admin UI requested, and wait until it
/// answers. Returns the base URL and the task, so the caller can abort it.
async fn start_fe(dir: &std::path::Path, admin: bool) -> (String, tokio::task::JoinHandle<()>) {
    let port = free_port().await;
    start_fe_on(dir, admin, format!("[0:0:0:0:0:0:0:1]:{port}")).await
}

/// [`start_fe`] on a bind of the caller's choosing.
async fn start_fe_on(
    dir: &std::path::Path,
    admin: bool,
    listen: String,
) -> (String, tokio::task::JoinHandle<()>) {
    let args = ServeArgs {
        cors: Vec::new(),
        dir: dir.to_string_lossy().into_owned(),
        listen: listen.clone(),
        hot_store: None,
        admin,
        publish_target: None,
        publish_interval: std::time::Duration::from_secs(60),
        publish_parallelism: 2,
    };
    let task = tokio::spawn(async move {
        // Runs until the process is signalled; the test aborts it instead.
        let _ = nuthatch::indexer::serve_role(args).await;
    });

    let base = format!("http://{listen}");
    let client = reqwest::Client::new();
    let start = Instant::now();
    while start.elapsed() < POLL_TIMEOUT {
        if let Ok(resp) = client.get(&base).send().await {
            if resp.status().is_success() {
                return (base, task);
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("FE never came up on {listen}");
}

/// The defect, stated as an assertion: `--admin` on an off-localhost bind with no token configured must
/// not serve the admin UI. Reverting `serve_role` to `admin_token_env()` turns this red with a 200.
#[tokio::test]
async fn the_fe_admin_ui_is_not_open_on_an_off_localhost_bind() {
    let _env = ENV.lock().await;
    // Removing it makes the "no token configured" premise true regardless of the developer's shell,
    // rather than the test quietly asserting something else on a machine that exports one.
    let _token = EnvGuard::unset();

    let dir = tempfile::tempdir().unwrap();
    scaffold_fe_nest(dir.path());
    let (base, task) = start_fe(dir.path(), true).await;

    let status = reqwest::Client::new()
        .get(format!("{base}/_admin/"))
        .send()
        .await
        .expect("admin request")
        .status();
    task.abort();

    assert_eq!(
        status.as_u16(),
        404,
        "an off-localhost FE with no NUTHATCH_ADMIN_TOKEN must not serve /_admin/ at all; got {status}"
    );
}

/// The other half, so the fix cannot be "disable the admin UI on the FE and call it secure": with a
/// token configured the surface is live and *gated*, which is what an operator behind a tunnel wants.
#[tokio::test]
async fn a_configured_token_gates_the_fe_admin_ui_rather_than_disabling_it() {
    let _env = ENV.lock().await;
    let _token = EnvGuard::set("s3cret");

    let dir = tempfile::tempdir().unwrap();
    scaffold_fe_nest(dir.path());
    let (base, task) = start_fe(dir.path(), true).await;

    let client = reqwest::Client::new();
    let without = client
        .get(format!("{base}/_admin/"))
        .send()
        .await
        .expect("admin request")
        .status();
    let with = client
        .get(format!("{base}/_admin/?token=s3cret"))
        .send()
        .await
        .expect("admin request")
        .status();
    task.abort();

    assert_eq!(without.as_u16(), 401, "no credential must be refused");
    assert_eq!(with.as_u16(), 200, "the configured token must be accepted");
}

/// A free port on the real `127.0.0.1`, released before we hand it to the FE.
async fn free_localhost_port() -> u16 {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    probe.local_addr().unwrap().port()
}

/// #2132: a token the operator set is required on a localhost bind too. Behind a reverse proxy every
/// request arrives from loopback, so an ignored token left the lifecycle API open to the proxy's users.
#[tokio::test]
async fn a_configured_token_is_required_on_a_localhost_bind_too() {
    let _env = ENV.lock().await;
    let _token = EnvGuard::set("s3cret");

    let dir = tempfile::tempdir().unwrap();
    scaffold_fe_nest(dir.path());
    let port = free_localhost_port().await;
    let (base, task) = start_fe_on(dir.path(), true, format!("127.0.0.1:{port}")).await;

    let client = reqwest::Client::new();
    let without = client
        .get(format!("{base}/_admin/"))
        .send()
        .await
        .expect("admin request")
        .status();
    let with = client
        .get(format!("{base}/_admin/"))
        .bearer_auth("s3cret")
        .send()
        .await
        .expect("admin request")
        .status();
    task.abort();

    assert_eq!(
        without.as_u16(),
        401,
        "a set NUTHATCH_ADMIN_TOKEN must not be ignored on 127.0.0.1"
    );
    assert_eq!(with.as_u16(), 200, "the configured token must be accepted");
}

/// #2136: the open (token-less) localhost surface answers only requests addressed to loopback. A
/// DNS-rebinding page reaches 127.0.0.1 under its own hostname, and its `Host` header gives it away.
#[tokio::test]
async fn the_open_localhost_admin_surface_refuses_a_foreign_host() {
    let _env = ENV.lock().await;
    let _token = EnvGuard::unset();

    let dir = tempfile::tempdir().unwrap();
    scaffold_fe_nest(dir.path());
    let port = free_localhost_port().await;
    let (base, task) = start_fe_on(dir.path(), true, format!("127.0.0.1:{port}")).await;

    let client = reqwest::Client::new();
    let get = |host: String, origin: Option<&'static str>| {
        let mut req = client
            .get(format!("{base}/_admin/"))
            .header(reqwest::header::HOST, host);
        if let Some(o) = origin {
            req = req.header(reqwest::header::ORIGIN, o);
        }
        async move { req.send().await.expect("admin request").status().as_u16() }
    };
    let local = get(format!("127.0.0.1:{port}"), None).await;
    let named = get(format!("localhost:{port}"), None).await;
    let rebound = get(format!("attacker.example:{port}"), None).await;
    let cross_origin = get(format!("127.0.0.1:{port}"), Some("http://attacker.example")).await;
    task.abort();

    assert_eq!(local, 200, "a loopback Host must still be served");
    assert_eq!(named, 200, "`localhost` is a loopback name");
    assert_ne!(rebound, 200, "a rebound hostname must be refused");
    assert_ne!(cross_origin, 200, "a foreign Origin must be refused");
}

/// The status line of one raw HTTP/1 exchange, for request shapes `reqwest` will not send.
async fn raw_status(port: u16, request: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    s.write_all(request.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(5), s.read_to_end(&mut buf)).await;
    String::from_utf8_lossy(&buf)
        .lines()
        .next()
        .unwrap_or("")
        .to_string()
}

/// #2136, over the real socket: a target named outside `Host` - absolute-form here, `:authority` in
/// HTTP/2 - is what the server routes on, so it is what the check must read; no target is refused.
#[tokio::test]
async fn the_open_localhost_admin_surface_checks_the_request_target_not_only_host() {
    let _env = ENV.lock().await;
    let _token = EnvGuard::unset();

    let dir = tempfile::tempdir().unwrap();
    scaffold_fe_nest(dir.path());
    let port = free_localhost_port().await;
    let (_base, task) = start_fe_on(dir.path(), true, format!("127.0.0.1:{port}")).await;

    let local = raw_status(
        port,
        &format!("GET /_admin/ HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"),
    )
    .await;
    let absolute = raw_status(
        port,
        &format!(
            "GET http://attacker.example:{port}/_admin/ HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\
             Connection: close\r\n\r\n"
        ),
    )
    .await;
    let no_host = raw_status(port, "GET /_admin/ HTTP/1.0\r\n\r\n").await;
    task.abort();

    assert!(
        local.contains(" 200 "),
        "premise: loopback is served: {local}"
    );
    assert!(
        absolute.contains(" 401 "),
        "a foreign absolute-form target must be refused: {absolute}"
    );
    assert!(
        no_host.contains(" 401 "),
        "a request naming no target must be refused: {no_host}"
    );
}

/// #292, the second half: `--no-admin` promises *"no routes"* (`cli.rs`, `DevArgs::no_admin`), and the
/// runtime's lifecycle half delivers exactly that - `lifecycle_routes` returns an empty `Router` when
/// the admin surface is off, so `/_admin/nests` is not a path that exists. The per-nest half of the
/// same surface only ever *answered* 404, from a handler that was mounted either way.
///
/// A disabled route that still replies in its own words is a different thing from a route that is not
/// there: it confirms to an unauthenticated caller that this is a nuthatch with the admin UI switched
/// off, which is the one fact `--no-admin` exists to withhold from a hosted deployment fronting its own
/// dashboard.
///
/// Stated as the property rather than as the body string, so it survives a reworded fallback: with the
/// admin surface disabled, every `/_admin*` path must be **indistinguishable** from a path that was
/// never registered. Re-mounting the routes unconditionally turns this red on the body.
///
/// **The token is set, not removed, and that is the whole point.** `admin_enabled` is
/// `!no_admin && (is_localhost || token_set)`, so on this suite's off-localhost `[0:0:0:0:0:0:0:1]` bind an
/// *unset* token disables the surface on its own - and a test that unset it passed just as well against
/// a `serve_role` that ignored `--no-admin` altogether (verified: replacing `admin_enabled(!args.admin,
/// ..)` with `admin_enabled(false, ..)` left this suite 3/3 green). Configuring a token leaves the flag
/// as the only thing that can still turn the surface off, which is the claim in this test's name.
#[tokio::test]
async fn a_disabled_admin_surface_is_route_free_not_merely_404() {
    let _env = ENV.lock().await;
    let _token = EnvGuard::set("s3cret");

    let dir = tempfile::tempdir().unwrap();
    scaffold_fe_nest(dir.path());
    // `admin: false` is the FE's spelling of `--no-admin`: both land on `admin_enabled == false` and
    // both compose the same `serve::router`, so the claim is tested where it is implemented.
    let (base, task) = start_fe(dir.path(), false).await;

    let client = reqwest::Client::new();
    let probe = |path: &'static str| {
        let client = client.clone();
        let url = format!("{base}{path}");
        async move {
            let resp = client.get(url).send().await.expect("probe request");
            (
                resp.status().as_u16(),
                resp.text().await.unwrap_or_default(),
            )
        }
    };

    // What an unregistered path looks like on this router - the baseline every admin path must match.
    let unrouted = probe("/_not_a_route_at_all").await;
    let admin = probe("/_admin").await;
    let admin_slash = probe("/_admin/").await;
    let admin_events = probe("/_admin/events").await;
    task.abort();

    assert_eq!(
        unrouted.0, 404,
        "premise: an unregistered path must 404, else the baseline means nothing"
    );
    for (path, got) in [
        ("/_admin", admin),
        ("/_admin/", admin_slash),
        ("/_admin/events", admin_events),
    ] {
        assert_eq!(
            got, unrouted,
            "with the admin surface disabled, {path} must be indistinguishable from an unregistered \
             path - got {got:?}, unregistered paths answer {unrouted:?}"
        );
    }
}

/// Sentinels planted in every config field that can carry a credential. Distinct per field, so a red
/// assertion names the leaking field rather than just "something leaked".
const RPC_KEY: &str = "rpckey-must-not-leak";
const WEBHOOK_PATH_SECRET: &str = "whpath-must-not-leak";
const WEBHOOK_HMAC_SECRET: &str = "whhmac-must-not-leak";
const ALERT_PATH_SECRET: &str = "alertpath-must-not-leak";

/// [`scaffold_fe_nest`] with a credential planted in every config field that can hold one: the RPC URL
/// (provider keys live in the path), a webhook URL (Slack/Discord style secret path), that webhook's
/// HMAC secret, and an alert sink URL.
///
/// `.invalid` is a reserved TLD that cannot resolve, so nothing here can be dialled by accident.
fn scaffold_fe_nest_with_secrets(dir: &std::path::Path) -> nuthatch::config::Config {
    scaffold_nest(dir, "usdc", USDC);
    let toml_path = dir.join("nuthatch.toml");
    let toml = std::fs::read_to_string(&toml_path).unwrap();
    let toml = toml.replace(
        "rpc_urls = []",
        &format!(r#"rpc_urls = ["https://rpc.invalid/v2/{RPC_KEY}"]"#),
    );
    std::fs::write(
        &toml_path,
        format!(
            r#"{toml}
[[webhooks]]
name = "transfers"
table = "usdc__transfer"
url = "https://hooks.invalid/services/T00/B00/{WEBHOOK_PATH_SECRET}"
secret = "{WEBHOOK_HMAC_SECRET}"

[[alerts]]
kinds = ["threshold_flag"]
url = "https://alerts.invalid/hook/{ALERT_PATH_SECRET}"
"#
        ),
    )
    .unwrap();
    // #520: `serve` opens the local redb non-creating now - plant it, same as `scaffold_fe_nest`.
    nuthatch::store::Store::open(&dir.join(nuthatch::config::DB_FILE)).unwrap();
    nuthatch::config::Config::load(dir).unwrap()
}

/// #292 item 1: **what the admin surface discloses once the credential has passed.**
///
/// Every prior pass on this issue stopped at the gate - they proved who may knock, never what is handed
/// over when the knock is answered. The open question was whether `/nest` and the `/_admin/events`
/// frames carry anything an operator would not knowingly give to whoever holds the admin token:
/// provider URLs with keys in them, webhook secrets, absolute filesystem locations.
///
/// The answer is meant to be "nothing", and part of it is already deliberate: `indexer::nest_info`
/// reduces each webhook URL to scheme+host via `webhook_host`. But that defence was pinned only by a
/// unit test on `webhook_host` itself, which asserts the redactor redacts and never that the served
/// payload uses it. That is the gap this closes: the sweep drives `indexer::serve_role` over a socket
/// and reads what the wire actually carries. Restoring the full `w.url` to `nest_info` turns it red.
///
/// **What this does not reach, stated so nobody inherits it as covered.** `serve::sanitize_sql_error`
/// rewrites the nest directory out of DuckDB errors, and the `/sql` and `/explain` probes below do
/// *not* exercise it: an unknown table is a Catalog Error carrying no path, and a table the registry
/// declares resolves against the hot store and succeeds. Making `sanitize_sql_error` a no-op leaves
/// this test green - verified, not assumed.
///
/// An earlier revision of this paragraph blamed the fixture: provoking the `IO Error` that names an
/// absolute parquet glob needs sealed segments this FE harness cannot build. That fixture was
/// afterwards built (the `e2e_solo` tape drives land -> seal), and the reason turns out to be
/// structural rather than a limit of this file - which is a better thing to inherit, so it is
/// recorded here instead:
///
/// - a sealed segment that is **missing** is filtered out of the glob list in
///   `analytics::define_views` before the DDL is assembled, with a `warn!`. No path is formatted, so
///   none can escape.
/// - a sealed segment that is **present but unreadable** throws while `read_parquet` binds the view,
///   and `define_views` catches that DDL failure, drops the segments that will not bind and rebuilds
///   the view from what remains (#419/#430 - before that fix it swallowed the failure at `debug!` and
///   the table vanished from `/sql` entirely). Either way the failing statement never returns to the
///   caller, so the path it carries is not formatted into a response.
/// - `analytics::run` opens a fresh in-memory connection per query, so views are defined and executed
///   inside one call. The window in which a view binds and then fails at execution against a file
///   that changed underneath it is one call wide, not the lifetime of a connection, and something
///   outside the process has to move the file to open it - nothing in-process removes a segment while
///   serving (`seal::verify_and_quarantine` is a startup pass).
///
/// So on the sealed-segment route `sanitize_sql_error` is defence-in-depth, not a live control - an
/// assertion here would be green whether or not the redactor is wired in, which is the vacuous
/// absence this comment exists to refuse. No request-reachable route on the bundled DuckDB has been
/// found that carries a path into it, which is why the coverage stays in `serve.rs`'s unit tests
/// alone. If you want a named lever rather than a negative, the honest one is the one-call window in
/// the third bullet above - not the `allowed_directories` lockdown, which an earlier revision of this
/// paragraph offered: `SET` errors do not echo the failing statement, the option accepts nonexistent
/// directories and empty lists so it barely fails at all, and
/// `analytics::tests::the_directory_lockdown_blocks_an_out_of_allowlist_file_read` records that the
/// second layer now does enforce (#289).
///
/// The absolute-path assertion below is therefore about the *payload* routes, not the error path, and
/// it has teeth there: adding the nest directory to `nest_info` turns it red.
///
/// The sweep is over every route the issue does *not* name, as well as the two it does. A secret
/// leaking from `/schema` is the same disclosure as one leaking from `/nest`, and probing the rest
/// costs nothing - whereas checking two routes would have proved two routes.
///
/// **The list is hand-maintained, and that is a real limit - do not read it as "the whole surface".**
/// `axum::Router` exposes no route table, so nothing here can enumerate what `serve::router`
/// registers. The list below was written against that function and covers all 24 of its GETs as of
/// this commit: the 19 base routes, plus `/_admin` and `/_admin/` (two distinct registrations of
/// `admin_index`), `/_admin/config` and `/_admin/storage`, plus `/_admin/events`, which is an SSE
/// stream and is probed separately. A route
/// added to `serve::router` and not added here is a route this sweep does not check, and no
/// assertion in this file will say so. An earlier revision claimed to enumerate the router while
/// omitting six of them - `/table/{name}`, `/entity/{id}`, `/q/{name}`, `/balance/{address}`,
/// `/exposure/{address}` and bare `/_admin` - which is the failure mode this paragraph exists to
/// stop repeating.
///
/// Deliberately run with a valid admin token: the question is what a credentialed caller receives, so
/// gating the surface is not an answer here.
#[tokio::test]
async fn the_admin_surface_discloses_no_credential_and_no_filesystem_path() {
    let _env = ENV.lock().await;
    let _token = EnvGuard::set("s3cret");

    let dir = tempfile::tempdir().unwrap();
    scaffold_fe_nest_with_secrets(dir.path());
    // The absolute paths a disclosure would embed, in *both* forms. `config.rs` and `indexer.rs`
    // canonicalise nothing, so the payload routes echo the raw `dir` they were handed, while DuckDB
    // and `std::fs` report the resolved one. The two are the same string only where TMPDIR resolves
    // without a symlink; on macOS (`/tmp` -> `/private/tmp`) they differ, and checking either alone
    // is decoration on that platform. Verified: with TMPDIR pointed at a symlink, the raw form is the
    // one the dir-publishing mutation trips.
    let raw_dir = dir.path().display().to_string();
    let nest_dir = dir.path().canonicalize().unwrap().display().to_string();
    let (base, task) = start_fe(dir.path(), true).await;

    let client = reqwest::Client::new();
    // Every GET the router registers, plus a deliberately failing `/sql` - the error path is where an
    // absolute path escapes if `sanitize_sql_error` is not actually wired in.
    let balance = format!("/balance/{USDC}");
    let exposure = format!("/exposure/{USDC}");
    let paths = [
        "/",
        "/nest",
        "/shape",
        "/health",
        "/ready",
        "/metrics",
        "/tables",
        "/schema",
        "/entities?limit=10",
        "/balances?limit=10",
        "/flags",
        "/queries",
        // Both SQL probes are here for the error *body*, not the happy path - a query error is the
        // classic place an absolute path escapes. See the note on `sanitize_sql_error` in this test's
        // doc comment for what this does and does not reach.
        "/sql?q=SELECT%20*%20FROM%20no_such_table",
        "/explain?q=SELECT%20*%20FROM%20no_such_table",
        "/sql?q=SELECT%20*%20FROM%20usdc__transfer",
        // The parameterised routes. They are here because a path parameter is the one input a caller
        // controls that a handler is most likely to echo back, and because `/entity/{id}` is the only
        // probe that reaches `analytics::get_row` - i.e. the sealed-segment path, which formats
        // absolute globs. `1-0` parses as (block 1, log 0) so it gets that far rather than being
        // rejected as malformed.
        "/table/usdc__transfer?limit=10",
        "/entity/1-0",
        "/q/transfers",
        balance.as_str(),
        exposure.as_str(),
        // Bare `/_admin`, distinct from the `/_admin/` above: they are two registered routes and
        // `admin_index` is reached by both.
        "/_admin?token=s3cret",
        // Serves the authored files' text, which is where the credentials are written down.
        "/_admin/config?token=s3cret",
        "/_admin/storage?token=s3cret&table=usdc__transfer",
    ];
    let mut bodies: Vec<(String, String)> = Vec::new();
    for p in paths {
        let resp = client
            .get(format!("{base}{p}"))
            .send()
            .await
            .unwrap_or_else(|e| panic!("GET {p}: {e}"));
        bodies.push((p.to_string(), resp.text().await.unwrap_or_default()));
    }

    // `/_admin/events` is an endless SSE stream, so take the first frame and stop. This is the payload
    // the issue singled out: it re-serves `summary_value`, the same body as `GET /`.
    let mut sse = client
        .get(format!("{base}/_admin/events?token=s3cret"))
        .send()
        .await
        .expect("SSE connect");
    assert_eq!(sse.status().as_u16(), 200, "the credentialed SSE must open");
    let frame = tokio::time::timeout(Duration::from_secs(10), sse.chunk())
        .await
        .expect("SSE frame within 10s")
        .expect("SSE chunk")
        .expect("the stream must push a frame, not close");
    bodies.push((
        "/_admin/events".to_string(),
        String::from_utf8_lossy(&frame).to_string(),
    ));
    drop(sse);
    task.abort();

    // Premise: the sweep actually read something *from every route it probed*. Without this a router
    // rename would silently reduce the test to asserting over empty strings, which passes for the
    // wrong reason. Named per path rather than counted: a count tolerates the one empty body that
    // matters, and reports "n of m" when what a reader needs is which one.
    let empty: Vec<&str> = bodies
        .iter()
        .filter(|(_, b)| b.is_empty())
        .map(|(p, _)| p.as_str())
        .collect();
    assert!(
        empty.is_empty(),
        "every probed route must return a payload, else the assertions below hold vacuously for it - \
         these answered with an empty body: {empty:?}"
    );
    // ...and that the nest whose secrets we planted is the one being served.
    let (_, nest_body) = bodies.iter().find(|(p, _)| p == "/nest").unwrap();
    assert!(
        nest_body.contains("hooks.invalid"),
        "premise: /nest must describe the webhook we configured, else the redaction assertion below \
         passes because the webhook is missing entirely - got {nest_body}"
    );

    for (path, body) in &bodies {
        for (label, secret) in [
            ("the RPC provider key", RPC_KEY),
            ("the webhook URL secret", WEBHOOK_PATH_SECRET),
            ("the webhook HMAC secret", WEBHOOK_HMAC_SECRET),
            ("the alert URL secret", ALERT_PATH_SECRET),
        ] {
            assert!(
                !body.contains(secret),
                "{path} disclosed {label} to a credentialed caller: {body}"
            );
        }
        for candidate in [&raw_dir, &nest_dir] {
            assert!(
                !body.contains(candidate),
                "{path} disclosed the nest's absolute filesystem path ({candidate}): {body}"
            );
        }
    }
}
