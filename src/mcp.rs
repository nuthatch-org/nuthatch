//! MCP server, compiled into the binary. `nuthatch mcp` speaks the Model Context Protocol over
//! stdio (newline-delimited JSON-RPC), so a coding agent (Claude Code, Cursor, …) can query a
//! running index directly. It is a thin, fully-offline bridge to the local HTTP API of a running
//! `nuthatch dev` - no external calls, no telemetry, no gated data service. Nothing phones home.
//!
//! Bridging (rather than reopening the redb/segments) means the MCP server never contends with the
//! indexer for the single-writer store, and it automatically reflects the live IVM views.

use anyhow::{anyhow, bail, Result};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

const PROTOCOL_VERSION: &str = "2025-06-18";
/// Above the node's own SQL timeout and permit wait; the stdio loop is sequential, so one endless
/// response would otherwise wedge the session (#2217).
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

/// The MCP server is meant to be launched by an MCP *client* (Claude Code, Cursor, …) as a stdio
/// subprocess. When a human runs `nuthatch mcp` in a terminal, stdin is a TTY and no client is
/// driving it - so instead of silently blocking on a read that never comes, show how to wire it up.
/// Returns true if we short-circuited (printed guidance and should exit).
fn guide_if_interactive(base: &str) -> bool {
    use std::io::IsTerminal;
    if !std::io::stdin().is_terminal() {
        return false; // a client is piping JSON-RPC in - run the server.
    }
    eprintln!("`nuthatch mcp` is a stdio server for an AI client to launch, not to run by hand.\n");
    print_client_config(base);
    true
}

/// Emit a copy-paste MCP client configuration for this binary bridging to `base`. One documented
/// command wires a coding agent to a running nest (RFC-0015 slice 5): print it, paste it, ask your
/// contract's data in plain English - fully offline, nothing phones home.
pub fn print_client_config(base: &str) {
    // Prefer this binary's absolute path so the snippet works even off `PATH`; fall back to the bare
    // command name.
    let exe = std::env::current_exe()
        .ok()
        .and_then(|p| p.to_str().map(str::to_owned))
        .unwrap_or_else(|| "nuthatch".to_string());

    let snippet = client_config_json(&exe, base);

    println!("First run your index:   nuthatch dev");
    println!("Then point an AI client at it - either way is one step:\n");
    println!("  Claude Code (one-liner):");
    println!("    claude mcp add nuthatch -- {exe} mcp --url {base}\n");
    println!("  Or add to .mcp.json / your client's MCP config:");
    println!(
        "{}",
        serde_json::to_string_pretty(&snippet).unwrap_or_default()
    );
    println!(
        "\nThen ask your agent: \"what are the top USDC holders?\" - it queries the nest over MCP."
    );
}

/// The MCP-client server entry for this binary bridging to `base` - the value that goes under
/// `mcpServers.nuthatch` in a client's config.
fn client_config_json(exe: &str, base: &str) -> Value {
    json!({
        "mcpServers": {
            "nuthatch": {
                "command": exe,
                "args": ["mcp", "--url", base]
            }
        }
    })
}

/// One message in either direction. A `/sql` answer is capped at 64 MiB already, so nothing legitimate
/// is larger, and an unbounded line is an unbounded allocation (#1661).
const MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

/// The next newline-delimited message, or `None` at end of input. A line longer than `max` is read
/// through to its newline and dropped rather than held.
async fn next_message<R: tokio::io::AsyncBufRead + Unpin>(
    r: &mut R,
    max: usize,
) -> std::io::Result<Option<String>> {
    let mut line = Vec::new();
    let mut oversized = false;
    loop {
        let buf = r.fill_buf().await?;
        if buf.is_empty() {
            return Ok((!line.is_empty() && !oversized)
                .then(|| String::from_utf8_lossy(&line).into_owned()));
        }
        let (take, done) = match buf.iter().position(|&b| b == b'\n') {
            Some(i) => (i + 1, true),
            None => (buf.len(), false),
        };
        if !oversized {
            if line.len() + take > max {
                oversized = true;
                line = Vec::new();
            } else {
                line.extend_from_slice(&buf[..take]);
            }
        }
        r.consume(take);
        if done {
            if oversized {
                eprintln!("nuthatch mcp: dropped a message over {max} bytes");
                oversized = false;
                continue;
            }
            return Ok(Some(String::from_utf8_lossy(&line).into_owned()));
        }
    }
}

/// Run the stdio MCP loop, bridging tool calls to `base` (a running `nuthatch dev` HTTP API).
pub async fn serve(base: String) -> Result<()> {
    if guide_if_interactive(&base) {
        return Ok(());
    }
    let client = http_client()?;
    let mut stdin = BufReader::new(tokio::io::stdin());
    let mut stdout = tokio::io::stdout();
    while let Some(line) = next_message(&mut stdin, MAX_MESSAGE_BYTES).await? {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(req) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if let Some(resp) = handle(&req, &client, &base).await {
            stdout
                .write_all(serde_json::to_string(&resp)?.as_bytes())
                .await?;
            stdout.write_all(b"\n").await?;
            stdout.flush().await?;
        }
    }
    Ok(())
}

/// Dispatch one JSON-RPC message. Returns None for notifications (no response expected).
async fn handle(req: &Value, client: &reqwest::Client, base: &str) -> Option<Value> {
    let id = req.get("id").cloned();
    match req.get("method").and_then(Value::as_str).unwrap_or("") {
        "initialize" => Some(ok(id?, initialize_result(req))),
        "notifications/initialized" => None,
        "ping" => Some(ok(id?, json!({}))),
        "tools/list" => Some(ok(
            id?,
            json!({ "tools": tool_specs(&fetch_shape(client, base).await) }),
        )),
        "tools/call" => {
            let id = id?;
            let params = req.get("params").cloned().unwrap_or_else(|| json!({}));
            match call_tool(&params, client, base).await {
                Ok(text) => Some(ok(id, content(&text, false))),
                Err(e) => Some(ok(id, content(&format!("{e:#}"), true))),
            }
        }
        // Resources (RFC-0016 §6): a client can preload the schema/tables/status context without
        // burning a tool call. Each maps to an HTTP GET on the running nest.
        "resources/list" => Some(ok(id?, json!({ "resources": resource_specs() }))),
        "resources/read" => {
            let id = id?;
            let uri = req
                .pointer("/params/uri")
                .and_then(Value::as_str)
                .unwrap_or("");
            match read_resource(uri, client, base).await {
                Ok(text) => Some(ok(
                    id,
                    json!({ "contents": [{ "uri": uri, "mimeType": "text/plain", "text": text }] }),
                )),
                Err(e) => Some(err(id, -32602, &format!("{e:#}"))),
            }
        }
        // Prompts (RFC-0016 §6): canned, argument-taking analysis flows that name real tools.
        "prompts/list" => Some(ok(
            id?,
            json!({ "prompts": prompt_specs(&fetch_shape(client, base).await) }),
        )),
        "prompts/get" => {
            let id = id?;
            let name = req
                .pointer("/params/name")
                .and_then(Value::as_str)
                .unwrap_or("");
            let args = req
                .pointer("/params/arguments")
                .cloned()
                .unwrap_or_else(|| json!({}));
            match render_prompt(name, &args) {
                Some(result) => Some(ok(id, result)),
                None => Some(err(id, -32602, &format!("unknown prompt `{name}`"))),
            }
        }
        _ => Some(err(id?, -32601, "method not found")),
    }
}

fn initialize_result(req: &Value) -> Value {
    // Echo the client's requested protocol version when present.
    let pv = req
        .pointer("/params/protocolVersion")
        .and_then(Value::as_str)
        .unwrap_or(PROTOCOL_VERSION);
    json!({
        "protocolVersion": pv,
        // Advertise exactly what we implement - tools, resources, prompts; nothing else. When a future
        // standing-queries RFC lands, `notifications` slots in here without breaking a client (§6).
        "capabilities": { "tools": {}, "resources": {}, "prompts": {} },
        "serverInfo": { "name": "nuthatch", "version": env!("CARGO_PKG_VERSION") },
    })
}

/// The resources a client may preload (RFC-0016 §6). Stable `nuthatch://…` URIs backed by the running
/// nest's HTTP surface - reading one is a GET, so a client gets the context without a tool round-trip.
fn resource_specs() -> Value {
    json!([
        { "uri": "nuthatch://schema", "name": "schema", "mimeType": "text/plain",
          "description": "The enriched data model: tables, meaning, footguns, and the hot/cold coverage seam." },
        { "uri": "nuthatch://tables", "name": "tables", "mimeType": "application/json",
          "description": "Every decoded table with its columns, Solidity types, and topic0." },
        { "uri": "nuthatch://status", "name": "status", "mimeType": "application/json",
          "description": "Index status: chain, contracts, last & sealed block." },
    ])
}

/// Resolve a `nuthatch://…` resource URI to its HTTP-backed content.
async fn read_resource(uri: &str, client: &reqwest::Client, base: &str) -> Result<String> {
    let path = match uri {
        "nuthatch://schema" => "/schema",
        "nuthatch://tables" => "/tables",
        "nuthatch://status" => "/",
        other => bail!("unknown resource `{other}`"),
    };
    get(client, &format!("{base}{path}")).await
}

/// A nest's capability *shape* (RFC-0025): which capability-gated surfaces are live. Fetched from the
/// running nest so the advertised tool/prompt surface matches what will actually return data, instead
/// of advertising every tool to every nest and letting the inert ones return `{"count":0}`.
struct Shape {
    /// A transfer-shaped decoder exists (the balance view's own gate) - `balance`/`top_balances`.
    transfers: bool,
    /// RFC-0008 compliance is configured - `flags`/`exposure` and `investigate-address`.
    compliance: bool,
}

/// Fetch the nest's shape from `GET /shape`. If the endpoint is missing (an older nest) or unreachable,
/// default to advertising **everything** - never hide a tool because a probe failed. The safe default
/// is the current, unconditional behaviour, so a stale nest degrades to exactly today's surface.
async fn fetch_shape(client: &reqwest::Client, base: &str) -> Shape {
    let all = Shape {
        transfers: true,
        compliance: true,
    };
    let Ok(body) = get(client, &format!("{base}/shape")).await else {
        return all;
    };
    let Ok(v) = serde_json::from_str::<Value>(&body) else {
        return all;
    };
    Shape {
        transfers: v.get("transfers").and_then(Value::as_bool).unwrap_or(true),
        compliance: v.get("compliance").and_then(Value::as_bool).unwrap_or(true),
    }
}

/// The argument-taking prompts (RFC-0016 §6) - canned analysis flows that name real tools. Rendered
/// entirely client-side (no network), so they work the instant a client lists them. `investigate-address`
/// is compliance-shaped (exposure/flags), so it's advertised only where that surface is live
/// (RFC-0025) - otherwise following it would walk an agent into no-ops.
fn prompt_specs(shape: &Shape) -> Value {
    let mut prompts = vec![
        json!({ "name": "profile-contract", "description": "An activity overview of the indexed contract(s).",
          "arguments": [] }),
    ];
    if shape.compliance {
        prompts.push(json!({ "name": "investigate-address", "description": "Balances, exposure and flags for one address.",
          "arguments": [ { "name": "address", "description": "The 0x address to investigate.", "required": true } ] }));
    }
    prompts.push(json!({ "name": "verify-a-number", "description": "Re-derive a figure from scratch with provenance.",
          "arguments": [ { "name": "claim", "description": "The number/claim to verify.", "required": true } ] }));
    Value::Array(prompts)
}

/// Render a prompt into MCP `prompts/get` result form (a list of user-role messages). Returns `None`
/// for an unknown prompt name.
fn render_prompt(name: &str, args: &Value) -> Option<Value> {
    let text = match name {
        "profile-contract" => "Give me an activity overview of this nest. First call `schema` to see \
            the tables and their meaning, then use `sql` to summarise: total events per table, the \
            block range covered, and the busiest addresses. Cite the provenance stamp in your answer."
            .to_string(),
        "investigate-address" => {
            let a = args.get("address").and_then(Value::as_str).unwrap_or("<address>");
            format!(
                "Investigate the address {a}. Use `balance` for its token balance, `exposure` for its \
                 exposure to labeled addresses, and `flags` for threshold/velocity flags. Then \
                 summarise the risk picture, citing blocks."
            )
        }
        "verify-a-number" => {
            let c = args.get("claim").and_then(Value::as_str).unwrap_or("<the claim>");
            format!(
                "Independently verify this claim: \"{c}\". Call `schema` first (mind the footguns: \
                 big-int *amounts* use the `_dec` companion, `SUM(value_dec)` is the values that \
                 fit, and `WHERE NOT value_overflow` is that same sum; ids, nonces and hashes stay \
                 on the raw column because `_dec` is NULL for a full-width uint256). Write the SQL \
                 from scratch with `sql`, and report the result *with* its provenance stamp (as-of \
                 block, sealed_through) so it is citable. If your first query errors, use the \
                 returned hint to correct it."
            )
        }
        _ => return None,
    };
    Some(json!({
        "messages": [ { "role": "user", "content": { "type": "text", "text": text } } ]
    }))
}

/// The tool surface. Not a thin single-endpoint wrapper - schema discovery, SQL, point-reads, and
/// the IVM views, each with an LLM-friendly description.
fn tool_specs(shape: &Shape) -> Value {
    // The seven generic tools - meaningful on every nest, so always advertised.
    let mut tools = vec![
        json!({ "name": "status", "description": "Index status: contract, chain, transfers indexed, holders, last & sealed block.",
          "inputSchema": { "type": "object", "properties": {} } }),
        json!({ "name": "schema", "description": "The data model - how tables/views are named and queried. Read this first, then `tables` for the exact tables and columns.",
          "inputSchema": { "type": "object", "properties": {} } }),
        json!({ "name": "tables", "description": "List every decoded table (`{alias}__{event}`) with its columns, Solidity types, and topic0.",
          "inputSchema": { "type": "object", "properties": {} } }),
        json!({ "name": "table", "description": "Recent rows of one table, merged across the hot tip and sealed segments.",
          "inputSchema": { "type": "object", "properties": { "name": { "type": "string" }, "limit": { "type": "integer", "default": 50 } }, "required": ["name"] } }),
        json!({ "name": "sql", "description": "Run a read-only SQL query over the live tip ∪ sealed history. Each event is a view named `{alias}__{event}` (e.g. \"usdc__transfer\") with block_number, log_index, tx_hash, address + the event's params. Call `schema` first. SELECT/WITH only. Returns a compact table + a provenance stamp; capped at `limit` rows (default 200).",
          "inputSchema": { "type": "object", "properties": { "query": { "type": "string", "description": "A SELECT or WITH query." }, "limit": { "type": "integer", "description": "Max rows to return (default 200).", "default": 200 } }, "required": ["query"] } }),
        json!({ "name": "explain", "description": "Validate a SQL query WITHOUT executing it - binds tables/columns/types and returns {valid:true} or an error with a fix hint. Cheaper than `sql`; use it to check a query before running it.",
          "inputSchema": { "type": "object", "properties": { "query": { "type": "string", "description": "A SELECT or WITH query to validate." } }, "required": ["query"] } }),
        json!({ "name": "entity", "description": "Look up one transfer by its id, formatted `{block:012}-{logindex:06}`.",
          "inputSchema": { "type": "object", "properties": { "id": { "type": "string" } }, "required": ["id"] } }),
    ];
    // ERC-20-shaped tools (RFC-0025): advertised only where a transfer-shaped decoder exists - the same
    // gate the balance view uses - so they never return an empty `{"count":0}` that an agent misreads
    // as "the index is empty".
    if shape.transfers {
        tools.push(json!({ "name": "balance", "description": "Derived token balance for an address (IVM view; i128 base units, returned as a decimal string).",
          "inputSchema": { "type": "object", "properties": { "address": { "type": "string" } }, "required": ["address"] } }));
        tools.push(json!({ "name": "top_balances", "description": "Top holder balances, descending (IVM view; i128 base units as decimal strings).",
          "inputSchema": { "type": "object", "properties": { "limit": { "type": "integer", "default": 20 } } } }));
    }
    // Compliance tools (RFC-0008): advertised only where compliance is configured (RFC-0025).
    if shape.compliance {
        tools.push(json!({ "name": "flags", "description": "Compliance flags (RFC-0008 C3): `kind=threshold` (single transfers over the configured amount) or `kind=velocity` (addresses over the windowed-volume threshold). Amounts are i128 base units as decimal strings.",
          "inputSchema": { "type": "object", "properties": { "kind": { "type": "string", "enum": ["threshold", "velocity"] }, "limit": { "type": "integer", "default": 50 } } } }));
        tools.push(json!({ "name": "exposure", "description": "Direct counterparty-exposure of an address to the labeled set (RFC-0008 C1): inbound/outbound count + summed amount per label.",
          "inputSchema": { "type": "object", "properties": { "address": { "type": "string" } }, "required": ["address"] } }));
    }
    Value::Array(tools)
}

async fn call_tool(params: &Value, client: &reqwest::Client, base: &str) -> Result<String> {
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing tool name"))?;
    let args = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    match name {
        "status" => get(client, &format!("{base}/")).await,
        // The enriched, per-nest schema (RFC-0016 §2): structure + meaning + footguns + coverage,
        // composed server-side from the running nest and `semantic.toml`. No longer a static string.
        "schema" => get(client, &format!("{base}/schema")).await,
        "tables" => get(client, &format!("{base}/tables")).await,
        "table" => {
            let name = args["name"]
                .as_str()
                .ok_or_else(|| anyhow!("`name` is required"))?;
            let n = args["limit"].as_u64().unwrap_or(50);
            get(
                client,
                &format!("{base}/table/{}?limit={n}", segment(name)?),
            )
            .await
        }
        "sql" => {
            let q = args["query"]
                .as_str()
                .ok_or_else(|| anyhow!("`query` is required"))?;
            // Shape the result for a context window (RFC-0016 §4): a small default row cap and a
            // compact table + provenance stamp, instead of 50K rows of verbose JSON.
            let limit = args["limit"].as_u64().unwrap_or(200).to_string();
            let raw = get_query(
                client,
                &format!("{base}/sql"),
                &[("q", q), ("max_rows", &limit)],
            )
            .await?;
            Ok(format_sql_result(&raw))
        }
        "explain" => {
            let q = args["query"]
                .as_str()
                .ok_or_else(|| anyhow!("`query` is required"))?;
            get_query(client, &format!("{base}/explain"), &[("q", q)]).await
        }
        "entity" => {
            let id = args["id"]
                .as_str()
                .ok_or_else(|| anyhow!("`id` is required"))?;
            get(client, &format!("{base}/entity/{}", segment(id)?)).await
        }
        "balance" => {
            let a = args["address"]
                .as_str()
                .ok_or_else(|| anyhow!("`address` is required"))?;
            get(client, &format!("{base}/balance/{}", segment(a)?)).await
        }
        "top_balances" => {
            let n = args["limit"].as_u64().unwrap_or(20);
            get(client, &format!("{base}/balances?limit={n}")).await
        }
        "flags" => {
            let kind = args["kind"].as_str().unwrap_or("threshold");
            if !matches!(kind, "threshold" | "velocity") {
                bail!("unknown flag kind `{kind}` (want: threshold, velocity)");
            }
            let n = args["limit"].as_u64().unwrap_or(50);
            get(client, &format!("{base}/flags?kind={kind}&limit={n}")).await
        }
        "exposure" => {
            let a = args["address"]
                .as_str()
                .ok_or_else(|| anyhow!("`address` is required"))?;
            get(client, &format!("{base}/exposure/{}", segment(a)?)).await
        }
        other => bail!("unknown tool `{other}`"),
    }
}

/// Shape a `/sql` JSON response for an agent's context window (RFC-0016 §4): a compact aligned table
/// instead of verbose per-row JSON (measured ≥3× fewer tokens), truncation stated as *guidance* (an
/// agent told *why* it was cut adapts; one silently truncated reports wrong totals), and a provenance
/// stamp so the answer is citable back to content-addressed data. An error body is relayed verbatim
/// (it already carries the §3 fix hint).
fn format_sql_result(raw: &str) -> String {
    let v: Value = match serde_json::from_str(raw) {
        Ok(v) => v,
        Err(_) => return raw.to_string(),
    };
    if let Some(err) = v.get("error").and_then(Value::as_str) {
        return err.to_string();
    }
    let rows = v
        .get("rows")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut out = String::new();
    // Ahead of the rows, unlike the truncation notice below. An agent silently consuming reduced cold
    // data is the worse half of #435: truncation explains a cut the agent can *see* in the row count,
    // whereas degradation explains rows that are not there at all and leave no trace in the table -
    // trailing a 200-row result it would be competing with the data for attention. Stated as guidance
    // for the same reason the truncation notice is: an agent told what is wrong can qualify its answer
    // or re-query, one told nothing reports a wrong total in confident prose.
    if let Some(tables) = v.get("degraded_tables").and_then(Value::as_array) {
        if !tables.is_empty() {
            let names: Vec<&str> = tables.iter().filter_map(Value::as_str).collect();
            // A statement about the nest rather than about these rows, because that is all the flag
            // knows: `degraded_tables` comes from schema ∪ manifest ∪ hot and never from the SQL. An
            // agent told "these rows are a subset" about a complete answer over a healthy table does
            // the damage this notice exists to prevent, in the other direction - it is instructed to
            // *say so in any answer*, so it qualifies a correct total as understated. Cause-neutral
            // too: an undefinable view lands in this set with every segment binding fine (#434).
            out.push_str(&format!(
                "⚠ incomplete: this nest could not serve complete cold data for {}. Any result \
                 drawing on {} is a subset of the real history, and totals or aggregates over {} \
                 are understated - say so in any answer that draws on {}, and check the node's \
                 logs.\n\n",
                names.join(", "),
                if names.len() == 1 { "it" } else { "them" },
                if names.len() == 1 { "it" } else { "them" },
                if names.len() == 1 { "it" } else { "them" }
            ));
        }
    }
    if rows.is_empty() {
        out.push_str("(0 rows)\n");
    } else {
        // A node older than #1609 sends no `columns`; its rows' keys come back sorted.
        let cols: Vec<String> = match v.get("columns").and_then(Value::as_array) {
            Some(named) if !named.is_empty() => named
                .iter()
                .filter_map(|c| c.as_str().map(str::to_string))
                .collect(),
            _ => rows[0]
                .as_object()
                .map(|o| o.keys().cloned().collect())
                .unwrap_or_default(),
        };
        let mut w: Vec<usize> = cols.iter().map(String::len).collect();
        for r in &rows {
            if let Some(o) = r.as_object() {
                for (i, c) in cols.iter().enumerate() {
                    w[i] = w[i].max(o.get(c).map(cell).map(|s| s.len()).unwrap_or(0));
                }
            }
        }
        let pad = |s: &str, i: usize| format!("{s:<width$}", width = w[i]);
        out.push_str(
            &cols
                .iter()
                .enumerate()
                .map(|(i, c)| pad(c, i))
                .collect::<Vec<_>>()
                .join("  "),
        );
        out.push('\n');
        for r in &rows {
            if let Some(o) = r.as_object() {
                out.push_str(
                    &cols
                        .iter()
                        .enumerate()
                        .map(|(i, c)| pad(&o.get(c).map(cell).unwrap_or_default(), i))
                        .collect::<Vec<_>>()
                        .join("  "),
                );
                out.push('\n');
            }
        }
    }

    let count = v
        .get("count")
        .and_then(Value::as_u64)
        .unwrap_or(rows.len() as u64);
    if v.get("truncated").and_then(Value::as_bool).unwrap_or(false) {
        out.push_str(&format!(
            "\n… truncated at {count} rows - aggregate (GROUP BY), tighten the WHERE, or raise `limit`.\n"
        ));
    }
    if let Some(p) = v.get("provenance") {
        let as_of = p
            .get("as_of")
            .and_then(Value::as_u64)
            .map(|b| b.to_string())
            .unwrap_or_else(|| "?".into());
        let sealed = p.get("sealed_through").and_then(Value::as_u64).unwrap_or(0);
        let rh: String = p
            .get("registry_hash")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim_start_matches("0x")
            .chars()
            .take(8)
            .collect();
        out.push_str(&format!(
            "- as of block {as_of}, sealed_through {sealed}, source hot+sealed, registry {rh}\n"
        ));
    }
    out
}

/// One result cell as compact text: strings bare (no JSON quotes), null empty, everything else its
/// JSON scalar form. This is the density win over `[{"k":"v",…}]`.
fn cell(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn http_client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .build()?)
}

/// One URL path segment from a tool argument. Encoding alone is not enough: the URL parser resolves
/// `..` (and `%2e%2e`) against the base, which would reach another route or mount.
fn segment(s: &str) -> Result<String> {
    if s.is_empty() || s == "." || s == ".." {
        bail!("`{s}` is not a valid path segment");
    }
    Ok(s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect())
}

async fn get(client: &reqwest::Client, url: &str) -> Result<String> {
    fetch(client.get(url), url).await
}

async fn get_query(client: &reqwest::Client, url: &str, q: &[(&str, &str)]) -> Result<String> {
    fetch(client.get(url).query(q), url).await
}

async fn fetch(req: reqwest::RequestBuilder, url: &str) -> Result<String> {
    fetch_capped(req, url, MAX_MESSAGE_BYTES).await
}

/// The error reaches the model's transcript, and the configured URL may carry credentials, so it
/// names the endpoint by scheme and host only (#1690).
async fn fetch_capped(req: reqwest::RequestBuilder, url: &str, cap: usize) -> Result<String> {
    let mut resp = req.send().await.map_err(|e| {
        anyhow!(
            "cannot reach nuthatch at {} - is `nuthatch dev` running? ({})",
            crate::rpc::redact_url(url),
            e.without_url()
        )
    })?;
    let status = resp.status();
    let mut bytes = Vec::new();
    while let Some(chunk) = resp.chunk().await.map_err(|e| e.without_url())? {
        if bytes.len() + chunk.len() > cap {
            bail!("the response from nuthatch is larger than {cap} bytes; ask for fewer rows");
        }
        bytes.extend_from_slice(&chunk);
    }
    let body = String::from_utf8_lossy(&bytes).into_owned();
    // A 404 or a refused query is a failed tool call, with the body as the agent's explanation.
    if !status.is_success() {
        bail!("HTTP {}: {body}", status.as_u16());
    }
    Ok(body)
}

fn ok(id: Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn err(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

fn content(text: &str, is_error: bool) -> Value {
    json!({ "content": [{ "type": "text", "text": text }], "isError": is_error })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #1661: a line over the cap is read through and dropped, and the next message still arrives.
    #[tokio::test]
    async fn an_oversized_message_is_dropped_and_the_next_one_read() {
        let input = format!("{}\n{{\"id\":1}}\n{{\"id\":2}}", "x".repeat(100));
        let mut r = tokio::io::BufReader::with_capacity(8, input.as_bytes());
        assert_eq!(
            super::next_message(&mut r, 16).await.unwrap().as_deref(),
            Some("{\"id\":1}\n")
        );
        assert_eq!(
            super::next_message(&mut r, 16).await.unwrap().as_deref(),
            Some("{\"id\":2}")
        );
        assert_eq!(super::next_message(&mut r, 16).await.unwrap(), None);
    }

    /// #1690: the configured URL can carry credentials and the error goes to the model, and a target
    /// cannot make the bridge buffer more than the cap.
    #[tokio::test]
    async fn a_fetch_error_names_no_credentials_and_a_large_answer_is_refused() {
        let client = reqwest::Client::new();
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let url = format!("http://user:hunter2@127.0.0.1:{port}/secret-path/sql");
        let err = super::fetch_capped(client.get(&url), &url, 1024)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            !err.contains("hunter2") && !err.contains("secret-path"),
            "{err}"
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app =
            axum::Router::new().route("/big", axum::routing::get(|| async { "y".repeat(4096) }));
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let big = format!("http://{addr}/big");
        let err = super::fetch_capped(client.get(&big), &big, 1024)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("larger than 1024 bytes"), "{err}");
        server.abort();
    }

    /// #2217: a tool argument is one path segment; it cannot climb to another route.
    #[tokio::test]
    async fn a_tool_argument_cannot_leave_its_route() {
        use axum::{extract::Path, routing::get, Router};
        let app = Router::new()
            .route(
                "/table/{name}",
                get(|Path(n): Path<String>| async move { format!("table:{n}") }),
            )
            .route("/_admin/events", get(|| async { "admin" }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = reqwest::Client::new();
        let call = |name: &str| json!({ "name": "table", "arguments": { "name": name } });

        let climbed = call_tool(&call("../_admin/events"), &client, &base).await;
        assert!(!matches!(&climbed, Ok(b) if b == "admin"), "{climbed:?}");
        for refused in ["..", "."] {
            let err = call_tool(&call(refused), &client, &base).await.unwrap_err();
            assert!(
                err.to_string().contains("not a valid path segment"),
                "{err}"
            );
        }
        assert_eq!(
            call_tool(&call("a/b?c"), &client, &base).await.unwrap(),
            "table:a/b?c"
        );
        let flags = json!({ "name": "flags", "arguments": { "kind": "x&limit=1" } });
        assert!(call_tool(&flags, &client, &base).await.is_err());
        server.abort();
    }

    /// #2217: an endless response ends in an error rather than wedging the sequential stdio loop.
    #[tokio::test(start_paused = true)]
    async fn an_endless_response_times_out() {
        use axum::{body::Body, routing::get, Router};
        let app = Router::new().route(
            "/table/{name}",
            get(|| async {
                Body::from_stream(futures::stream::pending::<Result<Vec<u8>, std::io::Error>>())
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let call = json!({ "name": "table", "arguments": { "name": "t" } });
        let answered = tokio::time::timeout(
            REQUEST_TIMEOUT * 5,
            call_tool(&call, &http_client().unwrap(), &base),
        )
        .await;
        assert!(matches!(answered, Ok(Err(_))), "{answered:?}");
        server.abort();
    }

    /// A one-endpoint fake nest. `shape` of `None` means "no `/shape` route" - an older nest, which is
    /// exactly the case the fail-open default exists for.
    async fn fake_nest(shape: Option<serde_json::Value>) -> (String, tokio::task::JoinHandle<()>) {
        use axum::{extract::State, routing::get, Json, Router};
        async fn handler(
            State(shape): State<Option<serde_json::Value>>,
        ) -> Result<Json<serde_json::Value>, axum::http::StatusCode> {
            match shape {
                Some(v) => Ok(Json(v)),
                None => Err(axum::http::StatusCode::NOT_FOUND),
            }
        }
        let app = Router::new()
            .route("/shape", get(handler))
            .with_state(shape);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}"), handle)
    }

    /// RFC-0025 fail-open (issue #150): tool advertisement must **degrade towards showing more**, never
    /// less. A nest predating `/shape`, an unreachable one, or one answering rubbish must all yield the
    /// permissive default - hiding a tool because a probe failed would strand an agent with no way to
    /// discover a capability the nest actually has.
    #[tokio::test]
    async fn fetch_shape_fails_open_on_a_missing_unreachable_or_malformed_endpoint() {
        let client = reqwest::Client::new();

        // No `/shape` route at all - the older-nest case this default encodes.
        let (base, h) = fake_nest(None).await;
        let shape = fetch_shape(&client, &base).await;
        assert!(
            shape.transfers,
            "a nest with no /shape must still advertise transfers"
        );
        assert!(shape.compliance);
        h.abort();

        // Unreachable host: nothing listening on this port.
        let shape = fetch_shape(&client, "http://127.0.0.1:1").await;
        assert!(shape.transfers, "an unreachable nest must fail open");
        assert!(shape.compliance);

        // Present but not JSON-shaped as expected: missing keys fall back to true per-field.
        let (base, h) = fake_nest(Some(serde_json::json!({"unrelated": 1}))).await;
        let shape = fetch_shape(&client, &base).await;
        assert!(shape.transfers);
        assert!(shape.compliance);
        h.abort();

        // A well-formed negative answer IS honoured - fail-open must not mean "ignore the nest".
        let (base, h) = fake_nest(Some(serde_json::json!({
            "transfers": false,
            "compliance": false
        })))
        .await;
        let shape = fetch_shape(&client, &base).await;
        assert!(!shape.transfers, "an explicit false must be respected");
        assert!(!shape.compliance);
        h.abort();
    }

    #[test]
    fn client_config_launches_this_binary_and_bridges_to_base() {
        let cfg = client_config_json("/usr/local/bin/nuthatch", "http://127.0.0.1:8288");
        let srv = &cfg["mcpServers"]["nuthatch"];
        assert_eq!(srv["command"], "/usr/local/bin/nuthatch");
        // The client launches `nuthatch mcp --url <base>` as a stdio subprocess.
        assert_eq!(
            srv["args"],
            json!(["mcp", "--url", "http://127.0.0.1:8288"])
        );
    }

    #[tokio::test]
    async fn initialize_and_tools_list_need_no_network() {
        let client = reqwest::Client::new();
        let init = json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": { "protocolVersion": "2025-03-26" } });
        let resp = handle(&init, &client, "http://127.0.0.1:1").await.unwrap();
        assert_eq!(resp["result"]["serverInfo"]["name"], "nuthatch");
        assert_eq!(
            resp["result"]["protocolVersion"], "2025-03-26",
            "echoes client version"
        );

        let list = json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list" });
        let resp = handle(&list, &client, "http://127.0.0.1:1").await.unwrap();
        let tools = resp["result"]["tools"].as_array().unwrap();
        // The base is unreachable here, so `fetch_shape` fails its probe and falls back to advertising
        // everything (RFC-0025) - the safe default, identical to the pre-RFC-0025 unconditional surface.
        assert_eq!(tools.len(), 11);
        assert!(tools.iter().any(|t| t["name"] == "sql"));
        assert!(tools.iter().any(|t| t["name"] == "explain"));
        assert!(tools.iter().any(|t| t["name"] == "tables"));
        // The compliance tools (RFC-0008 C6).
        assert!(tools.iter().any(|t| t["name"] == "flags"));
        assert!(tools.iter().any(|t| t["name"] == "exposure"));
    }

    #[test]
    fn tool_and_prompt_surface_follows_shape() {
        let names = |v: &Value| {
            v.as_array()
                .unwrap()
                .iter()
                .map(|t| t["name"].as_str().unwrap().to_string())
                .collect::<Vec<_>>()
        };

        // Bare nest (e.g. a Uniswap-V3 pool: Swap events, no transfers, no compliance): the 7 generic
        // tools only, and the compliance-shaped `investigate-address` prompt is withheld.
        let bare = Shape {
            transfers: false,
            compliance: false,
        };
        let t = names(&tool_specs(&bare));
        assert_eq!(t.len(), 7);
        for hidden in ["balance", "top_balances", "flags", "exposure"] {
            assert!(
                !t.contains(&hidden.to_string()),
                "{hidden} must be hidden on a bare nest"
            );
        }
        let p = names(&prompt_specs(&bare));
        assert_eq!(p, vec!["profile-contract", "verify-a-number"]);

        // Token nest: the balance tools appear; compliance stays hidden.
        let token = Shape {
            transfers: true,
            compliance: false,
        };
        let t = names(&tool_specs(&token));
        assert_eq!(t.len(), 9);
        assert!(t.contains(&"balance".to_string()) && t.contains(&"top_balances".to_string()));
        assert!(!t.contains(&"flags".to_string()));

        // Compliance-configured token nest: the full 11, and `investigate-address` returns.
        let full = Shape {
            transfers: true,
            compliance: true,
        };
        assert_eq!(names(&tool_specs(&full)).len(), 11);
        assert!(names(&prompt_specs(&full)).contains(&"investigate-address".to_string()));
    }

    #[tokio::test]
    async fn advertises_resources_and_prompts_and_lists_them() {
        let client = reqwest::Client::new();
        let init = json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {} });
        let resp = handle(&init, &client, "http://127.0.0.1:1").await.unwrap();
        let caps = &resp["result"]["capabilities"];
        assert!(
            caps.get("tools").is_some()
                && caps.get("resources").is_some()
                && caps.get("prompts").is_some()
        );

        let rl = json!({ "jsonrpc": "2.0", "id": 2, "method": "resources/list" });
        let resp = handle(&rl, &client, "http://127.0.0.1:1").await.unwrap();
        let uris: Vec<&str> = resp["result"]["resources"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|r| r["uri"].as_str())
            .collect();
        assert!(uris.contains(&"nuthatch://schema"));

        let pl = json!({ "jsonrpc": "2.0", "id": 3, "method": "prompts/list" });
        let resp = handle(&pl, &client, "http://127.0.0.1:1").await.unwrap();
        assert_eq!(resp["result"]["prompts"].as_array().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn prompt_get_interpolates_its_argument() {
        let client = reqwest::Client::new();
        let req = json!({ "jsonrpc": "2.0", "id": 1, "method": "prompts/get",
            "params": { "name": "investigate-address", "arguments": { "address": "0xBEEF" } } });
        let resp = handle(&req, &client, "http://127.0.0.1:1").await.unwrap();
        let text = resp["result"]["messages"][0]["content"]["text"]
            .as_str()
            .unwrap();
        assert!(
            text.contains("0xBEEF"),
            "renders the address into the prompt"
        );
        assert!(text.contains("`exposure`"), "names real tools");

        // Unknown prompt → a clean error, not a panic.
        let bad = json!({ "jsonrpc": "2.0", "id": 2, "method": "prompts/get", "params": { "name": "nope" } });
        let resp = handle(&bad, &client, "http://127.0.0.1:1").await.unwrap();
        assert!(resp.get("error").is_some());
    }

    #[test]
    fn sql_result_is_compact_with_provenance_and_smaller_than_json() {
        let raw = r#"{"count":2,"truncated":false,"rows":[{"n":10,"to":"0xabc"},{"n":5,"to":"0xdef"}],"provenance":{"as_of":100,"sealed_through":93,"source":"hot+sealed","registry_hash":"0x30ced74de367aa"}}"#;
        let out = format_sql_result(raw);
        assert!(out.contains("0xabc") && !out.contains("\"0xabc\""));
        assert!(out.contains("as of block 100"));
        assert!(out.contains("sealed_through 93"));
        assert!(out.contains("registry 30ced74d"));
        assert!(out.len() < raw.len(), "compact must beat verbose JSON");
    }

    /// #1609: the table follows the response's `columns`, not the parsed rows' sorted keys.
    #[test]
    fn sql_result_table_keeps_the_querys_column_order() {
        let raw =
            r#"{"count":1,"truncated":false,"columns":["z","a","m"],"rows":[{"z":1,"a":2,"m":3}]}"#;
        let out = format_sql_result(raw);
        let header: Vec<&str> = out.lines().next().unwrap().split_whitespace().collect();
        assert_eq!(header, ["z", "a", "m"], "{out}");
        let row: Vec<&str> = out.lines().nth(1).unwrap().split_whitespace().collect();
        assert_eq!(row, ["1", "2", "3"], "{out}");
    }

    #[test]
    fn sql_truncation_is_guidance_not_silence() {
        let raw = r#"{"count":200,"truncated":true,"rows":[{"n":1}],"provenance":{"as_of":9,"sealed_through":9,"source":"hot+sealed","registry_hash":"0xabcd1234"}}"#;
        let out = format_sql_result(raw);
        assert!(out.contains("truncated at 200 rows"));
        assert!(
            out.contains("GROUP BY") && out.contains("`limit`"),
            "tells the agent how to adapt"
        );
    }

    /// **Issue #435.** An agent must be told when the rows it is about to reason over are a subset of
    /// the history, and told *before* it reads them.
    ///
    /// This is the worse half of #435: a human at least sees the row count and may wonder, whereas an
    /// agent asked "how much moved?" will sum the column and answer in confident prose. The notice
    /// leads the result for that reason - after a long table it is competing with the data.
    #[test]
    fn sql_degradation_leads_the_result_for_an_agent() {
        let raw = r#"{"count":1,"truncated":false,"degraded":true,"degraded_tables":["t__transfer"],"rows":[{"n":1}],"provenance":{"as_of":9,"sealed_through":9,"source":"hot+sealed","registry_hash":"0xabcd1234"}}"#;
        let out = format_sql_result(raw);
        assert!(
            out.contains("incomplete") && out.contains("t__transfer"),
            "the notice names the affected table: {out}"
        );
        assert!(
            out.contains("understated"),
            "and says what it does to a total, which is the thing an agent is about to report"
        );
        let notice = out.find("incomplete").expect("the notice");
        let table = out.find("n\n").expect("the rendered table's header");
        assert!(
            notice < table,
            "the notice must precede the rows, not trail them: {out}"
        );
    }

    /// The control for the test above. A healthy result carries no caveat, or the caveat means
    /// nothing - and `degraded_tables` present-but-empty must read as healthy, not as a bare
    /// truthiness check on the field.
    #[test]
    fn a_healthy_sql_result_carries_no_degradation_notice() {
        let raw = r#"{"count":1,"truncated":false,"degraded":false,"degraded_tables":[],"rows":[{"n":1}],"provenance":{"as_of":9,"sealed_through":9,"source":"hot+sealed","registry_hash":"0xabcd1234"}}"#;
        let out = format_sql_result(raw);
        assert!(
            !out.contains("incomplete"),
            "an intact nest must not warn: {out}"
        );
    }

    #[test]
    fn sql_error_body_is_relayed_verbatim() {
        let raw = r#"{"error":"Binder Error: …\n\nhint: use value_dec"}"#;
        let out = format_sql_result(raw);
        assert!(out.contains("hint: use value_dec"));
    }

    #[tokio::test]
    async fn notifications_get_no_response() {
        let client = reqwest::Client::new();
        let note = json!({ "jsonrpc": "2.0", "method": "notifications/initialized" });
        assert!(handle(&note, &client, "http://127.0.0.1:1").await.is_none());
    }

    /// A nest that answers the HTTP surface MCP bridges to. Enough routes for every generic tool
    /// `call_tool` dispatches, so deleting an arm fails the matching assertion rather than leaving
    /// a brochure.
    async fn fake_serving_nest() -> (String, tokio::task::JoinHandle<()>) {
        use axum::{
            extract::{Path, Query},
            routing::get,
            Json, Router,
        };
        use std::collections::HashMap;

        let app = Router::new()
            .route("/", get(|| async { Json(json!({"ok": true, "last_block": 42})) }))
            .route("/schema", get(|| async { "tables are {alias}__{event}" }))
            .route(
                "/tables",
                get(|| async { Json(json!([{"name": "usdc__transfer"}])) }),
            )
            .route(
                "/shape",
                get(|| async { Json(json!({"transfers": true, "compliance": false})) }),
            )
            .route(
                "/table/{name}",
                get(
                    |Path(name): Path<String>, Query(q): Query<HashMap<String, String>>| async move {
                        Json(json!({"name": name, "limit": q.get("limit"), "rows": []}))
                    },
                ),
            )
            .route(
                "/sql",
                get(|Query(q): Query<HashMap<String, String>>| async move {
                    Json(json!({
                        "count": 1,
                        "truncated": false,
                        "rows": [{"n": 1, "q": q.get("q")}],
                        "provenance": {
                            "as_of": 42,
                            "sealed_through": 40,
                            "source": "hot+sealed",
                            "registry_hash": "0xdeadbeef"
                        }
                    }))
                }),
            )
            .route(
                "/explain",
                get(|Query(q): Query<HashMap<String, String>>| async move {
                    Json(json!({"valid": true, "query": q.get("q")}))
                }),
            )
            .route(
                "/entity/{id}",
                get(|Path(id): Path<String>| async move {
                    use axum::response::IntoResponse;
                    if id == "000000000000-000000" {
                        return (
                            axum::http::StatusCode::NOT_FOUND,
                            Json(json!({"error": "not found", "id": id})),
                        )
                            .into_response();
                    }
                    Json(json!({"id": id, "table": "usdc__transfer"})).into_response()
                }),
            )
            .route(
                "/balance/{address}",
                get(|Path(address): Path<String>| async move {
                    Json(json!({"address": address, "balance": "0"}))
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}"), handle)
    }

    fn tool_text(resp: &Value) -> &str {
        resp["result"]["content"][0]["text"].as_str().unwrap()
    }

    /// #304: schema discovery, SQL exec, entity lookup - the MCP surface the docs advertise -
    /// exercised through `tools/call` against a fake nest. Streaming subscribe is not shipped
    /// (RFC-0010) and is not advertised.
    #[tokio::test]
    async fn tools_call_covers_the_documented_surface() {
        let client = reqwest::Client::new();
        let (base, h) = fake_serving_nest().await;

        let list = handle(
            &json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }),
            &client,
            &base,
        )
        .await
        .unwrap();
        let names: Vec<&str> = list["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|t| t["name"].as_str())
            .collect();
        assert!(
            !names.contains(&"subscribe"),
            "RFC-0010: streaming subscribe is not shipped, so it must not be advertised"
        );
        for required in [
            "status", "schema", "tables", "table", "sql", "explain", "entity",
        ] {
            assert!(
                names.contains(&required),
                "{required} must be advertised: {names:?}"
            );
        }

        async fn call(client: &reqwest::Client, base: &str, name: &str, arguments: Value) -> Value {
            handle(
                &json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/call",
                    "params": { "name": name, "arguments": arguments }
                }),
                client,
                base,
            )
            .await
            .unwrap()
        }

        let status = call(&client, &base, "status", json!({})).await;
        assert!(tool_text(&status).contains("last_block"), "{status}");
        assert_ne!(status["result"]["isError"], true);

        let schema = call(&client, &base, "schema", json!({})).await;
        assert!(tool_text(&schema).contains("{alias}__{event}"), "{schema}");

        let tables = call(&client, &base, "tables", json!({})).await;
        assert!(tool_text(&tables).contains("usdc__transfer"), "{tables}");

        let table = call(
            &client,
            &base,
            "table",
            json!({ "name": "usdc__transfer", "limit": 10 }),
        )
        .await;
        assert!(tool_text(&table).contains("usdc__transfer"), "{table}");

        let sql = call(
            &client,
            &base,
            "sql",
            json!({ "query": "SELECT 1", "limit": 5 }),
        )
        .await;
        let sql_text = tool_text(&sql);
        assert!(
            sql_text.contains("as of block 42") && sql_text.contains("SELECT 1"),
            "sql must run and stamp provenance: {sql_text}"
        );
        assert_ne!(sql["result"]["isError"], true);

        let explain = call(&client, &base, "explain", json!({ "query": "SELECT 1" })).await;
        assert!(tool_text(&explain).contains("valid"), "{explain}");

        let entity = call(
            &client,
            &base,
            "entity",
            json!({ "id": "000000000042-000001" }),
        )
        .await;
        assert!(
            tool_text(&entity).contains("000000000042-000001"),
            "{entity}"
        );
        let missing = call(
            &client,
            &base,
            "entity",
            json!({ "id": "000000000000-000000" }),
        )
        .await;
        assert_eq!(missing["result"]["isError"], true, "{missing}");
        assert!(tool_text(&missing).contains("not found"), "{missing}");

        let balance = call(&client, &base, "balance", json!({ "address": "0xabc" })).await;
        assert!(tool_text(&balance).contains("0xabc"), "{balance}");

        let read = handle(
            &json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "resources/read",
                "params": { "uri": "nuthatch://schema" }
            }),
            &client,
            &base,
        )
        .await
        .unwrap();
        assert!(
            read["result"]["contents"][0]["text"]
                .as_str()
                .unwrap()
                .contains("{alias}__{event}"),
            "{read}"
        );

        h.abort();
    }

    /// #304: the no-network degrade path. `initialize` / `tools/list` already answer with no nest
    /// (fail-open). A tool call against an unreachable nest must not panic or hang: it returns
    /// `isError` and names `nuthatch dev`. There is no `--offline` flag; this test is the path.
    #[tokio::test]
    async fn tools_call_degrades_when_the_nest_is_unreachable() {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(2))
            .build()
            .unwrap();
        let resp = handle(
            &json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": { "name": "status", "arguments": {} }
            }),
            &client,
            "http://127.0.0.1:1",
        )
        .await
        .unwrap();
        assert_eq!(
            resp["result"]["isError"], true,
            "an unreachable nest is an error, not an empty result: {resp}"
        );
        let text = tool_text(&resp);
        assert!(
            text.contains("cannot reach nuthatch") && text.contains("nuthatch dev"),
            "tell the caller to start the local instance: {text}"
        );
    }
}
