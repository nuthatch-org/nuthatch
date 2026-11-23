//! Which scope a request needs. Pure functions of the request, so every rule is unit-tested.
//!
//! Scopes name nests by the gateway's own `[[nest]] name`, and mirror the URL they grant:
//!
//! - `read:<nest>` reads everything the nest serves, `/sql`, `/explain` and every MCP tool included;
//! - `read:<nest>/table/<table>` reads `/table/<table>` and the MCP `table` tool for that table;
//! - `read:<nest>/q/<query>` runs the named query `/q/<query>` and the MCP `query` tool for it.
//!
//! **Free-form SQL needs the nest scope.** A statement can reach any relation by a CTE, a join, a
//! view or a table function, and only the engine that binds it knows which. A gateway that guessed
//! from the SQL text would be one quoting trick from a bypass, so a token scoped to tables is refused
//! `/sql`, `/explain` and the MCP `sql`/`explain` tools by name instead.

use serde_json::Value;

/// What a request needs before it is forwarded.
#[derive(Debug, PartialEq, Eq)]
pub enum Need {
    /// Any one of these scopes.
    AnyOf(Vec<String>),
    /// An MCP message: the body decides (see [`mcp`]).
    Mcp,
    /// Refused whatever the token says, with this name.
    Refused(&'static str),
}

/// A name a fine-grained scope may grant: a nuthatch table or query identifier. Anything else (a
/// percent sign, a slash, a dot segment) needs the nest scope, so no encoding can make one name
/// stand for another.
fn plain(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// A path the proxy forwards as it was written: no percent-encoding, no empty or dot segments, no
/// backslash. Every route nuthatch serves is reachable in this form, and refusing the rest means the
/// path matched here is the path nuthatch routes.
pub fn canonical(path: &str) -> bool {
    if !path.starts_with('/') || path.contains(['%', '\\']) {
        return false;
    }
    let segs: Vec<&str> = path[1..].split('/').collect();
    // `/` alone, and one trailing slash, are the only empty segments.
    segs.iter().enumerate().all(|(i, seg)| match *seg {
        "." | ".." => false,
        "" => i == segs.len() - 1,
        _ => true,
    })
}

/// The scope a request for `rest` (the path below the nest's prefix, starting `/`) needs.
pub fn need(nest: &str, rest: &str) -> Need {
    let whole = || vec![format!("read:{nest}")];
    let or = |kind: &str, name: &str| {
        let mut v = whole();
        if plain(name) {
            v.push(format!("read:{nest}/{kind}/{name}"));
        }
        Need::AnyOf(v)
    };
    let segs: Vec<&str> = rest.trim_matches('/').split('/').collect();
    match segs.as_slice() {
        ["table", name] => or("table", name),
        ["q", name] => or("q", name),
        ["mcp"] => Need::Mcp,
        // The admin UI is the operator's, behind nuthatch's own admin token; a read gateway does not
        // carry it.
        [first, ..] if first.starts_with("_admin") => Need::Refused("admin_not_proxied"),
        // The nest router's read routes (src/serve.rs `mount_router`), the GraphQL and `/sync` routes
        // of feature builds included. `routes_are_classified` below fails when nuthatch adds a route
        // this list has not decided.
        [""]
        | ["health" | "ready" | "metrics" | "tables" | "schema" | "entities" | "sql" | "diff"]
        | ["explain" | "queries" | "derived" | "balances" | "flags" | "at" | "nest" | "shape"]
        | ["api" | "graphql" | "sync"]
        | ["ipfs", "gave-up"]
        | ["graph", "status"]
        | ["entity" | "derived" | "balance" | "exposure", _]
        | ["derived", _, _]
        | ["subgraphs", "id", _]
        | ["subgraphs", "name", _, ..] => Need::AnyOf(whole()),
        _ => Need::Refused("not_routed"),
    }
}

/// The scope one MCP JSON-RPC message needs. Discovery (the tool, prompt and resource lists) is open
/// to any token for the nest, so a client scoped to one table can still start; a call needs the scope
/// of what it reads.
pub fn mcp(nest: &str, msg: &Value) -> Vec<String> {
    let whole = vec![format!("read:{nest}")];
    let method = msg["method"].as_str().unwrap_or_default();
    let any_for_nest = || {
        let mut v = whole.clone();
        v.push(format!("read:{nest}/*"));
        v
    };
    match method {
        "initialize" | "ping" | "tools/list" | "prompts/list" | "resources/list" => any_for_nest(),
        m if m.starts_with("notifications/") => any_for_nest(),
        "tools/call" => {
            let name = msg["params"]["arguments"]["name"]
                .as_str()
                .unwrap_or_default();
            let kind = match msg["params"]["name"].as_str() {
                Some("table") => "table",
                Some("query") => "q",
                _ => return whole,
            };
            let mut v = whole;
            if plain(name) {
                v.push(format!("read:{nest}/{kind}/{name}"));
            }
            v
        }
        _ => whole,
    }
}

/// Whether `held` satisfies one of `needed`. `read:<nest>/*` is not a scope anyone is issued: it
/// stands for "any fine-grained scope on this nest".
pub fn granted(held: &[String], needed: &[String]) -> bool {
    needed.iter().any(|n| match n.strip_suffix("/*") {
        Some(nest) => held.iter().any(|h| {
            h.strip_prefix(nest)
                .is_some_and(|rest| rest.starts_with('/'))
        }),
        None => held.iter().any(|h| h == n),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn scopes(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn table_and_query_routes_take_their_own_scope_or_the_nest() {
        assert_eq!(
            need("usdc", "/table/usdc__transfer"),
            Need::AnyOf(scopes(&["read:usdc", "read:usdc/table/usdc__transfer"]))
        );
        assert_eq!(
            need("usdc", "/q/top"),
            Need::AnyOf(scopes(&["read:usdc", "read:usdc/q/top"]))
        );
        for free_form in [
            "/sql",
            "/explain",
            "/tables",
            "/",
            "/entity/1",
            "/derived/x/y",
        ] {
            assert_eq!(need("usdc", free_form), Need::AnyOf(scopes(&["read:usdc"])));
        }
        assert_eq!(
            need("usdc", "/_admin/config"),
            Need::Refused("admin_not_proxied")
        );
        assert_eq!(need("usdc", "/nests"), Need::Refused("not_routed"));
    }

    #[test]
    fn paths_that_could_decode_differently_are_not_canonical() {
        for ok in ["/", "/usdc", "/usdc/", "/usdc/table/t", "/a/b/q/top"] {
            assert!(canonical(ok), "{ok}");
        }
        for bad in [
            "",
            "usdc",
            "//usdc",
            "/usdc//table",
            "/usdc/./sql",
            "/usdc/table/../sql",
            "/usdc/table/t%2F..%2Fsql",
            "/usdc\\sql",
            "/usdc//",
            "/a//b/",
        ] {
            assert!(!canonical(bad), "{bad}");
        }
    }

    #[test]
    fn mcp_calls_need_the_scope_of_what_they_read() {
        let call = |tool: &str, name: &str| {
            json!({"jsonrpc":"2.0","id":1,"method":"tools/call",
                   "params":{"name":tool,"arguments":{"name":name,"query":"SELECT 1"}}})
        };
        let table = scopes(&["read:usdc/table/t"]);
        assert!(granted(&table, &mcp("usdc", &call("table", "t"))));
        assert!(!granted(&table, &mcp("usdc", &call("table", "u"))));
        assert!(!granted(&table, &mcp("usdc", &call("sql", "t"))));
        assert!(!granted(&table, &mcp("usdc", &call("explain", "t"))));
        assert!(!granted(&table, &mcp("usdc", &call("query", "t"))));
        assert!(granted(
            &scopes(&["read:usdc/q/t"]),
            &mcp("usdc", &call("query", "t"))
        ));
        let list = json!({"jsonrpc":"2.0","id":1,"method":"tools/list"});
        assert!(granted(&table, &mcp("usdc", &list)));
        assert!(!granted(
            &scopes(&["read:other/table/t"]),
            &mcp("usdc", &list)
        ));
        let read = json!({"jsonrpc":"2.0","id":1,"method":"resources/read","params":{"uri":"nuthatch://schema"}});
        assert!(!granted(&table, &mcp("usdc", &read)));
        assert!(granted(&scopes(&["read:usdc"]), &mcp("usdc", &read)));
        // `read:usdcx` is not a scope on `usdc`.
        assert!(!granted(
            &scopes(&["read:usdcx/table/t"]),
            &mcp("usdc", &list)
        ));
    }

    /// Every route nuthatch's nest router registers is decided here, so a new one cannot slip through
    /// a stale gateway unclassified: this fails until it is added above.
    #[test]
    fn routes_are_classified() {
        let serve = include_str!("../../../src/serve.rs");
        let start = serve
            .find("pub fn mount_router")
            .expect("mount_router in src/serve.rs");
        let body = &serve[start..];
        let body = &body[..body.find("\n}\n").expect("end of mount_router")];
        let mut seen = 0;
        for line in body.lines() {
            // `.route("/x", …)`, and the `/mcp` route, whose path sits on a line of its own.
            let path = match line.find(".route(\"") {
                Some(at) => line[at + 8..].split('"').next().unwrap(),
                None if line.trim() == "\"/mcp\"," => "/mcp",
                None => continue,
            };
            let concrete: String = path
                .split('/')
                .map(|s| {
                    if s.starts_with("{*") {
                        "a/b"
                    } else if s.starts_with('{') {
                        "x"
                    } else {
                        s
                    }
                })
                .collect::<Vec<_>>()
                .join("/");
            seen += 1;
            let decided = need("n", &concrete);
            assert!(
                decided != Need::Refused("not_routed"),
                "{path} is not classified"
            );
        }
        assert!(seen > 30, "found only {seen} routes in mount_router");
    }
}
