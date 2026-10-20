//! A webhook receiver for a nuthatch nest.
//!
//! Run it, point a `[[webhooks]]` or `[[alerts]]` sink at it, and it prints what arrives
//! and whether the signature checks out:
//!
//! ```sh
//! cargo run --example webhooks -- --secret hunter2
//! cargo run --example webhooks -- --secret hunter2 --port 9000
//! ```
//!
//! The whole point of this file is the verification below. Everything else is a print
//! statement. If you take one thing from it, take `verify()`.

use axum::body::Bytes;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::Router;
use clap::Parser;
use sha2::{Digest, Sha256};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value_t = 8099)]
    port: u16,
    /// the same string as `secret` in the webhook config
    #[arg(long)]
    secret: Option<String>,
    /// return 200 even when verification fails (for debugging only)
    #[arg(long)]
    accept_unsigned: bool,
}

/// Is this POST really from the nest that holds `secret`?
///
/// nuthatch signs the **exact bytes it sends**, not a re-serialisation of them, and sends the
/// result as `X-Nuthatch-Signature: sha256=<hex>`. So verify against the raw body you received.
/// If you parse the JSON first and re-encode it to check the signature, you will get a different
/// byte string and the check will fail for reasons that look like a bug in nuthatch and are not.
///
/// It is plain HMAC-SHA256 (RFC 2104), so your language's HMAC library agrees with it; it is
/// spelled out here only because this crate has no `hmac` dependency. Compare in constant time.
fn verify(secret: &str, body: &[u8], header: Option<&str>) -> bool {
    let Some(sent) = header.and_then(|h| h.strip_prefix("sha256=")) else {
        return false;
    };
    let mut key = [0u8; 64];
    if secret.len() > 64 {
        key[..32].copy_from_slice(&Sha256::digest(secret.as_bytes()));
    } else {
        key[..secret.len()].copy_from_slice(secret.as_bytes());
    }
    let pad = |p: u8| key.iter().map(|b| b ^ p).collect::<Vec<u8>>();
    let inner = Sha256::new()
        .chain_update(pad(0x36))
        .chain_update(body)
        .finalize();
    let outer = Sha256::new()
        .chain_update(pad(0x5c))
        .chain_update(inner)
        .finalize();
    let expected = hex::encode(outer);
    !sent.is_empty()
        && expected.len() == sent.len()
        && expected
            .bytes()
            .zip(sent.bytes())
            .fold(0, |acc, (a, b)| acc | (a ^ b))
            == 0
}

async fn receive(
    secret: Option<String>,
    strict: bool,
    headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, &'static str) {
    let sig = headers
        .get("x-nuthatch-signature")
        .and_then(|v| v.to_str().ok());

    if let Some(secret) = secret.as_deref() {
        let ok = verify(secret, &body, sig);
        println!(
            "signature: {}  ({})",
            if ok { "OK" } else { "BAD" },
            sig.unwrap_or("None")
        );
        if !ok && strict {
            // A non-2xx leaves the delivery in nuthatch's outbox and it will retry: an unverified
            // payload should not be silently accepted *or* silently dropped.
            return (StatusCode::UNAUTHORIZED, "");
        }
    } else if let Some(sig) = sig {
        println!("signature present but no --secret given, not verifying: {sig}");
    }

    match serde_json::from_slice::<serde_json::Value>(&body) {
        Ok(payload) => {
            let pretty = serde_json::to_string_pretty(&payload).unwrap_or_default();
            println!("{}", pretty.chars().take(2000).collect::<String>());
        }
        Err(_) => println!(
            "non-JSON body ({} bytes): {:?}",
            body.len(),
            String::from_utf8_lossy(&body[..body.len().min(200)])
        ),
    }

    // 2xx is the ack. nuthatch removes the entry from its outbox on any 2xx and retries on
    // anything else, so only return this once you have durably handled the payload -
    // at-least-once delivery means you may see it twice.
    (StatusCode::OK, "ok")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let (secret, strict) = (args.secret.filter(|s| !s.is_empty()), !args.accept_unsigned);
    if secret.is_none() {
        eprintln!("no --secret: signatures will be printed but not checked\n");
    }

    let app = Router::new()
        .fallback_service(post(move |headers, body| {
            receive(secret.clone(), strict, headers, body)
        }))
        .layer(axum::extract::DefaultBodyLimit::disable());
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", args.port)).await?;
    println!(
        "listening on http://127.0.0.1:{}/  (ctrl-c to stop)\n",
        args.port
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
