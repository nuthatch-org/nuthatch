//! `nuthatch-gateway <config.toml>`: see README.md.

use nuthatch_gateway::{Config, Gateway};

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("nuthatch-gateway: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let mut args = std::env::args().skip(1);
    let path = match (args.next(), args.next()) {
        (Some(p), None) if !p.starts_with('-') => p,
        _ => return Err("usage: nuthatch-gateway <config.toml>".into()),
    };
    let text = std::fs::read_to_string(&path).map_err(|e| format!("{path}: {e}"))?;
    let cfg = Config::parse(&text)?;
    let listen = cfg.listen.clone();
    let app = Gateway::new(cfg).await?.router();
    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .map_err(|e| format!("listen {listen}: {e}"))?;
    eprintln!("nuthatch-gateway: listening on {listen}");
    axum::serve(listener, app)
        .await
        .map_err(|e| format!("serve: {e}"))
}
