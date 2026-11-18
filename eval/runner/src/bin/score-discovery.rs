use clap::Parser;
use nuthatch_eval_runner::discovery::{self, Format, RunMeta};
use std::path::PathBuf;

/// Score one discovery run (#2988) and append it to the results. `eval/discovery/run.sh` calls this.
#[derive(Parser)]
struct Args {
    #[arg(long)]
    agent: String,
    #[arg(long)]
    agent_version: String,
    /// The model, when the transcript does not name one.
    #[arg(long)]
    model: Option<String>,
    #[arg(long)]
    condition: String,
    #[arg(long)]
    prompt_id: String,
    #[arg(long)]
    transcript: PathBuf,
    #[arg(long)]
    workdir: PathBuf,
    /// The fixture the workdir was seeded from; files identical to it are not the agent's.
    #[arg(long)]
    fixture: Option<PathBuf>,
    #[arg(long)]
    results: PathBuf,
    #[arg(long)]
    date: String,
    #[arg(long)]
    commit: String,
    #[arg(long)]
    prompt_set_sha256: String,
    #[arg(long)]
    seconds: u64,
    #[arg(long)]
    exit_code: i32,
    #[arg(long)]
    timed_out: bool,
}

fn main() {
    let a = Args::parse();
    let result = (|| -> Result<String, String> {
        let format = Format::for_agent(&a.agent)?;
        let transcript = std::fs::read_to_string(&a.transcript)
            .map_err(|e| format!("{}: {e}", a.transcript.display()))?;
        let events = discovery::parse(format, &transcript);
        if discovery::never_acted(&events) {
            return Err(format!(
                "not scored: {} took no turn, so this is a harness or login failure, not a verdict; see {}",
                a.agent,
                a.transcript.display()
            ));
        }
        let changed = discovery::changed_files(&a.workdir, a.fixture.as_deref());
        let mut s = discovery::score(&events, &changed);
        if a.timed_out && s.instead == ["advice-only"] {
            s.instead = vec!["timed-out"];
        }
        let (model, cost) = match format {
            Format::Claude => discovery::claude_model_and_cost(&transcript),
            _ => (None, None),
        };
        let meta = RunMeta {
            date: a.date,
            commit: a.commit,
            prompt_set_sha256: a.prompt_set_sha256,
            agent: a.agent,
            agent_version: a.agent_version,
            model: model.or(a.model),
            condition: a.condition,
            prompt_id: a.prompt_id,
            seconds: a.seconds,
            timed_out: a.timed_out,
            exit_code: a.exit_code,
        };
        discovery::append(&a.results, &meta, &s, cost)?;
        Ok(discovery::table_line(&meta, &s))
    })();
    match result {
        Ok(line) => print!("{line}"),
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(1);
        }
    }
}
