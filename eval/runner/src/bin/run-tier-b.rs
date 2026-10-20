use clap::Parser;
use nuthatch_eval_runner::tier_b;

fn main() {
    let args = tier_b::Args::parse();
    if args.self_test {
        std::process::exit(if tier_b::self_test().passed() { 0 } else { 1 });
    }
    if let Err(message) = tier_b::run(&args) {
        eprintln!("{message}");
        std::process::exit(1);
    }
}
