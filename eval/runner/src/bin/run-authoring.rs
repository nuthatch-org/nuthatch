use clap::Parser;
use nuthatch_eval_runner::authoring;

fn main() {
    let args = authoring::Args::parse();
    if args.self_test {
        std::process::exit(if authoring::self_test().passed() {
            0
        } else {
            1
        });
    }
    if let Err(message) = authoring::run(&args) {
        eprintln!("{message}");
        std::process::exit(1);
    }
}
