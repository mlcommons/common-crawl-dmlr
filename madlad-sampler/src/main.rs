use clap::Parser;

mod cli;
mod sampler;

fn main() {
    let args = cli::Args::parse();

    let res = sampler::sample(&args.src, &args.dst);

    match res {
        Ok(_) => (),
        Err(e) => eprintln!("Error: {}", e),
    }
}
