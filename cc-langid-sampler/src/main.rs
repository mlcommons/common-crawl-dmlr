use clap::Parser;

mod cli;
mod sampler;

#[tokio::main]
async fn main() {
    let args = cli::Args::parse();

    let res = sampler::sample(&args.src, &args.dst).await;

    match res {
        Ok(_) => (),
        Err(e) => eprintln!("Error: {}", e),
    }
}
