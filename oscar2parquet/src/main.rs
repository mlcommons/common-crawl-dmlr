use clap::Parser;

use crate::cli::Commands;

mod cli;
mod lote;

#[tokio::main]
async fn main() {
    let args = cli::Cli::parse();
    match args.command.unwrap() {
        Commands::Lote { src, dst } => {
            lote::extract_lote(&src, &dst).await;
        }
    }
}
