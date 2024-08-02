use clap::Parser;

use crate::cli::Commands;

mod cli;
mod convert;
mod oscar;

#[tokio::main]
async fn main() {
    let args = cli::Cli::parse();
    match args.command.unwrap() {
        Commands::Lote { src, dst } => {
            convert::convert_to_parquet(&src, &dst).await;
        }
    }
}
