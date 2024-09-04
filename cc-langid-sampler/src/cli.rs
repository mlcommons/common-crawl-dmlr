use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "cc-langid-sampler")]
#[command(author = "Pedro Ortiz Suarez <pedro@commoncrawl.org>")]
#[command(version = "0.1.0")]
#[command(about = "Compute host and domain db", long_about = None)]
pub struct Args {
    /// Folder containing the indices
    #[arg(value_name = "INPUT FOLDER")]
    pub src: PathBuf,

    /// Parquet file to write
    #[arg(value_name = "DESTINATION FILE")]
    pub dst: PathBuf,
}