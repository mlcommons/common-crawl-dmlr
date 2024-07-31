use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Extract the LOTE URLs from the OSCAR jsonl files
    Lote {
        /// Folder containing the indices
        #[arg(value_name = "INPUT FOLDER")]
        src: PathBuf,

        /// Parquet file to write
        #[arg(value_name = "DESTINATION FOLDER")]
        dst: PathBuf,
    },
}
