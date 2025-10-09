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
    /// Extract annotations from Dynabench files
    ExtractAnnotations {
        /// File containing the annotations
        #[arg(value_name = "INPUT FILE")]
        src: PathBuf,

        /// Destination file for the extracted annotations
        #[arg(value_name = "DESTINATION FILE")]
        dst: PathBuf,
    },
}
