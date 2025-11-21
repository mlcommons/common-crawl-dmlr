use std::path::PathBuf;

use clap::Parser;

#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// File containing the annotations
    #[arg(value_name = "INPUT FILE")]
    pub src: PathBuf,

    /// Destination file for the extracted annotations
    #[arg(value_name = "DESTINATION FILE")]
    pub dst: PathBuf,
}
