use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "madlad-sampler")]
#[command(author = "Pedro Ortiz Suarez <pedro@commoncrawl.org>")]
#[command(version = "0.1.0")]
#[command(about = "Sample documents from MADLAD", long_about = None)]
pub struct Args {
    /// Folder containing the data
    #[arg(value_name = "INPUT FOLDER")]
    pub src: PathBuf,

    /// Weather it is the new version of MADLAD or not
    #[arg(short, long)]
    pub new_version: bool,

    /// Parquet file to write
    #[arg(value_name = "DESTINATION FILE")]
    pub dst: PathBuf,
}
