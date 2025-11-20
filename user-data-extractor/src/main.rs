use std::{
    fs::{read_to_string, File},
    io::{BufRead, BufReader},
    path::PathBuf,
};

use clap::Parser;

use crate::cli::Commands;

mod cli;
mod schemas;

fn process_file(src: PathBuf, dst: PathBuf) {
    let file = read_to_string(src).unwrap();
    let json: annotations::Query = serde_json::from_str(&file).unwrap();

    for document in json.content {
        let ann: annotations::Annotation = serde_json::from_str(&document.input_json)
            .unwrap_or_else(|_| {
                eprintln!("Failed to parse JSON: {}", document.input_json);
                annotations::Annotation { labels: vec![] }
            });
        if ann.labels.is_empty() {
            eprintln!("No labels found in document: {}", document.input_json);
            continue;
        }
        for label in ann.labels {
            let text = label.text.replace("\\n", "\n");
            for line in text.lines() {
                if line.trim().is_empty() {
                    continue;
                }
                println!("{}\t{}", label.tag, line);
            }
        }
    }
}

fn main() {
    let args = cli::Cli::parse();
    match args.command.unwrap() {
        Commands::ExtractAnnotations { src, dst } => {
            process_file(src, dst);
        }
    }
}
