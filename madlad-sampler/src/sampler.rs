use core::str::FromStr;
use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
};

use arrow::array::RecordBatch;
use flate2::read::GzDecoder;
use isolang::Language;
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use walkdir::{DirEntry, WalkDir};

use madlad_sampler::{
    errors::MadError,
    schemas::{Document, MadBuilder, MadDocument, rows_to_batch},
};

fn process_jsonline(
    line: String,
    lang: String,
    script: Option<String>,
    locale: Option<String>,
    clean: bool,
    version: String,
) -> Result<Document, MadError> {
    let mad_doc: MadDocument = serde_json::from_str(&line)?;
    let doc = Document {
        text: mad_doc.text,
        lang,
        script,
        locale,
        timestamp: mad_doc.timestamp,
        url: mad_doc.url,
        clean,
        source: "MADLAD".to_string(),
        version,
    };
    Ok(doc)
}

fn process_lang(dir: DirEntry) -> Result<Vec<Document>, MadError> {
    // This is insane, but we can do it because we know these are language codes
    let language = dir
        .path()
        .components()
        .next_back()
        .unwrap()
        .as_os_str()
        .to_os_string()
        .into_string()
        .unwrap();
    let new_version = match dir.path().to_str().is_some_and(|s| s.contains("data-v1p5")) {
        true => "v1.5".to_string(),
        false => "v1".to_string(),
    };
    let tag: String;
    let script: Option<String>;
    let locale: Option<String>;
    let lang_parts: Vec<&str> = language.split('_').collect();

    let bcp_tag = lang_parts[0];
    let iso_tag = match Language::from_str(bcp_tag) {
        Ok(language) => language.to_639_3(),
        Err(e) => return Err(e.into()),
    };
    match lang_parts.len() {
        1 => {
            tag = iso_tag.to_string();
            script = None;
            locale = None;
        }
        2 => {
            tag = iso_tag.to_string();
            script = Some(lang_parts[1].to_string());
            locale = None;
        }
        3 => {
            tag = iso_tag.to_string();
            script = Some(lang_parts[1].to_string());
            locale = Some(lang_parts[2].to_string());
        }
        _ => {
            return Err(format!("Invalid language format: {language}").into());
        }
    }
    println!(
        "Processing language: {}, script: {:?}, locale: {:?}",
        tag, script, locale
    );

    let mut records: Vec<Document> = vec![];
    let sample_size = 1000; // Limit the number of records to sample

    let clean_file_paths = WalkDir::new(dir.path())
        .min_depth(1)
        .max_depth(1)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| {
            e.file_type().is_file() && e.path().to_str().is_some_and(|s| s.contains("clean_"))
        });

    for clean_file in clean_file_paths {
        println!("Processing clean file: {}", clean_file.path().display());

        let jsonl = {
            let file = File::open(clean_file.path()).unwrap();
            let gzip = GzDecoder::new(file);
            BufReader::new(gzip)
        };

        for line in jsonl.lines() {
            let line = match line {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("Error reading line: {}", e);
                    continue; // Skip this line if there's an error
                }
            };
            let doc = match process_jsonline(
                line,
                tag.clone(),
                script.clone(),
                locale.clone(),
                true,
                new_version.clone(),
            ) {
                Ok(doc) => doc,
                Err(e) => {
                    eprintln!("Error Ecoding document: {}", e);
                    continue; // Skip this line if there's an error
                }
            };
            if doc.text.is_empty() {
                continue; // Skip empty documents
            }
            records.push(doc);
            if records.len() >= sample_size {
                break; // Limit to sample size
            }
        }
    }

    let clean_len = records.len();

    let noisy_file_paths = WalkDir::new(dir.path())
        .min_depth(1)
        .max_depth(1)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| {
            e.file_type().is_file() && e.path().to_str().is_some_and(|s| s.contains("noisy_"))
        });

    for noisy_file in noisy_file_paths {
        println!("Processing noisy file: {}", noisy_file.path().display());

        let jsonl = {
            let file = File::open(noisy_file.path()).unwrap();
            let gzip = GzDecoder::new(file);
            BufReader::new(gzip)
        };

        for line in jsonl.lines() {
            let line = match line {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("Error reading line: {}", e);
                    continue; // Skip this line if there's an error
                }
            };
            let doc = match process_jsonline(
                line,
                tag.clone(),
                script.clone(),
                locale.clone(),
                false,
                new_version.clone(),
            ) {
                Ok(doc) => doc,
                Err(e) => {
                    eprintln!("Error Ecoding document: {}", e);
                    continue; // Skip this line if there's an error
                }
            };
            if doc.text.is_empty() {
                continue; // Skip empty documents
            }
            records.push(doc);
            if records.len() >= 2 * clean_len {
                break; // Limit to sample size
            }
        }
    }

    Ok(records)
}

pub fn sample(src: &PathBuf, dst: &PathBuf) -> Result<(), String> {
    // Get all the langueg dirs
    let folder_paths: Vec<DirEntry> = WalkDir::new(src)
        .min_depth(1)
        .max_depth(1)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_dir())
        .collect();

    // Create the destination file
    let dst = File::create(dst).unwrap();

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .build();

    let mut aux_builder = MadBuilder::default();
    let aux_records = RecordBatch::from(&aux_builder.finish());

    let mut writer = ArrowWriter::try_new(dst, aux_records.schema(), Some(props)).unwrap();

    //iterate over the lang folders in parallel
    for lang in folder_paths {
        println!("Processing lang folder: {}", lang.path().display());
        match process_lang(lang.clone()) {
            Ok(records) => {
                if records.is_empty() {
                    println!("No records found for language: {}", lang.path().display());
                    continue;
                }

                let batch = rows_to_batch(&records);
                writer.write(&batch).expect("Writing batch");
            }
            Err(e) => {
                eprintln!("WARNING! One of the languages couldn't be processed: {}", e);
                continue;
            }
        };
    }
    writer.close().unwrap();
    Ok(())
}
