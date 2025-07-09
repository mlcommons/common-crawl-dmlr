use core::str::FromStr;
use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
};

use arrow::array::RecordBatch;
use isolang::Language;
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use walkdir::{DirEntry, WalkDir};

use madlad_sampler::{
    errors::MadError,
    schemas::{Document, MadDocument},
};

fn write_to_parquet(batch: RecordBatch, folder_path: &PathBuf, lang: &str, part: usize) {
    let mut path = folder_path.clone();
    path.push(format!("{}_part_{}.parquet", lang, part));
    let parquet = File::create(path).unwrap();

    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .build();

    let mut writer = ArrowWriter::try_new(parquet, batch.schema(), Some(properties)).unwrap();
    writer.write(&batch).expect("Writing batch");
    writer.close().unwrap();
}

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
        lang: lang,
        script: script,
        locale: locale,
        timestamp: mad_doc.timestamp,
        url: mad_doc.url,
        clean: clean,
        source: "MADLAD".to_string(),
        version: version,
    };
    Ok(doc)
}

fn process_lang(dir: DirEntry, _dst: &PathBuf) -> Result<(), MadError> {
    // This is insane, but we can do it because we know these are language codes
    let language = dir
        .path()
        .components()
        .last()
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

    let clean_file_paths = WalkDir::new(dir.path())
        .min_depth(1)
        .max_depth(1)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| {
            e.file_type().is_file() && e.path().to_str().is_some_and(|s| s.contains("clean"))
        });

    for clean_file in clean_file_paths {
        println!("Processing clean file: {}", clean_file.path().display());
        let mut records: Vec<Document> = vec![];

        let jsonl = {
            let file = File::open(clean_file.path()).unwrap();
            BufReader::new(file)
        };

        let sample_size = 1000;

        for line in jsonl.lines() {
            let line = line.unwrap();
            let doc = process_jsonline(
                line,
                tag.clone(),
                script.clone(),
                locale.clone(),
                true,
                new_version.clone(),
            )?;
            if doc.text.is_empty() {
                continue; // Skip empty documents
            }
            records.push(doc);
            if records.len() >= sample_size {
                break; // Limit to sample size
            }
        }
    }
    Ok(())
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

    //iterate over the lang folders in parallel
    for lang in folder_paths {
        println!("Processing lang folder: {}", lang.path().display());
        match process_lang(lang, dst) {
            Ok(_) => {}
            Err(e) => {
                eprintln!("WARNING! One of the languages couldn't be processed: {}", e);
                continue;
            }
        };
    }
    Ok(())
}
