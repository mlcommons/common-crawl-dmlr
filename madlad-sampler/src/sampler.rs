use std::{fs::File, path::PathBuf};

use arrow::array::RecordBatch;
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use walkdir::{DirEntry, WalkDir};

use madlad_sampler::schemas::Document;

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

fn process_lang(dir: DirEntry, _dst: &PathBuf) -> Result<(), String> {
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
    let code: String;
    let mut script = String::new();
    let mut locale = String::new();
    let lang_parts: Vec<&str> = language.split('_').collect();
    match lang_parts.len() {
        1 => {
            code = lang_parts[0].to_string();
        }
        2 => {
            code = lang_parts[0].to_string();
            script = lang_parts[1].to_string();
        }
        3 => {
            code = lang_parts[0].to_string();
            script = lang_parts[1].to_string();
            locale = lang_parts[2].to_string();
        }
        _ => {
            return Err(format!("Invalid language format: {}", language));
        }
    }
    println!(
        "Processing language: {}, script: {}, locale: {}",
        code, script, locale
    );
    Ok(())
}

pub fn sample(src: &PathBuf, dst: &PathBuf, _new_version: bool) -> Result<(), String> {
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
        process_lang(lang, dst)?;
    }
    Ok(())
}
