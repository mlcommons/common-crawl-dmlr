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

fn process_lang(lang: DirEntry, _dst: &PathBuf) -> Result<(), String> {
    let language = lang.path().components().last().unwrap();
    println!("Processing language: {:?}", language);
    Ok(())
}

pub fn sample(src: &PathBuf, dst: &PathBuf, _new_version: bool) -> Result<(), String> {
    // find all the lang folders containing the parquet files in the src folder
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
