use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::Result;
use datafusion::prelude::*;
use std::{path::PathBuf, vec};

use rand::{seq::SliceRandom, thread_rng};
use walkdir::{DirEntry, WalkDir};

async fn process_lang(lang: DirEntry, dst: &PathBuf) -> Result<()> {
    let language = lang.path().components().last().unwrap();

    let mut file_paths: Vec<String> = WalkDir::new(lang.path())
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file())
        .filter(|e| e.file_name().to_str().unwrap().ends_with(".parquet"))
        .map(|e| e.path().to_str().unwrap().to_owned())
        .collect();

    file_paths.shuffle(&mut thread_rng());

    let ctx = SessionContext::new();
    // read parque files into a DataFrame
    let df = ctx
        .read_parquet(file_paths, ParquetReadOptions::default())
        .await?;

    // filter out rows with quality_warnings

    let dangerous_categories = make_array(vec![
        lit("agressif"),
        lit("adult"),
        lit("cryptojacking"),
        lit("dangerous_material"),
        lit("phishing"),
        lit("porn"),
        lit("warez"),
        lit("ddos"),
        lit("hacking"),
        lit("malware"),
        lit("mixed_adult"),
        lit("sect"),
    ]);

    let df = df.filter(col("quality_warnings").is_null())?;
    // let df = df.filter(array_has(col("categories"), lit("agressif")).not())?;
    // let df = df.filter(array_has(col("categories"), lit("adult")).not())?;
    // let df = df.filter(array_has(col("categories"), lit("cryptojacking")).not())?;
    // let df = df.filter(array_has(col("categories"), lit("dangerous_material")).not())?;
    // let df = df.filter(array_has(col("categories"), lit("phishing")).not())?;
    // let df = df.filter(array_has(col("categories"), lit("warez")).not())?;
    // let df = df.filter(array_has(col("categories"), lit("ddos")).not())?;
    // let df = df.filter(array_has(col("categories"), lit("hacking")).not())?;
    // let df = df.filter(array_has(col("categories"), lit("malware")).not())?;
    // let df = df.filter(array_has(col("categories"), lit("mixed_adult")).not())?;
    // let df = df.filter(array_has(col("categories"), lit("sect")).not())?;
    let df = df.filter(array_has_any(col("categories"), dangerous_categories).not())?;
    let df = df.limit(0, Some(1000))?;

    let mut dst = dst.clone();

    dst.push(format!(
        "{}.{}",
        language.as_os_str().to_str().unwrap(),
        "parquet"
    ));

    // stream the contents of the DataFrame to the `example.parquet` file
    let result = df
        .write_parquet(
            dst.to_str().unwrap(),
            DataFrameWriteOptions::new(),
            None, // writer_options
        )
        .await;
    if result.is_err() {
        eprintln!("Error writing parquet file: {:?}", result);
    }
    Ok(())
}

pub async fn sample(src: &PathBuf, dst: &PathBuf) -> Result<()> {
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
        process_lang(lang, dst).await?;
    }
    Ok(())
}
