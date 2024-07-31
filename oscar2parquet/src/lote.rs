use arrow::{
    array::{ArrayRef, RecordBatch, StringBuilder, StructArray},
    datatypes::{DataType, Field},
};
use futures::{stream, StreamExt};
use isolang::Language;
use oscar_io::v3::Document;
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use std::{
    collections::HashSet,
    fs::{self, File},
    io::{BufRead, BufReader},
    path::PathBuf,
    sync::Arc,
};
use url::Url;
use walkdir::{DirEntry, WalkDir};

struct Lote {
    host: Option<String>,
    lang: Option<String>,
    url: Option<String>,
}

/// Converts `Vec<LoteBuilder>` into `StructArray`
#[derive(Debug, Default)]
struct LoteBuilder {
    host: StringBuilder,
    lang: StringBuilder,
    url: StringBuilder,
}

impl LoteBuilder {
    fn append(&mut self, lote: &Lote) {
        self.host.append_option(lote.host.as_ref());
        self.lang.append_option(lote.lang.as_ref());
        self.url.append_option(lote.url.as_ref());
    }

    /// Note: returns StructArray to allow nesting within another array if desired
    fn finish(&mut self) -> StructArray {
        let host = Arc::new(self.host.finish()) as ArrayRef;
        let host_field = Arc::new(Field::new("host", DataType::Utf8, false));

        let lang = Arc::new(self.lang.finish()) as ArrayRef;
        let lang_field = Arc::new(Field::new("lang", DataType::Utf8, false));

        let url = Arc::new(self.url.finish()) as ArrayRef;
        let url_field = Arc::new(Field::new("url", DataType::Utf8, true));

        StructArray::from(vec![
            (host_field, host),
            (lang_field, lang),
            (url_field, url),
        ])
    }
}

impl<'a> Extend<&'a Lote> for LoteBuilder {
    fn extend<T: IntoIterator<Item = &'a Lote>>(&mut self, iter: T) {
        iter.into_iter().for_each(|row| self.append(row));
    }
}

/// Converts a slice of [`Lote`] to a [`RecordBatch`]
fn rows_to_batch(rows: &[Lote]) -> RecordBatch {
    let mut builder = LoteBuilder::default();
    builder.extend(rows);
    RecordBatch::from(&builder.finish())
}

async fn process_file(file: DirEntry, dangerous_categories: HashSet<&str>, dst: PathBuf) {
    // Create the output file
    let mut path = PathBuf::new();
    path.push(dst);
    let lang = {
        let aux = file
            .file_name()
            .to_str()
            .unwrap()
            .strip_suffix("_meta.jsonl")
            .unwrap();
        if aux.chars().count() == 2 {
            match Language::from_639_1(aux) {
                Some(lang) => lang.to_639_3(),
                None => {
                    eprintln!("Could not parse language: {}", aux);
                    return;
                }
            }
        } else {
            aux
        }
    };
    path.push(format!("{}.parquet", lang));
    let parquet_path = path.clone();
    let parquet = File::create(path).unwrap();

    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .build();

    let mut aux_builder = LoteBuilder::default();
    let aux_records = RecordBatch::from(&aux_builder.finish());

    let mut writer = ArrowWriter::try_new(parquet, aux_records.schema(), Some(properties)).unwrap();

    let mut records: Vec<Lote> = vec![];

    let jsonl = {
        let file = File::open(file.path()).unwrap();
        BufReader::new(file)
    };

    println!("Processing file: {}", file.file_name().to_str().unwrap());

    let mut record_number = 0;
    let mut document_number = 0;

    for line in jsonl.lines() {
        document_number += 1;
        let line = line.unwrap();
        let document: Document = serde_json::from_str(&line).unwrap();
        match document.metadata().annotation() {
            Some(_) => {
                continue;
            }
            None => {}
        }
        match document.metadata().categories() {
            Some(categories) => {
                let result = categories.iter().try_for_each(|category| {
                    if dangerous_categories.contains(category.as_str()) {
                        return Err(());
                    }
                    Ok(())
                });
                if result.is_err() {
                    continue;
                }
            }
            None => {}
        }
        match document.metadata().harmful_pp() {
            Some(harmful_pp) => {
                if harmful_pp < 25.0 || harmful_pp > 100000.0 {
                    continue;
                }
            }
            None => {}
        }

        let url = match document.url() {
            Some(url) => url,
            None => {
                eprintln!("No URL found in document");
                continue;
            }
        };

        let host = match Url::parse(&url) {
            Ok(url) => match url.host_str() {
                Some(host) => host.to_owned(),
                None => {
                    eprintln!("Could not parse host: {}", url);
                    continue;
                }
            },
            Err(_) => {
                eprintln!("Could not parse URL: {}", url);
                continue;
            }
        };

        records.push(Lote {
            host: Some(host),
            lang: Some(lang.to_string()),
            url: Some(url),
        });

        record_number += 1;

        if records.len() >= 1_000_000 {
            let batch = rows_to_batch(&records);
            writer.write(&batch).expect("Writing batch");
            records.clear();
        }
    }
    let batch = rows_to_batch(&records);
    writer.write(&batch).expect("Writing batch");
    writer.close().unwrap();
    println!(
        "Finished processing file: {} \n  Wrote {} records to it out of {} documents",
        file.file_name().to_str().unwrap(),
        record_number,
        document_number
    );
    if record_number == 0 {
        println!(
            "    Removing empty file: {}",
            parquet_path
                .as_path()
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
        );
        fs::remove_file(parquet_path).unwrap();
    }
}

pub async fn extract_lote(src: &PathBuf, dst: &PathBuf) {
    let dangerous_categories = HashSet::from([
        "agressif",
        "adult",
        "cryptojacking",
        "dangerous_material",
        "phishing",
        "warez",
        "ddos",
        "hacking",
        "malware",
        "mixed_adult",
        "sect",
    ]);

    let file_paths: Vec<DirEntry> = WalkDir::new(src)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file())
        .filter(|e| e.file_name().to_str().unwrap().ends_with(".jsonl"))
        .filter(|e| !e.file_name().to_str().unwrap().starts_with("en_meta"))
        .filter(|e| !e.file_name().to_str().unwrap().starts_with("x-eml_meta"))
        .filter(|e| !e.file_name().to_str().unwrap().starts_with("multi_meta"))
        .collect();

    let stream = stream::iter(file_paths);

    let tasks = stream
        .enumerate()
        .for_each_concurrent(Some(50), |(_number, file)| {
            let dangerous_categories = dangerous_categories.clone();
            let dst = dst.clone();
            async move {
                let _task = tokio::task::spawn(process_file(file, dangerous_categories, dst)).await;
            }
        });
    tasks.await;
}
