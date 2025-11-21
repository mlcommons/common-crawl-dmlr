use clap::Parser;
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use std::{
    collections::HashMap,
    fs::{File, read_to_string},
    path::PathBuf,
};

mod cli;
mod schemas;

fn process_file(src: PathBuf, dst: PathBuf) {
    let file = read_to_string(src).unwrap();
    let json: schemas::Query = serde_json::from_str(&file).unwrap();

    let mut users_map: HashMap<u64, schemas::User> = HashMap::new();

    for simple_user in json.content {
        users_map
            .entry(simple_user.id)
            .and_modify(|user| {
                user.annotations.push(schemas::Annotation {
                    iso639_3: simple_user.iso639_3.clone(),
                    examples_with_label: simple_user.examples_with_label,
                })
            })
            .or_insert(schemas::User {
                id: simple_user.id,
                username: simple_user.username,
                email: simple_user.email,
                annotations: vec![schemas::Annotation {
                    iso639_3: simple_user.iso639_3.clone(),
                    examples_with_label: simple_user.examples_with_label,
                }],
                total_examples_labeled: simple_user.total_examples_labeled,
            });
    }
    let users = users_map.into_values().collect::<Vec<schemas::User>>();

    let batch = schemas::rows_to_batch(&users);

    let parquet = File::create(dst).unwrap();

    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .build();

    let mut writer = ArrowWriter::try_new(parquet, batch.schema(), Some(properties)).unwrap();
    writer.write(&batch).expect("Writing batch");
    writer.close().unwrap();
}

fn main() {
    let args = cli::Args::parse();
    process_file(args.src, args.dst);
}
