use std::{env, fs, io::Write};
use yaml_rust2::YamlLoader;

fn main() {
    let args: Vec<String> = env::args().collect();
    let data = fs::read_to_string(args[1].clone()).unwrap();
    let docs = YamlLoader::load_from_str(&data).unwrap();
    let doc = &docs[0];
    let tags = &doc["context"]["generative_context"]["artifacts"]["tags"];
    let mut file = fs::File::create(args[2].clone()).unwrap();
    for tag in tags.as_vec().unwrap() {
        writeln!(file, "{}", tag["display_label"].as_str().unwrap()).unwrap();
    }
}
