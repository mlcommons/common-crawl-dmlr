use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Query {
    pub content: Vec<Document>,
}

#[derive(Debug, Deserialize)]
pub struct Document {
    pub id: u64,
    pub cid: u64,
    pub uid: u64,
    pub text: String,
    pub input_json: String,
}

#[derive(Debug, Deserialize)]
pub struct Annotation {
    pub labels: Vec<Label>,
}

#[derive(Debug, Deserialize)]
pub struct Label {
    pub start: u64,
    pub end: u64,
    pub text: String,
    pub tag: String,
}
