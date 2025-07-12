use std::sync::Arc;

use arrow::{
    array::{ArrayRef, BooleanBuilder, RecordBatch, StringBuilder, StructArray},
    datatypes::{DataType, Field},
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct MadDocument {
    pub text: String,
    pub timestamp: Option<String>,
    pub url: Option<String>,
}

pub struct Document {
    pub text: String,
    pub lang: String,
    pub script: Option<String>,
    pub locale: Option<String>,
    pub timestamp: Option<String>,
    pub url: Option<String>,
    pub clean: bool,
    pub source: String,
    pub version: String,
}

#[derive(Debug, Default)]
pub struct MadBuilder {
    text: StringBuilder,
    lang: StringBuilder,
    script: StringBuilder,
    locale: StringBuilder,
    timestamp: StringBuilder,
    url: StringBuilder,
    clean: BooleanBuilder,
    source: StringBuilder,
    version: StringBuilder,
}

impl MadBuilder {
    fn append(&mut self, document: &Document) {
        self.text.append_value(document.text.as_str());
        self.lang.append_value(document.lang.as_str());
        self.script.append_option(document.script.as_ref());
        self.locale.append_option(document.locale.as_ref());
        self.timestamp.append_option(document.timestamp.as_ref());
        self.url.append_option(document.url.as_ref());
        self.clean.append_value(document.clean);
        self.source.append_value(document.source.as_str());
        self.version.append_value(document.version.as_str());
    }

    /// Note: returns StructArray to allow nesting within another array if desired
    pub fn finish(&mut self) -> StructArray {
        let text = Arc::new(self.text.finish()) as ArrayRef;
        let text_field = Arc::new(Field::new("text", DataType::Utf8, false));

        let lang = Arc::new(self.lang.finish()) as ArrayRef;
        let lang_field = Arc::new(Field::new("lang", DataType::Utf8, false));

        let script = Arc::new(self.script.finish()) as ArrayRef;
        let script_field = Arc::new(Field::new("script", DataType::Utf8, true));

        let locale = Arc::new(self.locale.finish()) as ArrayRef;
        let locale_field = Arc::new(Field::new("locale", DataType::Utf8, true));

        let timestamp = Arc::new(self.timestamp.finish()) as ArrayRef;
        let timestamp_field = Arc::new(Field::new("timestamp", DataType::Utf8, true));

        let url = Arc::new(self.url.finish()) as ArrayRef;
        let url_field = Arc::new(Field::new("url", DataType::Utf8, true));

        let clean = Arc::new(self.clean.finish()) as ArrayRef;
        let clean_field = Arc::new(Field::new("clean", DataType::Boolean, false));

        let source = Arc::new(self.source.finish()) as ArrayRef;
        let source_field = Arc::new(Field::new("source", DataType::Utf8, false));

        let version = Arc::new(self.version.finish()) as ArrayRef;
        let version_field = Arc::new(Field::new("version", DataType::Utf8, false));

        StructArray::from(vec![
            (text_field, text),
            (lang_field, lang),
            (script_field, script),
            (locale_field, locale),
            (timestamp_field, timestamp),
            (url_field, url),
            (clean_field, clean),
            (source_field, source),
            (version_field, version),
        ])
    }
}

impl<'a> Extend<&'a Document> for MadBuilder {
    fn extend<T: IntoIterator<Item = &'a Document>>(&mut self, iter: T) {
        iter.into_iter().for_each(|row| self.append(row));
    }
}

/// Converts a slice of [`Document`] to a [`RecordBatch`]
pub fn rows_to_batch(rows: &[Document]) -> RecordBatch {
    let mut builder = MadBuilder::default();
    builder.extend(rows);
    RecordBatch::from(&builder.finish())
}
