use std::{sync::Arc, vec};

use arrow::{
    array::{ArrayRef, ListBuilder, RecordBatch, StringBuilder, StructArray, UInt64Builder},
    datatypes::{DataType, Field},
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Query {
    pub content: Vec<UserSimple>,
}

#[derive(Debug, Deserialize)]
pub struct UserSimple {
    pub id: u64,
    pub username: String,
    pub email: String,
    #[serde(rename = "iso639-3")]
    pub iso639_3: String,
    pub examples_with_label: u64,
    pub total_examples_labeled: u64,
}

#[derive(Debug, Deserialize, Default)]
pub struct User {
    pub id: u64,
    pub username: String,
    pub email: String,
    pub annotations: Vec<Annotation>,
    pub total_examples_labeled: u64,
}

#[derive(Debug, Deserialize, Default)]
pub struct Annotation {
    pub iso639_3: String,
    pub examples_with_label: u64,
}

#[derive(Debug, Default)]
struct UserBuilder {
    id: UInt64Builder,
    username: StringBuilder,
    email: StringBuilder,
    annotations_langs: ListBuilder<StringBuilder>,
    numbers_by_lang: ListBuilder<UInt64Builder>,
    total_examples_labeled: UInt64Builder,
}

impl UserBuilder {
    fn append(&mut self, user: &User) {
        self.id.append_value(user.id);
        self.username.append_value(user.username.as_str());
        self.email.append_value(user.email.as_str());

        let mut ann_langs: Vec<Option<String>> = vec![];
        let mut ann_numbers: Vec<Option<u64>> = vec![];

        for annotation in &user.annotations {
            ann_langs.push(Some(annotation.iso639_3.clone()));
            ann_numbers.push(Some(annotation.examples_with_label));
        }

        self.annotations_langs.append_value(ann_langs);
        self.numbers_by_lang.append_value(ann_numbers);

        self.total_examples_labeled
            .append_value(user.total_examples_labeled);
    }

    /// Note: returns StructArray to allow nesting within another array if desired
    fn finish(&mut self) -> StructArray {
        let id = Arc::new(self.id.finish()) as ArrayRef;
        let id_field = Arc::new(Field::new("id", DataType::UInt64, false));

        let username = Arc::new(self.username.finish()) as ArrayRef;
        let username_field = Arc::new(Field::new("username", DataType::Utf8, false));

        let email = Arc::new(self.email.finish()) as ArrayRef;
        let email_field = Arc::new(Field::new("email", DataType::Utf8, false));

        let annotations_langs = Arc::new(self.annotations_langs.finish()) as ArrayRef;
        let annotations_langs_value_field = Arc::new(Field::new("item", DataType::Utf8, true));
        let annotations_langs_field = Arc::new(Field::new(
            "annotations_langs",
            DataType::List(annotations_langs_value_field),
            true,
        ));

        let numbers_by_lang = Arc::new(self.numbers_by_lang.finish()) as ArrayRef;
        let numbers_by_lang_value_field = Arc::new(Field::new("item", DataType::UInt64, true));
        let numbers_by_lang_field = Arc::new(Field::new(
            "numbers_by_lang",
            DataType::List(numbers_by_lang_value_field),
            true,
        ));

        let total_examples_labeled = Arc::new(self.total_examples_labeled.finish()) as ArrayRef;
        let total_examples_labeled_field = Arc::new(Field::new(
            "total_examples_labeled",
            DataType::UInt64,
            false,
        ));

        StructArray::from(vec![
            (id_field, id),
            (username_field, username),
            (email_field, email),
            (annotations_langs_field, annotations_langs),
            (numbers_by_lang_field, numbers_by_lang),
            (total_examples_labeled_field, total_examples_labeled),
        ])
    }
}

impl<'a> Extend<&'a User> for UserBuilder {
    fn extend<T: IntoIterator<Item = &'a User>>(&mut self, iter: T) {
        iter.into_iter().for_each(|row| self.append(row));
    }
}

/// Converts a slice of [`User`] to a [`RecordBatch`]
pub fn rows_to_batch(rows: &[User]) -> RecordBatch {
    let mut builder = UserBuilder::default();
    builder.extend(rows);
    RecordBatch::from(&builder.finish())
}
