use std::sync::Arc;

use arrow::{
    array::{ArrayRef, RecordBatch, StringBuilder, StructArray, UInt64Builder},
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
struct AnnotationBuilder {
    iso639_3: StringBuilder,
    examples_with_label: UInt64Builder,
}

impl AnnotationBuilder {
    fn append(&mut self, annotation: &Annotation) {
        self.iso639_3.append_value(annotation.iso639_3.as_str());
        self.examples_with_label
            .append_value(annotation.examples_with_label);
    }

    fn finish(&mut self) -> StructArray {
        let iso639_3 = Arc::new(self.iso639_3.finish()) as ArrayRef;
        let iso639_3_field = Arc::new(Field::new("iso639_3", DataType::Utf8, false));

        let examples_with_label = Arc::new(self.examples_with_label.finish()) as ArrayRef;
        let examples_with_label_field =
            Arc::new(Field::new("examples_with_label", DataType::UInt64, false));

        StructArray::from(vec![
            (iso639_3_field, iso639_3),
            (examples_with_label_field, examples_with_label),
        ])
    }
}

impl<'a> Extend<&'a Annotation> for AnnotationBuilder {
    fn extend<T: IntoIterator<Item = &'a Annotation>>(&mut self, iter: T) {
        iter.into_iter().for_each(|row| self.append(row));
    }
}

#[derive(Debug, Default)]
struct UserBuilder {
    id: UInt64Builder,
    username: StringBuilder,
    email: StringBuilder,
    annotations: AnnotationBuilder,
    total_examples_labeled: UInt64Builder,
}

impl UserBuilder {
    fn append(&mut self, user: &User) {
        self.id.append_value(user.id);
        self.username.append_value(user.username.as_str());
        self.email.append_value(user.email.as_str());
        self.annotations.extend(user.annotations.iter());
        self.total_examples_labeled
            .append_value(user.total_examples_labeled);
    }

    /// Note: returns StructArray to allow nesting within another array if desired
    fn finish(&mut self) -> StructArray {
        let id = Arc::new(self.id.finish()) as ArrayRef;
        let id_field = Arc::new(Field::new("id", DataType::Utf8, false));

        let username = Arc::new(self.username.finish()) as ArrayRef;
        let username_field = Arc::new(Field::new("username", DataType::Utf8, false));

        let email = Arc::new(self.email.finish()) as ArrayRef;
        let email_field = Arc::new(Field::new("email", DataType::Utf8, false));

        let annotations = Arc::new(self.annotations.finish());
        let annotations_field = Arc::new(Field::new(
            "annotations",
            DataType::Struct(annotations.fields().clone()),
            false,
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
            (annotations_field, annotations),
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
fn rows_to_batch(rows: &[User]) -> RecordBatch {
    let mut builder = UserBuilder::default();
    builder.extend(rows);
    RecordBatch::from(&builder.finish())
}
