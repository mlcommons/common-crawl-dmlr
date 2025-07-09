use core::fmt;

#[derive(Debug)]
/// An error type for the sampling process, encapsulating various error sources.
pub enum MadError {
    ParseLanguageError(isolang::ParseLanguageError),
    Custom(String),
    SerdeJson(serde_json::Error),
}

impl fmt::Display for MadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            MadError::ParseLanguageError(ref err) => err.fmt(f),
            MadError::SerdeJson(ref err) => err.fmt(f),
            MadError::Custom(ref err) => err.fmt(f),
        }
    }
}

impl From<isolang::ParseLanguageError> for MadError {
    fn from(err: isolang::ParseLanguageError) -> Self {
        MadError::ParseLanguageError(err)
    }
}

impl From<serde_json::Error> for MadError {
    fn from(err: serde_json::Error) -> Self {
        MadError::SerdeJson(err)
    }
}

impl From<String> for MadError {
    fn from(err: String) -> Self {
        MadError::Custom(err)
    }
}
