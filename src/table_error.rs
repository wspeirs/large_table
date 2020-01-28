use std::error::Error;
use std::fmt::{Display, Formatter, Error as FmtError};

#[derive(Debug, Clone)]
pub struct TableError {
    reason: String
}

impl Error for TableError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        // Generic error, underlying cause isn't tracked.
        None
    }
}

impl Display for TableError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        writeln!(f, "{}", self.reason)
    }
}

impl TableError {
    pub(crate) fn new(reason :&str) -> TableError {
        TableError { reason: String::from(reason) }
    }
}
