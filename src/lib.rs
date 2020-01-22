#[macro_use]
extern crate log;
//extern crate csv;
//extern crate chrono;
//extern crate dtparse;

use std::io::{Error as IOError, Read};
use std::path::Path;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{Display, Formatter, Error as FmtError};

mod value;
mod row_table;

use value::Value;

///
/// The main interface into the mem_table library
///
pub trait Table {
    fn group_by(&self, column :&str) -> Result<HashMap<&Value, TableSlice<Self>>, TableError> where Self: Sized;
    fn unique(&self, column :&str) -> Result<HashSet<&Value>, TableError>;

    fn append(&mut self, row :&Vec<&str>);
    fn append_values(&mut self, row :Vec<Value>);

    fn find(&self, column :&str, value :&Value) -> Result<TableSlice<Self>, TableError> where Self: Sized;
    fn find_by<P: FnMut(&Vec<Value>) -> bool>(&self, predicate :P) -> Result<TableSlice<Self>, TableError> where Self: Sized;
}

#[derive(Debug, Clone)]
pub struct TableSlice<'a, T: Table> {
    columns: Vec<String>,
    rows: Vec<usize>,
    table: &'a T
}

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
    fn new(reason :&str) -> TableError {
        TableError { reason: String::from(reason) }
    }
}

// these are for tests
#[cfg(test)] extern crate simple_logger;
#[cfg(test)] extern crate rand;
#[cfg(test)] use std::sync::{Once};
use std::hash::{Hash, Hasher};

#[cfg(test)] static LOGGER_INIT: Once = Once::new();

