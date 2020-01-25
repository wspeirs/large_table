#[macro_use]
extern crate log;

use std::io::{Error as IOError};
use std::path::Path;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{Display, Formatter, Error as FmtError};

use rayon::prelude::*;
use csv::{Writer};

mod value;
mod row_table;

// expose some of the underlying structures from other files
pub use crate::row_table::RowTable;
pub use crate::value::Value;

/// The main interface into the mem_table library
pub trait Table<'a, T: TableSlice<'a, T>>: TableOperations<'a, T> {
    /// Create a blank RowTable
    fn new(columns :&[&str]) -> Self;

    /// Read in a CSV file, and construct a RowTable
    fn from_csv<P: AsRef<Path>>(path: P) -> Result<Self, IOError> where Self: Sized;

    /// Write a table out to a CSV file
    fn to_csv(&self, csv_path :&Path) -> Result<(), TableError> {
        let mut csv = Writer::from_path(csv_path).map_err(|e| TableError::new(e.to_string().as_str()))?;

        // write out the headers first
        csv.write_record(self.columns());

        // go through each row, writing the records converted to Strings
        for row in self.iter() {
            csv.write_record(row.iter().map(|f| String::from(f)));
        }

        Ok( () )
    }
}

/// Operations that can be performed on `Table`s or `TableSlice`s.
pub trait TableOperations<'a, T: TableSlice<'a, T>> {
    fn iter(&self) -> RowIter;
    fn into_iter(self) -> RowIntoIter;
//    fn row_mut_iter(&mut self) -> RowMutIter;

    fn columns(&self) -> &Vec<String>;
    fn len(&self) -> usize;
    fn width(&self) -> usize;

    fn group_by(&'a self, column :&str) -> Result<HashMap<&Value, T>, TableError>;
    fn unique(&self, column :&str) -> Result<HashSet<&Value>, TableError>;

    fn append<'b, S: TableSlice<'b, S>>(&mut self, table :impl Table<'b, S>) -> Result<(), TableError>;
    fn append_row(&mut self, row :Vec<Value>) -> Result<(), TableError>;

    /// Adds a column with `column_name` to the end of the table filling in all rows with `value`.
    /// This method works in parallel and is therefore usually faster than `add_column_with`
    fn add_column(&mut self, column_name :&str, value :&Value);

    /// Adds a column with `column_name` to the end of the table using `f` to generate the values for each row.
    /// This method works a row-at-a-time and therefore can be slower than `add_column`.
    fn add_column_with<F: FnMut() -> Value>(&mut self, column_name :&str, f :F);

    fn find(&'a self, column :&str, value :&Value) -> Result<T, TableError>;
    fn find_by<P: FnMut(&Vec<Value>) -> bool>(&'a self, predicate :P) -> Result<T, TableError>;
}

/// A `TableSlice` is a view into a `Table`.
pub trait TableSlice<'a, T: TableSlice<'a, T>>: TableOperations<'a, T> { }


//
// Row-oriented iterators
//
pub struct RowIter<'a> {
    iter: core::slice::Iter<'a, Vec<Value>>
}

impl <'a> Iterator for RowIter<'a> {
    type Item = &'a Vec<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl <'a> DoubleEndedIterator for RowIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back()
    }
}

impl <'a> ExactSizeIterator for RowIter<'a> { }

pub struct RowIntoIter(Vec<Vec<Value>>);

impl Iterator for RowIntoIter {
    type Item = Vec<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.pop()
    }
}

//pub struct RowTableIterMut {
//    mut_iter: core::slice::IterMut<'a, Vec<Value>>
//}
//
//impl  Iterator for RowTableIterMut {
//    type Item = &'a mut Vec<Value>;
//
//    fn next(&mut self) -> Option<Self::Item> {
//        self.mut_iter.next()
//    }
//}

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
use std::cell::Ref;
use rayon::iter::ParallelExtend;
use rayon::prelude::IntoParallelIterator;

#[cfg(test)] static LOGGER_INIT: Once = Once::new();

