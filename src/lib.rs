#[macro_use]
extern crate log;

use std::io::{Error as IOError, Read};
use std::path::Path;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{Display, Formatter, Error as FmtError};

use rayon::prelude::*;

mod value;
mod row_table;

// expose some of the underlying structures from other files
pub use crate::row_table::RowTable;
pub use crate::value::Value;

///
/// The main interface into the mem_table library
///
pub trait Table {
    fn iter(&self) -> RowIter;
    fn into_iter(self) -> RowIntoIter;
//    fn row_mut_iter(&mut self) -> RowMutIter;

    fn columns(&self) -> &Vec<String>;
    fn len(&self) -> usize;
    fn width(&self) -> usize;

    // iterators that only return some of the columns
    // TODO: Think about this... maybe it's just a TableSliceIterator
//    fn col_iter(&self, cols :&Vec<&str>) -> ColIter;
//    fn col_into_iter(self, cols :&Vec<&str>) -> ColIntoIter;
//    fn col_mut_iter(&mut self, cols :&Vec<&str>) -> ColMutIter;

    fn group_by(&self, column :&str) -> Result<HashMap<&Value, TableSlice<Self>>, TableError> where Self: Sized;
    fn unique(&self, column :&str) -> Result<HashSet<&Value>, TableError>;

    fn append(&mut self, table :impl Table) -> Result<(), TableError>;
    fn append_row(&mut self, row :Vec<Value>) -> Result<(), TableError>;

    /// Adds a column with `column_name` to the end of the table filling in all rows with `value`.
    /// This method works in parallel and is therefore usually faster than `add_column_with`
    fn add_column(&mut self, column_name :&str, value :&Value);

    /// Adds a column with `column_name` to the end of the table using `f` to generate the values for each row.
    /// This method works a row-at-a-time and therefore can be slower than `add_column`.
    fn add_column_with<F: FnMut() -> Value>(&mut self, column_name :&str, f :F);

    fn find(&self, column :&str, value :&Value) -> Result<TableSlice<Self>, TableError> where Self: Sized;
    fn find_by<P: FnMut(&Vec<Value>) -> bool>(&self, predicate :P) -> Result<TableSlice<Self>, TableError> where Self: Sized;

    fn to_csv(&self, csv_path :&Path) -> Result<(), TableError>;
}

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

//pub struct RowTableIterMut<'a> {
//    mut_iter: core::slice::IterMut<'a, Vec<Value>>
//}
//
//impl <'a> Iterator for RowTableIterMut<'a> {
//    type Item = &'a mut Vec<Value>;
//
//    fn next(&mut self) -> Option<Self::Item> {
//        self.mut_iter.next()
//    }
//}

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
use std::cell::Ref;
use rayon::iter::ParallelExtend;
use rayon::prelude::IntoParallelIterator;

#[cfg(test)] static LOGGER_INIT: Once = Once::new();

