//! mem_table is an in-memory table of data, modeled after [Pandas](https://pandas.pydata.org/) for Python.
#[macro_use]
extern crate log;

use std::io::{Error as IOError, Read};
use std::path::Path;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{Display, Formatter, Error as FmtError};
use std::hash::{Hash, Hasher};
use std::cell::Ref;
use std::iter::FusedIterator;
use std::ops::Index;

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

    /// Groups values that are the same together, providing a `TableSlice` for each unique value found.
    /// Note: You cannot hash or compare properly `f64`, so any `f64` value is wrapped in [`OrderedFloat`](https://docs.rs/ordered-float/*/ordered_float/).
    fn group_by(&self, column :&str) -> Result<HashMap<&Value, TableSlice<Self>>, TableError> where Self: Sized;

    /// Returns the unique values for a given column
    /// Note: You cannot hash or compare properly `f64`, so any `f64` value is wrapped in [`OrderedFloat`](https://docs.rs/ordered-float/*/ordered_float/).
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

    /// Sorts the rows in the table, in an unstable way, in ascending order, by the columns provided, in the order they're provided.
    ///
    /// If the columns passed are `A`, `B`, `C`, then the rows will be sored by column `A` first, then `B`, then `C`.
    fn sort(&mut self, columns :&[&str]) -> Result<(), TableError> { unimplemented!(); }
    fn sort_by<F: FnMut(&Vec<Value>, &Vec<Value>) -> Ordering>(&mut self, columns :&[&str], compare :F) { unimplemented!(); }
    fn stable_sort(&mut self, columns :&[&str]) -> Result<(), TableError> { unimplemented!(); }
    fn stable_sort_by<F: FnMut(&Vec<Value>, &Vec<Value>) -> Ordering>(&mut self, compare :F) -> Result<(), TableError> { unimplemented!(); }

    fn split_rows_at<T: Table>(&self, mid :usize) -> (TableSlice<T>, TableSlice<T>) { unimplemented!(); }

    /// Writes the data out to a CSV file
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
#[cfg(test)] static LOGGER_INIT: Once = Once::new();

