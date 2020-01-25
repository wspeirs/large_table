use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::io::{Error as IOError};

use csv::{Reader};
use rayon::prelude::*;

use crate::{Table, TableOperations, TableSlice, TableError, RowIter, RowIntoIter};
use crate::value::Value;
use std::ops::Index;
use std::collections::hash_map::RandomState;

///
/// A table with row-oriented data
///
#[derive(Debug, Clone)]
pub struct RowTable {
    columns: Vec<String>,
    rows: Vec<Vec<Value>>
}

#[derive(Debug, Clone)]
pub struct RowTableSlice<'a> {
    columns: Vec<String>,
    rows: Vec<usize>,
    table: &'a RowTable
}

impl <'a> Table<'a, RowTableSlice<'a>> for RowTable {
    /// Create a blank RowTable
    fn new(columns :&[&str]) -> Self {
        RowTable {
            columns: columns.into_iter().map(|s| String::from(*s)).collect::<Vec<_>>(),
            rows: Vec::new()
        }
    }

    /// Read in a CSV file, and construct a RowTable
    fn from_csv<P: AsRef<Path>>(path: P) -> Result<Self, IOError> {
        let mut csv = Reader::from_path(path)?;

        // get the headers from the CSV file
        let columns = csv.headers()?.iter().map(|h| String::from(h)).collect::<Vec<_>>();
        let mut rows = Vec::new();

        // go through each row, in parallel, and insert it into rows vector
        rows.par_extend(csv.records().par_bridge().map(|result| {
            if result.is_err() {
                panic!("Error parsing row: {:?}", result.err().unwrap());
            }

            let csv_row = result.unwrap();

            let mut table_row = Vec::with_capacity(columns.len());

            for c in 0..columns.len() {
                let val = csv_row.get(c);

                table_row.push(match val { Some(s) => Value::new(s), None => Value::Empty });
            }

            table_row
        }));

        Ok(RowTable {
            columns,
            rows
        })
    }
}

impl <'a> TableOperations<'a, RowTableSlice<'a>> for RowTable {
    fn iter(&self) -> RowIter {
        RowIter{ iter: self.rows.iter() }
    }

    fn into_iter(self) -> RowIntoIter {
        RowIntoIter(self.rows)
    }

    #[inline]
    fn columns(&self) -> &Vec<String> {
        &self.columns
    }

    #[inline]
    fn len(&self) -> usize {
        self.rows.len()
    }

    #[inline]
    fn width(&self) -> usize {
        if self.rows.is_empty() {
            self.columns.len()
        } else {
            self.rows.first().unwrap().len()
        }
    }

    fn group_by(&'a self, column: &str) -> Result<HashMap<&Value, RowTableSlice>, TableError> {
        // get the position in the row we're concerned with
        let pos = if let Some(pos) = self.columns.iter().position(|c| c == column) {
            pos
        } else {
            return Err(TableError::new(format!("Column {} not found in table", column).as_str()));
        };

        let mut ret = HashMap::new();
        let empty_slice = RowTableSlice {
            columns: self.columns.clone(),
            rows: Vec::new(),
            table: self
        };

        // go through each row, and add them to our result
        for (i, row) in self.rows.iter().enumerate() {
            // get the slice, or create a new one
//            let slice = ret.get_mut(&row[pos]).unwrap_or(empty_slice.clone());

            let slice = ret.entry(&row[pos]).or_insert(empty_slice.clone());

            // insert this row
            slice.rows.push(i);
        }

        Ok(ret)
    }

    /// Returns the unique values for a given column
    fn unique(&self, column: &str) -> Result<HashSet<&Value, RandomState>, TableError> {
        // get the position in the row we're concerned with
        let pos = if let Some(pos) = self.columns.iter().position(|c| c == column) {
            pos
        } else {
            return Err(TableError::new(format!("Column {} not found in table", column).as_str()));
        };

        let mut ret = HashSet::new();

        // in parallel insert the values into the HashSet
        ret.par_extend(self.rows.par_iter().map(|row| &row[pos]));

        Ok(ret)
    }

    fn append<'b, T: TableSlice<'b, T>>(&mut self, table :impl Table<'b, T>) -> Result<(), TableError> {
        // make sure the columns are the same
        if !self.columns.iter().zip(table.columns().iter()).all(|(a, b)| a == b) {
            let err_str = format!("Columns don't match between tables: {:?} != {:?}", self.columns, table.columns());
            return Err(TableError::new(err_str.as_str()));
        }

        Ok(self.rows.extend(table.into_iter()))
    }

    fn append_row(&mut self, row: Vec<Value>) -> Result<(), TableError> {
        // make sure the rows are the same width
        if self.width() != row.len() {
            let err_str = format!("Row widths don't match: {} != {}", self.width(), row.len());
            return Err(TableError::new(err_str.as_str()));
        }

        Ok(self.rows.push(row))
    }

    fn add_column(&mut self, column_name :&str, value :&Value) {
        self.columns.push(String::from(column_name));
        self.rows.par_iter_mut().for_each(|row| row.push(value.clone()));
    }

    fn add_column_with<F: FnMut() -> Value>(&mut self, column_name :&str, mut f :F) {
        self.columns.push(String::from(column_name));
        self.rows.iter_mut().for_each(|row| row.push(f()));
    }

    fn find(&'a self, column: &str, value: &Value) -> Result<RowTableSlice, TableError> {
        // get the position in the row we're concerned with
        let pos = if let Some(pos) = self.columns.iter().position(|c| c == column) {
            pos
        } else {
            return Err(TableError::new(format!("Column {} not found in table", column).as_str()));
        };

        self.find_by(|row| row[pos] == *value)
    }

    fn find_by<P: FnMut(&Vec<Value>) -> bool>(&'a self, mut predicate :P) -> Result<RowTableSlice, TableError> {
        let mut slice_rows = Vec::new();

        for (i, row) in self.rows.iter().enumerate() {
            if predicate(row) {
                slice_rows.push(i);
            }
        }

        Ok(RowTableSlice {
            columns: self.columns.clone(),
            rows: slice_rows,
            table: self
        })
    }
}

impl <'a> TableOperations<'a, RowTableSlice<'a>> for RowTableSlice<'a> {
    fn iter(&self) -> RowIter {
        unimplemented!()
    }

    fn into_iter(self) -> RowIntoIter {
        unimplemented!()
    }

    fn columns(&self) -> &Vec<String> {
        &self.columns
    }

    fn len(&self) -> usize {
        self.rows.len()
    }

    fn width(&self) -> usize {
        self.columns.len()
    }

    fn group_by(&'a self, column: &str) -> Result<HashMap<&Value, RowTableSlice<'a>>, TableError> {
        unimplemented!()
    }

    fn unique(&self, column: &str) -> Result<HashSet<&Value, RandomState>, TableError> {
        unimplemented!()
    }

    fn append<'b, S: TableSlice<'b, S>>(&mut self, table: impl Table<'b, S>) -> Result<(), TableError> {
        unimplemented!()
    }

    fn append_row(&mut self, row: Vec<Value>) -> Result<(), TableError> {
        unimplemented!()
    }

    fn add_column(&mut self, column_name: &str, value: &Value) {
        unimplemented!()
    }

    fn add_column_with<F: FnMut() -> Value>(&mut self, column_name: &str, f: F) {
        unimplemented!()
    }

    fn find(&'a self, column: &str, value: &Value) -> Result<RowTableSlice<'a>, TableError> {
        unimplemented!()
    }

    fn find_by<P: FnMut(&Vec<Value>) -> bool>(&'a self, predicate: P) -> Result<RowTableSlice<'a>, TableError> {
        unimplemented!()
    }
}

impl <'a> TableSlice<'a, RowTableSlice<'a>> for RowTableSlice<'a> { }

impl IntoIterator for RowTable {
    type Item = Vec<Value>;
    type IntoIter = RowIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        RowIntoIter(self.rows)
    }
}



#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Instant;

    use log::Level;
    use chrono::Duration;

    use crate::LOGGER_INIT;
    use crate::row_table::RowTable;
    use crate::{Table, TableOperations};
    use crate::value::Value;

    #[test]
    fn from_csv() {
        LOGGER_INIT.call_once(|| simple_logger::init_with_level(Level::Debug).unwrap()); // this will panic on error

        let path = Path::new("/export/stock_stuff/199x.csv");

        let start = Instant::now();
        let table = RowTable::from_csv(path).expect("Error creating RowTable");
        let end = Instant::now();

        println!("DONE: {}s", (end-start).as_secs());
    }

    #[test]
    fn new_append() {
        let mut table :RowTable = Table::new(&["A", "B"]);
        let row = ["1", "2.3"].iter().map(|x| Value::new(x)).collect::<Vec<_>>();

        table.append_row(row);

        for row in table.iter() {
            println!("{:?}", row);
        }

        for row in table {
            println!("{:?}", row);
        }
    }
}