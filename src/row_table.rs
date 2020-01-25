use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::io::{Error as IOError, ErrorKind};

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
    columns: Vec<String>,   // the columns in this slice's view
    rows: Vec<usize>,       // index of the corresponding row in the Table
    table: &'a RowTable     // reference to the underlying table
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

        if columns.iter().collect::<HashSet<_>>().len() != columns.len() {
            return Err(IOError::new(ErrorKind::InvalidData, "Duplicate columns detected in the file"));
        }

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

    fn append_row(&mut self, row: Vec<Value>) -> Result<(), TableError> {
        // make sure the rows are the same width
        if self.width() != row.len() {
            let err_str = format!("Row widths don't match: {} != {}", self.width(), row.len());
            return Err(TableError::new(err_str.as_str()));
        }

        Ok(self.rows.push(row))
    }

    fn add_column_with<F: FnMut() -> Value>(&mut self, column_name :&str, mut f :F) -> Result<(), TableError> {
        // make sure we're not duplicating column names
        if let Ok(_) = self.column_position(column_name) {
            let err_str = format!("Attempting to add duplicate column: {} already exists", column_name);
            return Err(TableError::new(err_str.as_str()));
        }

        self.columns.push(String::from(column_name));
        self.rows.iter_mut().for_each(|row| row.push(f()));

        Ok( () )
    }
}

impl <'a> TableOperations<'a, RowTableSlice<'a>> for RowTable {
    fn iter(&self) -> RowIter {
        RowIter{ iter: self.rows.iter() }
    }

    fn into_iter(self) -> RowIntoIter {
        RowIntoIter(self.rows.into_iter())
    }

    #[inline]
    fn columns(&self) -> &Vec<String> {
        &self.columns
    }

    fn group_by(&'a self, column: &str) -> Result<HashMap<&Value, RowTableSlice>, TableError> {
        // get the position in the row we're concerned with
        let pos = self.column_position(column)?;

        let mut ret = HashMap::new();
        let empty_slice = RowTableSlice {
            columns: self.columns.clone(),
            rows: Vec::new(),
            table: self
        };

        // go through each row, and add them to our result
        for (i, row) in self.rows.iter().enumerate() {
            // get the slice, or create a new one
            let slice = ret.entry(&row[pos]).or_insert(empty_slice.clone());

            // insert this row
            slice.rows.push(i);
        }

        Ok(ret)
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

    #[inline]
    fn columns(&self) -> &Vec<String> {
        &self.columns
    }

    fn group_by(&'a self, column: &str) -> Result<HashMap<&Value, RowTableSlice<'a>>, TableError> {
        unimplemented!();
    }

    fn find_by<P: FnMut(&Vec<Value>) -> bool>(&'a self, mut predicate: P) -> Result<RowTableSlice<'a>, TableError> {
        let mut slice_rows = Vec::new();

        for &row_index in self.rows.iter() {
            // run the predicate against the row
            if predicate(&self.table.rows[row_index]) {
                slice_rows.push(row_index);
            }
        }

        Ok(RowTableSlice {
            columns: self.columns.clone(),
            rows: slice_rows,
            table: self.table
        })
    }
}

impl <'a> TableSlice<'a, RowTableSlice<'a>> for RowTableSlice<'a> { }

impl IntoIterator for RowTable {
    type Item = Vec<Value>;
    type IntoIter = RowIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        RowIntoIter(self.rows.into_iter())
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
    use ordered_float::OrderedFloat;

//    #[test]
//    fn from_csv() {
//        LOGGER_INIT.call_once(|| simple_logger::init_with_level(Level::Debug).unwrap()); // this will panic on error
//
//        let path = Path::new("/export/stock_stuff/199x.csv");
//
//        let start = Instant::now();
//        let table = RowTable::from_csv(path).expect("Error creating RowTable");
//        let end = Instant::now();
//
//        println!("DONE: {}s", (end-start).as_secs());
//    }

    #[test]
    fn new_append() {
        let mut t1 :RowTable = Table::new(&["A", "B"]);
        let mut t2 :RowTable = Table::new(&["A", "B"]);

        t1.append_row(vec![Value::new("1"), Value::new("2.3")]);
        t1.append_row(vec![Value::new("2"), Value::new("hello")]);

        assert_eq!(2, t1.iter().count());

        t2.append(t1);
        assert_eq!(2, t2.iter().count());
    }

    #[test]
    fn find() {
        let mut t1 :RowTable = Table::new(&["A", "B"]);

        t1.append_row(vec![Value::new("1"), Value::new("2.3")]);
        t1.append_row(vec![Value::new("1"), Value::new("7.5")]);
        t1.append_row(vec![Value::new("2"), Value::new("hello")]);

        let ts = t1.find("A", &Value::Integer(1)).expect("Error finding 1");

        ts.find("B", &Value::Float(OrderedFloat(2.3))).expect("Error finding 2.3");
    }
}