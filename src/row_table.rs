use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::io::{Error as IOError};

use csv::Reader;
use rayon::prelude::*;

use crate::{Table, TableSlice, TableError, RowIter, RowIntoIter};
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

impl Table for RowTable {
    fn group_by(&self, column: &str) -> Result<HashMap<&Value, TableSlice<RowTable>>, TableError> {
        // get the position in the row we're concerned with
        let pos = if let Some(pos) = self.columns.iter().position(|c| c == column) {
            pos
        } else {
            return Err(TableError::new(format!("Column {} not found in table", column).as_str()));
        };

        let mut ret = HashMap::new();
        let empty_slice = TableSlice {
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

    ///
    /// Returns the unique values for a given column
    ///
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

    fn append(&mut self, row :&Vec<&str>) {
        let values = row.iter().map(|s| Value::new(*s)).collect::<Vec<_>>();

        self.append_values(values);
    }

    fn append_values(&mut self, row: Vec<Value>) {
        self.rows.push(row);
    }

    fn find(&self, column: &str, value: &Value) -> Result<TableSlice<RowTable>, TableError> {
        // get the position in the row we're concerned with
        let pos = if let Some(pos) = self.columns.iter().position(|c| c == column) {
            pos
        } else {
            return Err(TableError::new(format!("Column {} not found in table", column).as_str()));
        };

        self.find_by(|row| row[pos] == *value)
    }

    fn find_by<P: FnMut(&Vec<Value>) -> bool>(&self, mut predicate :P) -> Result<TableSlice<RowTable>, TableError> {
        let mut slice_rows = Vec::new();

        for (i, row) in self.rows.iter().enumerate() {
            if predicate(row) {
                slice_rows.push(i);
            }
        }

        Ok(TableSlice {
            columns: self.columns.clone(),
            rows: slice_rows,
            table: self
        })
    }

    fn iter(&self) -> RowIter {
        RowIter{ iter: self.rows.iter() }
    }

    fn into_iter(self) -> RowIntoIter {
        RowIntoIter(self.rows)
    }
}

impl IntoIterator for RowTable {
    type Item = Vec<Value>;
    type IntoIter = RowIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        RowIntoIter(self.rows)
    }
}

impl RowTable {
    ///
    /// Create a blank RowTable
    ///
    pub fn new(columns :&[&str]) -> RowTable {
        RowTable {
            columns: columns.into_iter().map(|s| String::from(*s)).collect::<Vec<_>>(),
            rows: Vec::new()
        }
    }

    ///
    /// Read in a CSV file, and construct a RowTable
    ///
    pub fn from_csv<P: AsRef<Path>>(path: P) -> Result<impl Table, IOError> {
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


#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Instant;

    use log::Level;
    use chrono::Duration;

    use crate::LOGGER_INIT;
    use crate::row_table::RowTable;
    use crate::Table;

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
        let mut table = RowTable::new(&["A", "B"]);

        table.append(&vec!["1", "2.3"]);

        for row in table.iter() {
            println!("{:?}", row);
        }

        for row in table {
            println!("{:?}", row);
        }

        for row in &table {
            println!("{:?}", row);
        }
    }
}