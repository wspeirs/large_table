use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::io::{Error as IOError, ErrorKind};
use std::ops::Index;
use std::collections::hash_map::RandomState;
use std::iter::Map;
use std::rc::Rc;


use csv::{Reader};
use rayon::prelude::*;

use crate::{Table, TableOperations, TableSlice, TableError, OwnedRow, BorrowedRow};
use crate::value::Value;

/// A table with row-oriented data
#[derive(Debug, Clone)]
pub struct RowTable {
    columns: Vec<String>,
    rows: Vec<Vec<Value>>
}

impl <'a> Table<'a> for RowTable {
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

    fn append_row<R>(&mut self, row: R) -> Result<(), TableError> {
        // make sure the rows are the same width
//        if self.width() != row.width() {
//            let err_str = format!("Row width doesn't match table width: {} != {}", row.width(), self.width());
//            return Err(TableError::new(err_str.as_str()));
//        }
//
//        // convert to a Vec
//        let row_vec = row.iter().cloned().collect::<Vec<_>>();
//
//        Ok(self.rows.push(row_vec))
        unimplemented!()
    }

    fn add_column_with<F: FnMut() -> Value>(&mut self, column_name :&str, mut f :F) -> Result<(), TableError> {
        // make sure we're not duplicating column names
        if let Ok(_) = self.column_position(column_name) {
            let err_str = format!("Attempting to add duplicate column: {} already exists", column_name);
            return Err(TableError::new(err_str.as_str()));
        }

        // add the column name to our list of columns
        self.columns.push(String::from(column_name));

        // add the default value for the column
        self.rows.iter_mut().for_each(|row| row.push(f()));

        Ok( () )
    }
}

impl <'a> TableOperations<'a> for RowTable {
    type TableSliceType = RowTableSlice<'a>;
    type IntoIter = RowTableIntoIter;
    type Iter = RowTableIter<'a>;
    type MutIter = RowTableMutIter<'a>;

    fn into_iter(self) -> RowTableIntoIter {
        RowTableIntoIter{ columns: Rc::new(self.columns), iter: self.rows.into_iter() }
    }

    fn iter(&'a self) -> RowTableIter<'a> {
        self.into_iter()
    }

    fn iter_mut(&'a mut self) -> RowTableMutIter<'a> {
        self.into_iter()
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

#[derive(Debug, Clone)]
pub struct RowTableSlice<'a> {
    columns: Vec<String>,   // the columns in this slice's view
    rows: Vec<usize>,       // index of the corresponding row in the Table
    table: &'a RowTable     // reference to the underlying table
}

impl <'a> TableOperations<'a> for RowTableSlice<'a> {
    type TableSliceType = RowTableSlice<'a>;
    type IntoIter = RowTableIntoIter;
    type Iter = RowTableIter<'a>;
    type MutIter = RowTableMutIter<'a>;

    fn into_iter(self) -> RowTableIntoIter {
        unimplemented!()
    }

    fn iter(&self) -> RowTableIter<'a> {
        unimplemented!()
    }

    fn iter_mut(&mut self) -> RowTableMutIter<'a> {
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

impl <'a> TableSlice<'a> for RowTableSlice<'a> { }

//
// 3 types of Iterators for RowTable: into, reference, and mutable reference
//

/// Consuming `Iterator` for rows in the table.
pub struct RowTableIntoIter {
    columns: Rc<Vec<String>>,
    iter: std::vec::IntoIter<Vec<Value>>
}

impl Iterator for RowTableIntoIter {
    type Item=OwnedRow;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(row) = self.iter.next() {
            // self.columns.clone is cheap because it's an Rc
            Some(OwnedRow{ columns: self.columns.clone(), row })
        } else {
            None
        }
    }
}

/// Reference `Iterator` for rows in a table.
pub struct RowTableIter<'a> {
    columns: &'a Vec<String>,
    iter: std::slice::Iter<'a, Vec<Value>>
}

impl <'a> Iterator for RowTableIter<'a> {
    type Item=BorrowedRow<'a, &'a Vec<Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(row) = self.iter.next() {
            Some(BorrowedRow{ columns: &self.columns, row })
        } else {
            None
        }
    }

}

/// Mutable reference `Iterator` for rows in a table.
pub struct RowTableMutIter<'a> {
    columns: &'a Vec<String>,
    iter: std::slice::IterMut<'a, Vec<Value>>
}

impl <'a> Iterator for RowTableMutIter<'a> {
    type Item=BorrowedRow<'a, &'a mut Vec<Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(row) = self.iter.next() {
            Some(BorrowedRow{ columns: &self.columns, row })
        } else {
            None
        }
    }
}

impl IntoIterator for RowTable {
    type Item=OwnedRow;
    type IntoIter=RowTableIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        RowTableIntoIter{ columns: Rc::new(self.columns), iter: self.rows.into_iter() }
    }
}

impl <'a> IntoIterator for &'a RowTable {
    type Item=BorrowedRow<'a, &'a Vec<Value>>;
    type IntoIter=RowTableIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        RowTableIter{ columns: &self.columns, iter: self.rows.iter() }
    }
}

impl <'a> IntoIterator for &'a mut RowTable {
    type Item=BorrowedRow<'a, &'a mut Vec<Value>>;
    type IntoIter=RowTableMutIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        RowTableMutIter{ columns: &self.columns, iter: self.rows.iter_mut() }
    }
}

//
//#[cfg(test)]
//mod tests {
//    use std::path::Path;
//    use std::time::Instant;
//
//    use log::Level;
//    use chrono::Duration;
//
//    use crate::LOGGER_INIT;
//    use crate::row_table::{RowTable, RowTableSlice};
//    use crate::{Table, TableOperations};
//    use crate::value::Value;
//    use ordered_float::OrderedFloat;
//
//    #[test]
//    fn to_from_csv() {
//        LOGGER_INIT.call_once(|| simple_logger::init_with_level(Level::Debug).unwrap()); // this will panic on error
//        let columns = ["A", "B", "C", "D"];
//        let mut t1:RowTable = Table::new(&columns);
//
//        for i in 0..10 {
//            let mut row = (0..t1.width()).map(|v| Value::Integer((v+i) as i64)).collect::<Vec<_>>();
//            t1.append_row(row);
//        }
//
//        assert_eq!(10, t1.len());
//        assert_eq!(columns.len(), t1.width());
//
//        let path = Path::new("/tmp/test.csv");
//        t1.to_csv(path).expect("Error writing CSV"); // write it out
//
//        let t2 :RowTable = Table::from_csv(path).expect("Error reading CSV");
//
//        assert_eq!(10, t2.len());
//        assert_eq!(columns.len(), t2.width());
//    }
//
//    #[test]
//    fn slice_to_from_csv() {
//        LOGGER_INIT.call_once(|| simple_logger::init_with_level(Level::Debug).unwrap()); // this will panic on error
//        let columns = ["A", "B", "C", "D"];
//        let mut t1:RowTable = Table::new(&columns);
//
//        for i in 0..10 {
//            let mut row = (0..t1.width()).map(|v| Value::Integer((v+i%2) as i64)).collect::<Vec<_>>();
//            t1.append_row(row);
//        }
//
//        assert_eq!(10, t1.len());
//        assert_eq!(columns.len(), t1.width());
//
//        // get a slice for writing
//        let groups = t1.group_by("A").expect("Error group_by");
//
//        for (v, slice) in groups.clone() {
//            let path_str = format!("/tmp/test_slice_{}.csv", String::from(v));
//            let path = Path::new(&path_str);
//
//            slice.to_csv(path).expect("Error writing CSV");
//        }
//
//        for (v, slice) in groups {
//            let path_str = format!("/tmp/test_slice_{}.csv", String::from(v));
//            let path = Path::new(&path_str);
//
//            let t :RowTable = Table::from_csv(path).expect("Error writing CSV");
//
//            let s = t.find("A", v).expect("Error getting slice");
//
//            assert_eq!(5, s.len());
//            assert_eq!(columns.len(), s.width());
//        }
//    }
//
//    #[test]
//    fn new_append() {
//        LOGGER_INIT.call_once(|| simple_logger::init_with_level(Level::Debug).unwrap()); // this will panic on error
//
//        let mut t1 :RowTable = Table::new(&["A", "B"]);
//        let mut t2 :RowTable = Table::new(&["A", "B"]);
//
//        t1.append_row(vec![Value::new("1"), Value::new("2.3")]);
//        t1.append_row(vec![Value::new("2"), Value::new("hello")]);
//
//        assert_eq!(2, t1.iter().count());
//
//        t2.append(t1);
//        assert_eq!(2, t2.iter().count());
//    }
//
//    #[test]
//    fn find() {
//        LOGGER_INIT.call_once(|| simple_logger::init_with_level(Level::Debug).unwrap()); // this will panic on error
//
//        let mut t1 :RowTable = Table::new(&["A", "B"]);
//
//        t1.append_row(vec![Value::new("1"), Value::new("2.3")]);
//        t1.append_row(vec![Value::new("1"), Value::new("7.5")]);
//        t1.append_row(vec![Value::new("2"), Value::new("hello")]);
//
//        let ts = t1.find("A", &Value::Integer(1)).expect("Error finding 1");
//
//        ts.find("B", &Value::Float(OrderedFloat(2.3))).expect("Error finding 2.3");
//    }
//}
