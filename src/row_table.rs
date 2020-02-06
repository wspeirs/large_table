use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::io::{Error as IOError, ErrorKind};
use std::ops::Index;
use std::collections::hash_map::RandomState;
use std::iter::Map;
use std::rc::Rc;
use std::sync::Arc;
use std::cell::RefCell;
use std::fmt::{Display, Formatter, Error as FmtError};


use csv::{Reader, StringRecord, ByteRecord, ReaderBuilder, Trim};
use rayon::prelude::*;

use crate::{Table, TableOperations, TableSlice, TableError};
use crate::value::Value;
use crate::row::{Row, RowSlice, ValueIterator};
use chrono::format::Item::OwnedSpace;
use std::cmp::Ordering;

/// A table with row-oriented data
#[derive(Debug, Clone)]
pub struct RowTableInner {
    columns: Vec<String>,
    rows: Vec<Vec<Value>>
}

//https://play.rust-lang.org/?version=stable&mode=debug&edition=2018&gist=85a1c46e9e455bba144e442cdf0e57b3 - Rc<RefCell<>> Playground
#[derive(Debug, Clone)]
pub struct RowTable(Rc<RefCell<RowTableInner>>);

impl Table for RowTable {
    /// Create a blank RowTable
    fn new(columns :&[&str]) -> Self {
        RowTable(Rc::new(RefCell::new(RowTableInner {
            columns: columns.into_iter().map(|s| String::from(*s)).collect::<Vec<_>>(),
            rows: Vec::new()
        })))
    }

    fn update_by<F: FnMut(&mut Self::RowType)>(&mut self, mut update: F) {
        for mut row in self.iter() {
            update(&mut row);
        }
    }

    /// Read in a CSV file, and construct a RowTable
    fn from_csv<P: AsRef<Path>>(path: P) -> Result<Self, IOError> {
//        let mut csv = ReaderBuilder::new().trim(Trim::All).from_path(path)?;
        let mut csv = Reader::from_path(path)?;

        // get the headers from the CSV file
        let columns = csv.headers()?.iter().map(|h| String::from(h)).collect::<Vec<_>>();

        if columns.iter().collect::<HashSet<_>>().len() != columns.len() {
            return Err(IOError::new(ErrorKind::InvalidData, "Duplicate columns detected in the file"));
        }

        let mut rows = Vec::new();
////        let mut record = ByteRecord::new();
        let mut record = StringRecord::new();
//
////        while csv.read_byte_record(&mut record).map_err(|e| IOError::new(ErrorKind::Other, e))? {
        while csv.read_record(&mut record).map_err(|e| IOError::new(ErrorKind::Other, e))? {
//            let row = record.iter().map(|s| Value::String(s.to_string())).collect::<Vec<_>>();
            let row = record.iter().map(|s| Value::new(s)).collect::<Vec<_>>();

            rows.push(row);
        }

        // shrink the vector down so we're not chewing up more memory than needed
        rows.shrink_to_fit();

        Ok(RowTable(Rc::new(RefCell::new(RowTableInner { columns, rows }))))
    }

    fn append_row<R>(&mut self, row: R) -> Result<(), TableError>  where R: Row {
        // make sure the rows are the same width
        if self.width() != row.width() {
            let err_str = format!("Row width doesn't match table width: {} != {}", row.width(), self.width());
            return Err(TableError::new(err_str.as_str()));
        }

        // convert to a Vec
        let row_vec = row.iter().cloned().collect::<Vec<_>>();

        Ok(self.0.borrow_mut().rows.push(row_vec))
    }

    fn add_column_with<F: FnMut() -> Value>(&mut self, column_name :&str, mut f :F) -> Result<(), TableError> {
        // make sure we're not duplicating column names
        if let Ok(_) = self.column_position(column_name) {
            let err_str = format!("Attempting to add duplicate column: {} already exists", column_name);
            return Err(TableError::new(err_str.as_str()));
        }

        // add the column name to our list of columns
        self.0.borrow_mut().columns.push(String::from(column_name));

        // add the default value for the column
        self.0.borrow_mut().rows.iter_mut().for_each(|row| row.push(f()));

        Ok( () )
    }
}

impl TableOperations for RowTable {
    type TableSliceType = RowTableSlice;
    type RowType = RowSlice<RowTableInner>;
    type Iter = RowTableIter;

    fn iter(&self) -> RowTableIter {
        RowTableIter {
            table: self.0.clone(),
            column_map: Rc::new(self.0.borrow().columns.iter().enumerate().map(|(i, s)| (s.clone(), i)).collect()),
            cur_pos: 0
        }
    }

    #[inline]
    fn columns(&self) -> Vec<String> {
        self.0.borrow().columns.clone()
    }

    fn group_by(&self, column: &str) -> Result<HashMap<Value, RowTableSlice>, TableError> {
        // get the position in the row we're concerned with
        let pos = self.column_position(column)?;

        let mut row_map = HashMap::new();

        // go through each row, and add them to our result
        for (i, row) in self.0.borrow().rows.iter().enumerate() {
            // get the slice, or create a new one
            let slice = row_map.entry(row[pos].clone()).or_insert(Vec::new());

            // insert this row
            slice.push(i);
        }

        let column_map :Rc<HashMap<String, usize>> = Rc::new(self.0.borrow().columns.iter().enumerate().map(|(i, s)| (s.clone(), i)).collect());

        Ok(row_map.into_iter().map(|(k, v)| (k, RowTableSlice {
            column_map: column_map.clone(),
            rows: Rc::new(v),
            table: self.0.clone()
        })).collect())
    }

    fn find_by<P: FnMut(&RowSlice<RowTableInner>) -> bool>(&self, mut predicate :P) -> Result<RowTableSlice, TableError> {
        let mut slice_rows = Vec::new();

        for (i, row) in self.iter().enumerate() {
            if predicate(&row) {
                slice_rows.push(i);
            }
        }

        Ok(RowTableSlice {
            column_map: Rc::new(self.0.borrow().columns.iter().enumerate().map(|(i, s)| (s.clone(), i)).collect()),
            rows: Rc::new(slice_rows),
            table: self.0.clone()
        })
    }

//    fn sort_by<F: FnMut(Self::RowType, Self::RowType) -> Ordering>(&self, mut compare: F) -> Result<RowTableSlice, TableError> {
//        let column_map :Rc<HashMap<String, usize>> = Rc::new(self.0.borrow().columns.iter().enumerate().map(|(i,s)| (s.clone(), i)).collect());
//
//        let slice = RowTableSlice {
//            column_map,
//            rows: Rc::new((0..self.len()).collect()),
//            table: self.0.clone()
//        };
//
//        slice.sort_by(compare)
//    }
//
//    fn stable_sort_by<F: FnMut(Self::RowType, Self::RowType) -> Ordering>(&self, mut compare: F) -> Result<Self::TableSliceType, TableError> {
//        let column_map :Rc<HashMap<String, usize>> = Rc::new(self.0.borrow().columns.iter().enumerate().map(|(i,s)| (s.clone(), i)).collect());
//
//        let slice = RowTableSlice {
//            column_map,
//            rows: Rc::new((0..self.len()).collect()),
//            table: self.0.clone()
//        };
//
//        slice.stable_sort_by(compare)
//    }

    fn split_rows_at(&self, mid: usize) -> Result<(Self::TableSliceType, Self::TableSliceType), TableError> {
        if mid >= self.0.borrow().rows.len() {
            let err_str = format!("Midpoint too large: {} >= {}", mid, self.0.borrow().rows.len());
            return Err(TableError::new(err_str.as_str()));
        }

        Ok( (
            RowTableSlice {
                column_map: Rc::new(self.0.borrow().columns.iter().enumerate().map(|(i, s)| (s.clone(), i)).collect()),
                rows: Rc::new((0..mid).collect::<Vec<_>>()),
                table: self.0.clone()
            },
            RowTableSlice {
                column_map: Rc::new(self.0.borrow().columns.iter().enumerate().map(|(i, s)| (s.clone(), i)).collect()),
                rows: Rc::new((mid..self.0.borrow().rows.len()).collect::<Vec<_>>()),
                table: self.0.clone()
            }
            )
        )
    }
}


impl Row for RowSlice<RowTableInner> {
    fn get(&self, column: &str) -> Result<Value, TableError> {
        let pos = self.column_map.get(column);

        if pos.is_none() {
            let err_str = format!("Could not find column in RowSlice: {}", column);
            return Err(TableError::new(err_str.as_str()));
        }

        let row = &self.table.borrow().rows[self.row];

        Ok(row[*pos.unwrap()].clone())
    }

    fn columns(&self) -> Vec<String> {
        self.column_map.keys().cloned().collect()
    }

    fn iter(&self) -> ValueIterator {
        unimplemented!()
//        ValueIterator { iter: self.table.borrow().rows[self.row].iter() }
    }
}

impl Display for RowSlice<RowTableInner> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        // TODO: Fix this
        write!(f, "{:?}", self.table.borrow().rows[self.row])
    }
}


// Iterator for RowTable
//https://play.rust-lang.org/?version=stable&mode=debug&edition=2018&gist=856142d55cbca5cfda7bc97a744a0c4e - Iterator/Row Playground

/// `Iterator` for rows in a table.
pub struct RowTableIter {
    table: Rc<RefCell<RowTableInner>>,
    column_map: Rc<HashMap<String, usize>>,
    cur_pos: usize
}

impl Iterator for RowTableIter {
    type Item=RowSlice<RowTableInner>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_pos >= self.table.borrow().rows.len() {
            None
        } else {
            self.cur_pos += 1;
            Some(RowSlice {
                table: self.table.clone(),
                column_map: self.column_map.clone(),
                row: self.cur_pos-1
            })
        }
    }
}

//impl IntoIterator for RowTable {
//    type Item=OwnedRow;
//    type IntoIter=RowTableIntoIter;
//
//    fn into_iter(self) -> Self::IntoIter {
//        let columns = self.0.borrow().columns.clone();
//
//        RowTableIntoIter{ columns: Arc::new(columns), iter: self.0.borrow().rows.into_iter() }
//    }
//}

#[derive(Clone)]
pub struct RowTableSlice {
    column_map: Rc<HashMap<String, usize>>, // mapping of column names to row offsets
    rows: Rc<Vec<usize>>,                   // index of the corresponding row in the Table
    table: Rc<RefCell<RowTableInner>>       // reference to the underlying table
}

impl Display for RowTableSlice {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        for row in self.rows.iter() {
            writeln!(f, "{:?}", self.table.borrow().rows[*row]);
        }

        Ok( () )
    }
}


impl TableOperations for RowTableSlice {
    type TableSliceType = RowTableSlice;
    type RowType = RowSlice<RowTableInner>;
    type Iter = RowTableSliceIter;

    fn iter(&self) -> RowTableSliceIter{
        RowTableSliceIter {
            column_map: self.column_map.clone(),
            rows: self.rows.clone(),
            table: self.table.clone(),
            cur_pos: 0
        }
    }

    #[inline]
    fn columns(&self) -> Vec<String> {
        self.column_map.keys().cloned().collect()
    }

    fn group_by(&self, column: &str) -> Result<HashMap<Value, RowTableSlice>, TableError> {
        unimplemented!();
    }

    fn find_by<P: FnMut(&RowSlice<RowTableInner>) -> bool>(&self, mut predicate: P) -> Result<RowTableSlice, TableError> {
        let mut slice_rows = Vec::new();

        for &row_index in self.rows.iter() {
            let row = RowSlice { column_map: self.column_map.clone(), table: self.table.clone(), row: row_index };

            // run the predicate against the row
            if predicate(&row) {
                slice_rows.push(row_index);
            }
        }

        Ok(RowTableSlice {
            column_map: self.column_map.clone(),
            table: self.table.clone(),
            rows: Rc::new(slice_rows),
        })
    }

    fn split_rows_at(&self, mid: usize) -> Result<(Self::TableSliceType, Self::TableSliceType), TableError> {
        if mid >= self.rows.len() {
            let err_str = format!("Midpoint too large: {} >= {}", mid, self.rows.len());
            return Err(TableError::new(err_str.as_str()));
        }

        Ok( (
            RowTableSlice { column_map: self.column_map.clone(), rows: Rc::new((0..mid).collect()), table: self.table.clone() },
            RowTableSlice { column_map: self.column_map.clone(), rows: Rc::new((mid..self.rows.len()).collect()), table: self.table.clone() }
            )
        )
    }
}

impl TableSlice for RowTableSlice {
    fn sort_by<F: FnMut(Self::RowType, Self::RowType) -> Ordering>(&self, mut compare: F) -> Result<Self::TableSliceType, TableError> {
        let mut rows = self.rows.iter().cloned().collect::<Vec<_>>();

        rows.sort_unstable_by(|&a, &b| {
            let a_row = RowSlice { column_map: self.column_map.clone(), table: self.table.clone(), row: a };
            let b_row = RowSlice { column_map: self.column_map.clone(), table: self.table.clone(), row: b };

            compare(a_row, b_row)
        });

        Ok(RowTableSlice {
            column_map: self.column_map.clone(),
            rows: Rc::new(rows),
            table: self.table.clone()
        })
    }

//    fn stable_sort_by<F: FnMut(Self::RowType, Self::RowType) -> Ordering>(&self, compare: F) -> Result<Self::TableSliceType, TableError> {
//        unimplemented!()
//        let columns = self.columns.clone();
//        let table = self.table.clone();
//
//        Ok(self.rows.sort_by(|a, b| {
//            let a_row = RowSlice<RowTableInner> { columns: &columns, row: &table.borrow().rows[*a] };
//            let b_row = RowSlice<RowTableInner> { columns: &columns, row: &table.borrow().rows[*b] };
//
//            compare(a_row, b_row)
//        }))
//    }

}

/// Reference `Iterator` for rows in a table.
pub struct RowTableSliceIter {
    column_map: Rc<HashMap<String, usize>>,
    rows: Rc<Vec<usize>>,
    table: Rc<RefCell<RowTableInner>>,
    cur_pos: usize
}

impl Iterator for RowTableSliceIter {
    type Item=RowSlice<RowTableInner>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_pos >= self.rows.len() {
            None
        } else {
            self.cur_pos += 1;
            let row_index = self.rows[self.cur_pos-1];

            Some(RowSlice { column_map: self.column_map.clone(), table: self.table.clone(), row: row_index})
        }
    }
}


#[cfg(test)]
mod tests {
    use crate::{RowTable, TableOperations, Table, Row, Value};

    #[test]
    fn to_from_csv() {
        let mut table :RowTable = RowTable::new(&["B"]);

        table.find_by(|r| { r.get("B"); true });
//        table.find_by(|r| { r.set("B", Value::Integer(7)); true });
        table.update_by(|r| { r.set("B", Value::Integer(7));} );
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
