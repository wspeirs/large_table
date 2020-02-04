use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::io::{Error as IOError, ErrorKind};
use std::ops::Index;
use std::collections::hash_map::RandomState;
use std::iter::Map;
use std::rc::Rc;
use std::sync::Arc;
use std::cell::RefCell;


use csv::{Reader, StringRecord, ByteRecord, ReaderBuilder, Trim};
use rayon::prelude::*;

use crate::{Table, TableOperations, TableSlice, TableError, OwnedRow, RefRow, MutRefRow};
use crate::value::Value;
use crate::row::Row;
use chrono::format::Item::OwnedSpace;
use std::cmp::Ordering;

/// A table with row-oriented data
#[derive(Debug, Clone)]
struct RowTableInner {
    columns: Vec<String>,
    rows: Vec<Vec<Value>>
}

#[derive(Debug, Clone)]
pub struct RowTable(Rc<RefCell<RowTableInner>>); //https://play.rust-lang.org/?version=stable&mode=debug&edition=2018&gist=85a1c46e9e455bba144e442cdf0e57b3

impl <'a> Table<'a> for RowTable {
    type MutIter = RowTableMutIter<'a>;

    /// Create a blank RowTable
    fn new(columns :&[&str]) -> Self {
        RowTable(Rc::new(RefCell::new(RowTableInner {
            columns: columns.into_iter().map(|s| String::from(*s)).collect::<Vec<_>>(),
            rows: Vec::new()
        })))
    }

    fn iter_mut(&'a mut self) -> Self::MutIter {
        self.into_iter()
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

    fn append_row<'b, R: 'b>(&mut self, row: R) -> Result<(), TableError>  where R: Row<'b> {
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

impl <'a> TableOperations<'a> for RowTable {
    type TableSliceType = RowTableSlice;
    type IntoIter = RowTableIntoIter;
    type Iter = RowTableIter<'a>;

//    fn into_iter(self) -> RowTableIntoIter {
//        let inner = self.clone().0.into_inner();
//
//        RowTableIntoIter{ columns: Arc::new(inner.columns), iter: inner.rows.into_iter() }
//    }

    fn into_iter(self) -> RowTableIntoIter {
        unimplemented!()
    }

    fn iter(&'a self) -> RowTableIter<'a> {
        self.into_iter()
    }

    #[inline]
    fn columns(&self) -> Vec<String> {
        self.0.borrow().columns.clone()
    }

    fn group_by(&'a self, column: &str) -> Result<HashMap<Value, RowTableSlice>, TableError> {
        // get the position in the row we're concerned with
        let pos = self.column_position(column)?;

        let mut ret = HashMap::new();
        let empty_slice = RowTableSlice {
            columns: self.0.borrow().columns.clone(),
            rows: Vec::new(),
            table: self.0.clone()
        };

        // go through each row, and add them to our result
        for (i, row) in self.0.borrow().rows.iter().enumerate() {
            // get the slice, or create a new one
            let slice = ret.entry(row[pos].clone()).or_insert(empty_slice.clone());

            // insert this row
            slice.rows.push(i);
        }

        Ok(ret)
    }

    fn find_by<P: FnMut(RefRow) -> bool>(&'a self, mut predicate :P) -> Result<RowTableSlice, TableError> {
        let mut slice_rows = Vec::new();

        for (i, row) in self.iter().enumerate() {
            if predicate(row) {
                slice_rows.push(i);
            }
        }

        Ok(RowTableSlice {
            columns: self.0.borrow().columns.clone(),
            rows: slice_rows,
            table: self.0.clone()
        })
    }

    fn sort_by<F: FnMut(RefRow, RefRow) -> Ordering>(&mut self, mut compare: F) -> Result<(), TableError> {
        let columns = self.0.borrow().columns.clone();

        Ok(self.0.borrow_mut().rows.sort_unstable_by(|a, b| {
            let a_row = RefRow { columns: &columns, row: a };
            let b_row = RefRow { columns: &columns, row: b };

            compare(a_row, b_row)
        }))
    }

    fn stable_sort_by<F: FnMut(RefRow, RefRow) -> Ordering>(&mut self, mut compare: F) -> Result<(), TableError> {
        let columns = self.0.borrow().columns.clone();

        Ok(self.0.borrow_mut().rows.sort_by(|a, b| {
            let a_row = RefRow { columns: &columns, row: a };
            let b_row = RefRow { columns: &columns, row: b };

            compare(a_row, b_row)
        }))
    }

    fn split_rows_at(&'a self, mid: usize) -> Result<(Self::TableSliceType, Self::TableSliceType), TableError> {
        if mid >= self.0.borrow().rows.len() {
            let err_str = format!("Midpoint too large: {} >= {}", mid, self.0.borrow().rows.len());
            return Err(TableError::new(err_str.as_str()));
        }

        Ok( (
            RowTableSlice { columns: self.0.borrow().columns.clone(), rows: (0..mid).collect::<Vec<_>>(), table: self.0.clone()},
            RowTableSlice { columns: self.0.borrow().columns.clone(), rows: (mid..self.0.borrow().rows.len()).collect::<Vec<_>>(), table: self.0.clone()}
            )
        )
    }
}

//
// 3 types of Iterators for RowTable: into, reference, and mutable reference
//

/// Consuming `Iterator` for rows in the table.
pub struct RowTableIntoIter {
    columns: Arc<Vec<String>>,
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
    type Item= RefRow<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(row) = self.iter.next() {
            Some(RefRow { columns: &self.columns, row })
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
    type Item= MutRefRow<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(row) = self.iter.next() {
            Some(MutRefRow { columns: &self.columns, row })
        } else {
            None
        }
    }
}

impl IntoIterator for RowTable {
    type Item=OwnedRow;
    type IntoIter=RowTableIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        let columns = self.0.borrow().columns.clone();

        RowTableIntoIter{ columns: Arc::new(columns), iter: self.0.borrow().rows.into_iter() }
    }
}

impl <'a> IntoIterator for &'a RowTable {
    type Item= RefRow<'a>;
    type IntoIter=RowTableIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        RowTableIter{ columns: &self.0.borrow().columns, iter: self.0.borrow().rows.iter() }
    }
}

impl <'a> IntoIterator for &'a mut RowTable {
    type Item= MutRefRow<'a>;
    type IntoIter=RowTableMutIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        RowTableMutIter{ columns: &self.0.borrow().columns, iter: self.0.borrow().rows.iter_mut() }
    }
}


#[derive(Debug, Clone)]
pub struct RowTableSlice {
    columns: Vec<String>,               // the columns in this slice's view
    rows: Vec<usize>,                   // index of the corresponding row in the Table
    table: Rc<RefCell<RowTableInner>>   // reference to the underlying table
}

impl <'a> TableOperations<'a> for RowTableSlice {
    type TableSliceType = RowTableSlice;
    type IntoIter = RowTableSliceIntoIter;
    type Iter = RowTableSliceIter<'a>;

    fn into_iter(self) -> RowTableSliceIntoIter {
        let cols = Arc::new(self.columns.clone());

        RowTableSliceIntoIter{ slice: self, columns: cols, cur_pos: 0 }
    }

    fn iter(&'a self) -> RowTableSliceIter<'a>{
        self.into_iter()
    }


    #[inline]
    fn columns(&self) -> Vec<String> {
        self.columns.clone()
    }

    fn group_by(&'a self, column: &str) -> Result<HashMap<Value, RowTableSlice>, TableError> {
        unimplemented!();
    }

    fn find_by<P: FnMut(RefRow) -> bool>(&'a self, mut predicate: P) -> Result<RowTableSlice, TableError> {
        let mut slice_rows = Vec::new();

        for &row_index in self.rows.iter() {
            let row = RefRow{ columns: &self.columns, row: &self.table.borrow().rows[row_index]};

            // run the predicate against the row
            if predicate(row) {
                slice_rows.push(row_index);
            }
        }

        Ok(RowTableSlice {
            columns: self.columns.clone(),
            rows: slice_rows,
            table: self.table.clone()
        })
    }

    fn sort_by<F: FnMut(RefRow, RefRow) -> Ordering>(&mut self, mut compare: F) -> Result<(), TableError> {
        let columns = self.columns.clone();
        let table = self.table.clone();

        Ok(self.rows.sort_unstable_by(|a, b| {
            let a_row = RefRow { columns: &columns, row: &table.borrow().rows[*a].clone() };
            let b_row = RefRow { columns: &columns, row: &table.borrow().rows[*b].clone() };

            compare(a_row, b_row)
        }))
    }

    fn stable_sort_by<F: FnMut(RefRow, RefRow) -> Ordering>(&mut self, compare: F) -> Result<(), TableError> {
        let columns = self.columns.clone();
        let table = self.table.clone();

        Ok(self.rows.sort_by(|a, b| {
            let a_row = RefRow { columns: &columns, row: &table.borrow().rows[*a] };
            let b_row = RefRow { columns: &columns, row: &table.borrow().rows[*b] };

            compare(a_row, b_row)
        }))
    }

    fn split_rows_at(&self, mid: usize) -> Result<(Self::TableSliceType, Self::TableSliceType), TableError> {
        if mid >= self.rows.len() {
            let err_str = format!("Midpoint too large: {} >= {}", mid, self.rows.len());
            return Err(TableError::new(err_str.as_str()));
        }

        Ok( (
            RowTableSlice { columns: self.columns.clone(), rows: (0..mid).collect::<Vec<_>>(), table: self.table.clone() },
            RowTableSlice { columns: self.columns.clone(), rows: (mid..self.rows.len()).collect::<Vec<_>>(), table: self.table.clone() }
            )
        )
    }
}

impl <'a> TableSlice<'a> for RowTableSlice { }

//
// 3 types of Iterators for RowTableSlice: into, reference, and mutable reference
//

/// Consuming `Iterator` for rows in the table.
pub struct RowTableSliceIntoIter {
    slice: RowTableSlice,
    columns: Arc<Vec<String>>,
    cur_pos: usize
}

impl Iterator for RowTableSliceIntoIter {
    type Item=OwnedRow;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_pos >= self.slice.rows.len() {
            None
        } else {
            let row_index = self.slice.rows[self.cur_pos];
            let row_vec = self.slice.table.borrow().rows[row_index].clone();
            self.cur_pos += 1;

            Some(OwnedRow{ columns: self.columns.clone(), row: row_vec})
        }
    }
}

/// Reference `Iterator` for rows in a table.
pub struct RowTableSliceIter<'a> {
    slice: &'a RowTableSlice,
    columns: &'a Vec<String>,
    cur_pos: usize
}

impl <'a> Iterator for RowTableSliceIter<'a>{
    type Item= RefRow<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_pos >= self.slice.rows.len() {
            None
        } else {
            let row_index = self.slice.rows[self.cur_pos];
            let row_vec = self.slice.table.borrow().rows[row_index].as_ref();
            self.cur_pos += 1;

            Some(RefRow{ columns: self.columns, row: row_vec})
        }
    }
}

//impl <'a> IntoIterator for RowTableSlice {
//    type Item=OwnedRow;
//    type IntoIter=RowTableSliceIntoIter<'a>;
//
//    fn into_iter(self) -> Self::IntoIter {
//        let cols = Rc::new(self.columns.clone());
//
//        RowTableSliceIntoIter{ slice: self, columns: cols, cur_pos: 0 }
//    }
//}

impl <'a> IntoIterator for &'a RowTableSlice {
    type Item= RefRow<'a>;
    type IntoIter=RowTableSliceIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        RowTableSliceIter{ slice: &self, columns: &self.columns, cur_pos: 0 }
    }
}

//impl <'a> IntoIterator for &'a mut RowTableSlice {
//    type Item= MutRefRow<'a>;
//    type IntoIter=RowTableMutIter<'a>;
//
//    fn into_iter(self) -> Self::IntoIter {
//        RowTableSliceMutIter{ columns: &self.columns, iter: self.rows.iter_mut() }
//    }
//}

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
