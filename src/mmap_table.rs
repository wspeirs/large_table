use std::path::Path;
use std::collections::hash_map::RandomState;
use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::{Error as IOError, ErrorKind, Cursor};
use std::sync::{Mutex, Arc};
use std::cmp::Ordering;

use memmap::{MmapMut, MmapOptions};
use csv_core::{Reader as CsvCoreReader, ReadRecordResult};
use csv::Reader;
use rayon::prelude::*;

use crate::{Table, TableOperations, Value, TableError, Row, RowSlice, TableSlice, ValueType};

pub struct MMapTableInner {
    columns: Vec<String>,
    mmap: MmapMut,
    rows: Vec<usize>,
    schema: Option<Vec<ValueType>>
}

pub struct MMapTable (Arc<Mutex<MMapTableInner>>);

impl MMapTable {
    pub fn from_csv<P: AsRef<Path>>(file :P) -> Result<Self, IOError> {
        let table_inner = MMapTable::map_file(file)?;

        Ok(MMapTable (Arc::new(Mutex::new(table_inner))))
    }

    pub fn from_csv_with_schema<P: AsRef<Path>>(file :P, schema :&[ValueType]) -> Result<Self, IOError> {
        let mut table_inner = MMapTable::map_file(file)?;

        table_inner.schema = Some(schema.to_vec());

        Ok(MMapTable (Arc::new(Mutex::new(table_inner))))
    }

    // Maps the file and returns the struct... used for the create functions
    fn map_file<P: AsRef<Path>>(file :P) -> Result<MMapTableInner, IOError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file)?;

        let mut mmap = unsafe { MmapMut::map_mut(&file)? };

        let mut reader = CsvCoreReader::new();
        let mut rows = vec![0usize];
        let mut pos = 0;
        let mut output = [0u8; 1024*1024];

        loop {
            let mut ends = [0usize; 100];

            let (res, read, written, num_ends) = reader.read_record(&mmap[pos..], &mut output, &mut ends);

//            println!("POS: {} RES: {:?} READ: {} WRITTEN: {} NUM_ENDS: {}", pos, res, read, written, num_ends);

            if let ReadRecordResult::End = res {
                break;
            }

            pos += read;

            if let ReadRecordResult::Record = res {
                rows.push(pos);
            }
        }

        rows.pop();
        rows.shrink_to_fit();

//        println!("ROWS: {}", rows.len());

        let mut header_buffer = vec![0u8; rows[1]];

        header_buffer.copy_from_slice(&mmap[0..rows[1]]);

        let mut header_reader = Reader::from_reader(header_buffer.as_slice());

        let columns = header_reader.headers()?.iter().map(|h| String::from(h)).collect::<Vec<_>>();

        if columns.iter().collect::<HashSet<_>>().len() != columns.len() {
            return Err(IOError::new(ErrorKind::InvalidData, "Duplicate columns detected in the file"));
        }

        Ok(MMapTableInner {
            columns,
            mmap,
            rows,
            schema: None
        })
    }
}

impl Table for MMapTable {
    fn update_by<F: FnMut(&mut Self::RowType)>(&mut self, update: F) {
        unimplemented!()
    }

    fn append_row<R>(&mut self, row: R) -> Result<(), TableError> where R: Row {
        unimplemented!("You can only modify the contents of memory-mapped table, not change it's size")
    }

    fn add_column_with<F: FnMut() -> Value>(&mut self, column_name: &str, f: F) -> Result<(), TableError> {
        unimplemented!("You can only modify the contents of memory-mapped table, not change it's size")
    }

    fn rename_column(&mut self, old_col :&str, new_col :&str) -> Result<(), TableError> {
        unimplemented!()
    }
}

impl TableOperations for MMapTable {
    type TableSliceType = MMapTableSlice;
    type RowType = RowSlice<MMapTableInner>;
    type Iter = MMapTableIter;

    fn iter(&self) -> Self::Iter {
        MMapTableIter {
            table: self.0.clone(),
            column_map: Arc::new(self.0.lock().unwrap().columns.iter().enumerate().map(|(i, s)| (s.clone(), i)).collect()),
            cur_pos: 0
        }
    }

    fn get(&self, index: usize) -> Result<Self::RowType, TableError> {
        if index >= self.len() {
            let err_str = format!("Index {} is beyond table length {}", index, self.len());
            return Err(TableError::new(err_str.as_str()));
        }

        Ok(RowSlice {
            column_map: Arc::new(self.0.lock().unwrap().columns.iter().enumerate().map(|(i, s)| (s.clone(), i)).collect()),
            table: self.0.clone(),
            row: index
        })
    }

    fn columns(&self) -> Vec<String> {
        let inner = self.0.lock().unwrap();

        inner.columns.clone()
    }

    fn group_by(&self, column: &str) -> Result<HashMap<Value, Self::TableSliceType, RandomState>, TableError> {
        unimplemented!()
    }

    fn filter_by<P: FnMut(&Self::RowType) -> bool>(&self, mut predicate: P) -> Result<Self::TableSliceType, TableError> {
        let mut slice_rows = Vec::new();

        // self.iter().enumerate().par_bridge().filter_map(|(i,r)| {
        //     if predicate(&r) {
        //         Some(i)
        //     } else {
        //         None
        //     }
        // });

        for (i, row) in self.iter().enumerate() {
            if predicate(&row) {
                slice_rows.push(i);
            }
        }

        Ok(MMapTableSlice {
            column_map: Arc::new(self.0.lock().unwrap().columns.iter().enumerate().map(|(i, s)| (s.clone(), i)).collect()),
            rows: Arc::new(slice_rows),
            table: self.0.clone()
        })
    }

    fn split_rows_at(&self, mid: usize) -> Result<(Self::TableSliceType, Self::TableSliceType), TableError> {
        unimplemented!()
    }
}

/// `Iterator` for rows in a table.
pub struct MMapTableIter {
    table: Arc<Mutex<MMapTableInner>>,
    column_map: Arc<Vec<(String, usize)>>,
    cur_pos: usize
}

impl Iterator for MMapTableIter {
    type Item=RowSlice<MMapTableInner>;

    fn next(&mut self) -> Option<Self::Item> {
         if self.cur_pos >= self.table.lock().unwrap().rows.len() {
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

impl Row for RowSlice<MMapTableInner> {
    fn try_get(&self, column: &str) -> Result<Value, TableError> {
        let pos = self.column_map.iter().position(|(c, i)| c == column);

        if pos.is_none() {
            let err_str = format!("Could not find column in RowSlice: {}", column);
            return Err(TableError::new(err_str.as_str()));
        }

        let pos = self.column_map[pos.unwrap()].1;

        // get the offset into the file
        let table = self.table.lock().unwrap();
        let offset = table.rows[self.row];

        // parse the row
        let mut reader = CsvCoreReader::new();
        let mut output = [0u8; 1024*1024];
        let mut ends = [0usize; 100];

        let (res, read, written, num_ends) = reader.read_record(&table.mmap[offset..], &mut output, &mut ends);

        if let ReadRecordResult::Record = res {
            let (s, e) = if pos == 0 {
                (0, ends[0])
            } else {
                (ends[pos-1], ends[pos])
            };

            let value = String::from_utf8(output[s..e].to_vec()).unwrap();

            // use the schema if we have it
            Ok(if let Some(schema) = self.schema {
                Value::with_type(value.as_str(), schema[pos])
            } else {
                Value::new(value.as_str())
            })
        } else {
            let err_str = format!("Could not parse column {}: {:?}", column, res);
            Err(TableError::new(err_str.as_str()))
        }
    }

    fn columns(&self) -> Vec<String> {
        self.column_map.iter().map(|(c,i)| c.clone()).collect()
    }
}

pub struct MMapTableSlice {
    column_map: Arc<Vec<(String, usize)>>,   // mapping of column names to row offsets
    rows: Arc<Vec<usize>>,                   // index of the corresponding row in the Table
    table: Arc<Mutex<MMapTableInner>>       // reference to the underlying table
}

impl TableOperations for MMapTableSlice {
    type TableSliceType = MMapTableSlice;
    type RowType = RowSlice<MMapTableInner>;
    type Iter = MMapTableSliceIter;

    fn iter(&self) -> Self::Iter {
        MMapTableSliceIter {
            column_map: self.column_map.clone(),
            rows: self.rows.clone(),
            table: self.table.clone(),
            cur_pos: 0
        }
    }

    fn get(&self, index: usize) -> Result<Self::RowType, TableError> {
        if index >= self.len() {
            let err_str = format!("Index {} is beyond table length {}", index, self.len());
            return Err(TableError::new(err_str.as_str()));
        }

        Ok(RowSlice {
            column_map: self.column_map.clone(),
            table: self.table.clone(),
            row: self.rows[index]
        })
    }

    fn columns(&self) -> Vec<String> {
        self.column_map.iter().map(|(c,i)| c.clone()).collect()
    }

    fn group_by(&self, column: &str) -> Result<HashMap<Value, Self::TableSliceType, RandomState>, TableError> {
        unimplemented!()
    }

    fn filter_by<P: FnMut(&Self::RowType) -> bool>(&self, mut predicate: P) -> Result<Self::TableSliceType, TableError> {
        let mut slice_rows = Vec::new();

        for &row_index in self.rows.iter() {
            let row = RowSlice { column_map: self.column_map.clone(), table: self.table.clone(), row: row_index };

            // run the predicate against the row
            if predicate(&row) {
                slice_rows.push(row_index);
            }
        }

        Ok(MMapTableSlice {
            column_map: self.column_map.clone(),
            table: self.table.clone(),
            rows: Arc::new(slice_rows),
        })
    }

    fn split_rows_at(&self, mid: usize) -> Result<(Self::TableSliceType, Self::TableSliceType), TableError> {
        unimplemented!()
    }
}

impl TableSlice for MMapTableSlice {
    fn rename_column(&self, old_col :&str, new_col :&str) -> Result<Self::TableSliceType, TableError> {
        unimplemented!()
    }

    fn sort_by<F: FnMut(Self::RowType, Self::RowType) -> Ordering>(&self, compare: F) -> Result<Self::TableSliceType, TableError> {
        unimplemented!()
    }
}

pub struct MMapTableSliceIter {
    column_map: Arc<Vec<(String, usize)>>,
    rows: Arc<Vec<usize>>,
    table: Arc<Mutex<MMapTableInner>>,
    cur_pos: usize
}

impl Iterator for MMapTableSliceIter {
    type Item=RowSlice<MMapTableInner>;

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
    use log::Level;
    use chrono::Duration;

    use std::time::Instant;

    use crate::LOGGER_INIT;

    use crate::TableOperations;
    use crate::mmap_table::MMapTable;

    #[test]
    fn new() {
        LOGGER_INIT.call_once(|| simple_logger::init_with_level(Level::Debug).unwrap()); // this will panic on error

        let start = Instant::now();
        let table = MMapTable::from_csv("/export/stock_stuff/199x_100_sample.csv").unwrap();
        let end = Instant::now();

        println!("COLS: {:?}", table.columns());

        println!("TIME: {}ms", (end-start).as_millis());
    }
}
