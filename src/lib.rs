//! large_table is an memory-mapping of data, modeled after [Pandas](https://pandas.pydata.org/) for Python.
extern crate log;

use std::str;
use std::io::{Error as IOError, ErrorKind};
use std::path::Path;
use std::collections::{HashMap, HashSet};
use std::cmp::Ordering;
use std::fs::OpenOptions;
use std::sync::{Arc, Mutex};

use bstr::ByteSlice;
use memmap::{Mmap};
use csv_core::{Reader as CsvCoreReader, ReadRecordResult};
use csv::{Reader};
use rayon::prelude::*;

mod value;
mod table_error;

// expose some of the underlying structures from other files
pub use crate::value::{Value, ValueType};
pub use crate::table_error::TableError;

// type ColumnOffsets = SmallVec<[(usize,usize); 32]>;
type ColumnOffsets = Vec<(usize,usize)>;

// this is all the immutable stuff about the table itself
#[derive(Debug)]
struct LargeTableInner {
    columns: Vec<String>,   // mapping of column names to row offsets
    mmap: Mmap,
    schema: Option<Vec<ValueType>>
}


pub struct LargeTable {
    inner: Arc<LargeTableInner>,
    rows: Vec<ColumnOffsets>,       // offset into the mmap/array of the start of each row
}

#[derive(Debug)]
pub struct Row {
    table: Arc<LargeTableInner>,
    col_offsets: ColumnOffsets,
}

impl Row {
    #[inline]
    pub fn get(&self, column :&str) -> Value {
        self.try_get(column).unwrap()
    }

    pub fn try_get(&self, column :&str) -> Result<Value, TableError> {
        match self.table.columns.iter().position(|c| c == column) {
            None => Err(TableError::new(format!("Could not find column: {}", column).as_str())),
            Some(pos) => self.try_at(pos)
        }
    }

    pub fn at(&self, index :usize) -> Value {
        self.try_at(index).unwrap()
    }

    pub fn try_at(&self, index :usize) -> Result<Value, TableError> {
/*
        // parse the row
        let mut reader = CsvCoreReader::new();
        let mut output = [0u8; 1024*1024];
        let mut ends = [0usize; 100];

        let (res, _read, _written, _num_ends) = reader.read_record(&self.table.mmap[self.col_offsets..], &mut output, &mut ends);

        if let ReadRecordResult::Record = res {
            let (s, e) = if index == 0 {
                (0, ends[0])
            } else {
                (ends[index-1], ends[index])
            };

            let val = unsafe { str::from_utf8_unchecked(&output[s..e]) };

            // check to see if we have a schema to use or not
            if let Some(value_types) = self.table.schema.as_ref() {
                Ok(Value::with_type(val, &value_types[index]))
            } else {
                Ok(Value::new(val))
            }
        } else {
            let err_str = format!("Could not parse column {}: {:?}", index, res);
            Err(TableError::new(err_str.as_str()))
        }
 */

        let val = unsafe { str::from_utf8_unchecked(&self.table.mmap[self.col_offsets[index].0..self.col_offsets[index].1]) };

        // check to see if we have a schema to use or not
        if let Some(value_types) = self.table.schema.as_ref() {
            Ok(Value::with_type(val, &value_types[index]))
        } else {
            Ok(Value::new(val))
        }
    }

    #[inline]
    pub fn width(&self) -> usize {
        self.table.columns.len()
    }

    #[inline]
    pub fn columns(&self) -> Vec<String> {
        self.table.columns.clone()
    }
}

impl Display for Row {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for col in self.columns() {
            if let Err(e) = write!(f, "{}: {}\t", col, self.get(&col)) {
                return Err(e)
            }
        }

        Ok( () )
    }
}

/// `Iterator` for rows in a table.
pub struct LargeTableIter {
    table: LargeTable,
    cur_pos: usize
}

impl Iterator for LargeTableIter {
    type Item=Row;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_pos >= self.table.rows.len() {
            None
        } else {
            let ret = Some(Row {
                table: self.table.inner.clone(),
                col_offsets: self.table.rows[self.cur_pos].clone(), // TODO: use reference
            });

            self.cur_pos += 1;

            ret
        }
    }
}

/// The main interface into the large_table library
impl LargeTable {
    fn load<P: AsRef<Path>>(file :P, schema :Option<Vec<ValueType>>) -> Result<Self, IOError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file)?;

        let mmap = unsafe { Mmap::map(&file)? };
        let mut columns = Vec::new();

        let mut reader = CsvCoreReader::new();
        let mut rows = Vec::new();
        let mut pos = 0;
        let mut output = [0u8; 1024*1024];

        loop {
            let mut ends = [0usize; 100];

            let (res, read, written, num_ends) = reader.read_record(&mmap[pos..], &mut output, &mut ends);

            // println!("POS: {} RES: {:?} READ: {} WRITTEN: {} NUM_ENDS: {}", pos, res, read, written, num_ends);
            // println!("OUTPUT: {:?} {}", str::from_utf8(&output[0..20]).unwrap(), ends[0]);

            if let ReadRecordResult::End = res {
                break;
            }

            if let ReadRecordResult::Record = res {
                let mut row = Vec::with_capacity(ends.len() + 1);
                // let mut row :SmallVec<[(usize,usize); 32]> = SmallVec::with_capacity(ends.len() + 1);

                // go through the ends, making start/end pairs:
                for i in 0..num_ends {

                    if i == 0 {
                        // println!("0: {} Searching for {} in {}", ends[i], str::from_utf8(&output[0..1]).unwrap(), str::from_utf8(&mmap[pos..(pos+20)]).unwrap());

                        let start = mmap[pos..].find_byte(output[0]).expect("Could not find character in mmap, but was in output");

                        row.push( (pos+start, pos+start+ends[i]) );
                    } else {
                        // println!("{} Searching for '{}' in {}", (ends[i] - ends[i-1]), str::from_utf8(&output[ends[i-1]..(ends[i-1]+1)]).unwrap(), str::from_utf8(&mmap[pos..(pos+20)]).unwrap());
                        let start = mmap[pos..].find_byte(output[ends[i-1]]).expect("Could not find character in mmap, but was in output");

                        row.push( (pos+start, pos+start+(ends[i]-ends[i-1])) );
                    }

                    // println!("POS: {} -> {}", pos, row.last().unwrap().1);
                    pos = row.last().unwrap().1;
                }

                // print!("ROW: ");
                // for (s,e) in row.iter() {
                //     print!("{}|", str::from_utf8(&mmap[*s..*e]).unwrap());
                // }
                // println!();

                // the first row is the column header
                if columns.is_empty() {
                    for (s,e) in row.iter() {
                        columns.push(String::from_utf8(mmap[*s..*e].to_vec()).unwrap());
                    }
                } else {
                    rows.push(row);
                }
            } else {
                // println!("IN HERE: {:?}", res);
                pos += read;
            }
        }

        // try and conserve some memory here
        rows.shrink_to_fit();

        let inner = LargeTableInner {
            columns,
            mmap,
            schema
        };

        Ok(LargeTable {
            inner: Arc::new(inner),
            rows
        })
    }

    pub fn from_csv<P: AsRef<Path>>(file :P) -> Result<Self, IOError> {
        LargeTable::load(file, None)
    }

    pub fn from_csv_with_schema<P: AsRef<Path>>(file :P, schema :&[ValueType]) -> Result<Self, IOError> {
        LargeTable::load(file, Some(schema.to_vec()))
    }

    pub fn iter(&self) -> LargeTableIter {
        LargeTableIter {
            table: LargeTable { inner: self.inner.clone(), rows: self.rows.clone() },
            cur_pos: 0
        }
    }

    pub fn get(&self, index :usize) -> Result<Row, TableError> {
        if index >= self.len() {
            let err_str = format!("Index {} is beyond table length {}", index, self.len());
            return Err(TableError::new(err_str.as_str()));
        }

        Ok(Row {
            col_offsets: self.rows[index].clone(), // TODO: use reference
            table: self.inner.clone(),
        })
    }

    pub fn columns(&self) -> Vec<String> {
        self.inner.columns.clone()
    }

    /// Finds the position of a column in a table by name
    pub fn column_position(&self, column :&str) -> Result<usize, TableError> {
        if let Some(pos) = self.columns().iter().position(|c| c == column) {
            Ok(pos)
        } else {
            Err(TableError::new(format!("Column not found: {}", column).as_str()))
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    #[inline]
    pub fn width(&self) -> usize {
        self.columns().len()
    }

    pub fn group_by(&self, column :&str) -> Result<HashMap<Value, LargeTable>, TableError> {
        let index = self.column_position(column)?;
        let ret = Arc::new(Mutex::new(HashMap::new()));

        self.rows.par_iter().enumerate().for_each(|(i, offsets)| {
            let val = unsafe { str::from_utf8_unchecked(&self.inner.mmap[offsets[index].0..offsets[index].1]) };

            let mut ret_lock = ret.lock().unwrap();

            ret_lock.entry(val).or_insert(Vec::new()).push(self.rows[i].clone());
        });

        let ret_lock = ret.lock().unwrap();

        Ok(ret_lock.par_iter().map(|(v, r)| {
            let val = if let Some(schema) = self.inner.schema.as_ref() {
                Value::with_type(v, &schema[index])
            } else {
                Value::new(v)
            };

            (val, LargeTable {
                inner: self.inner.clone(),
                rows: r.clone()
            })
        }).collect::<HashMap<_, _>>())

        // let col_vals = self.unique(column)?;
        // let mut ret = HashMap::with_capacity(col_vals.len());
        //
        // for val in col_vals {
        //     ret.insert(val.clone(), self.filter(column, &val)?);
        // }
        //
        // Ok(ret)
    }

    /// Get a set of unique values for a given column
    pub fn unique(&self, column :&str) -> Result<HashSet<Value>, TableError>  {
        let index = self.column_position(column)?;

        // collect all the values as strings first
        let vals = self.rows.par_iter().map(|offsets| {
            unsafe { str::from_utf8_unchecked(&self.inner.mmap[offsets[index].0..offsets[index].1]) }
        }).collect::<HashSet<_>>();

        // then convert them to values
        Ok(vals.par_iter().map(|v| {
            if let Some(schema) = self.inner.schema.as_ref() {
                Value::with_type(v, &schema[index])
            } else {
                Value::new(v)
            }
        }).collect::<HashSet<_>>())
    }

    /// Returns a `LargeTable` with only those rows that match the value in that column
    pub fn filter(&self, column :&str, value :&Value) -> Result<LargeTable, TableError> {
        // get the position in the underlying table
        let pos = self.column_position(column)?;

        self.filter_by(|row| row.at(pos) == *value)
    }

    pub fn filter_by<P: Fn(&Row) -> bool + Sync + Send>(&self, predicate :P) -> Result<LargeTable, TableError> {
        let new_rows = self.iter().enumerate().par_bridge().filter_map(|(index, row)| {
            if predicate(&row) {
                Some(self.rows[index].clone()) // TODO: use reference
            } else {
                None
            }
        }).collect::<Vec<_>>();

        Ok(LargeTable {
            inner: self.inner.clone(),
            rows: new_rows
        })
    }

    /// Sorts the rows in the table, in an unstable way, in ascending order, by the columns provided, in the order they're provided.
    ///
    /// If the columns passed are `A`, `B`, `C`, then the rows will be sored by column `A` first, then `B`, then `C`.
    pub fn sort(&self, columns :&[&str]) -> Result<LargeTable, TableError> {
        // make sure columns were passed
        if columns.is_empty() {
            return Err(TableError::new("No columns passed to sort"));
        }

        let mut indices = Vec::new();

        // convert from columns to indexes
        for col in columns {
            indices.push(self.column_position(col)?);
        }

        Ok(self.sort_by(|a, b| {
            let mut ret = Ordering::Equal;

            for index in &indices {
                ret = a.at(*index).cmp(&b.at(*index));

                if ret != Ordering::Equal {
                    return ret;
                }
            }

            ret
        }))
    }

    /// Sorts the rows in the table, in an unstable way, in ascending order using the `compare` function to compare values.
    pub fn sort_by<F: Fn(&Row, &Row) -> Ordering + Send + Sync>(&self, compare :F) -> LargeTable {
        let mut new_rows = self.rows.clone();

        // sort the rows using the comparator
        new_rows.sort_unstable_by(|offset1, offset2| {
            let r1 = Row { col_offsets: offset1.clone(), table: self.inner.clone() };
            let r2 = Row { col_offsets: offset2.clone(), table: self.inner.clone() };

            compare(&r1, &r2)
        });

        LargeTable {
            inner: self.inner.clone(),
            rows: new_rows
        }
    }
}


// these are for tests
#[cfg(test)] extern crate simple_logger;
#[cfg(test)] extern crate rand;
#[cfg(test)] use std::sync::{Once};
use std::fmt::{Display, Formatter};
use std::fmt;
use smallvec::SmallVec;

#[cfg(test)] static LOGGER_INIT: Once = Once::new();

