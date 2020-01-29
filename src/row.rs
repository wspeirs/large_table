use std::rc::Rc;

use crate::value::Value;
use crate::table_error::TableError;
use crate::Table;


// playground: https://play.rust-lang.org/?version=stable&mode=debug&edition=2018&gist=fbac8bab1dc26bc89edf35e6d62b3170

// playground for Row & Iterators: https://play.rust-lang.org/?version=stable&mode=debug&edition=2018&gist=5b1ead8cdf0cbaac2941ec9e15a942d5


/// A owned row for a `Table` or `TableSlice`.
#[derive(Debug)]
pub struct OwnedRow {
    pub(crate) columns: Rc<Vec<String>>,
    pub(crate) row: Vec<Value>
}

/// A row with ref values for a `Table` or `TableSlice`.
#[derive(Debug)]
pub struct RefRow<'a> {
    pub(crate) columns: &'a Vec<String>,
    pub(crate) row: &'a Vec<Value>
}

/// A row with mut ref values for a `Table` or `TableSlice`.
#[derive(Debug)]
pub struct MutRefRow<'a> {
    pub(crate) columns: &'a Vec<String>,
    pub(crate) row: &'a mut Vec<Value>
}

/// Operations that you can perform on a Row
pub trait Row<'a> {
    fn get(&'a self, column :&str) -> Result<Value, TableError> {
        let pos = self.columns().iter().position(|c| c == column);

        match pos {
            Some(p) => self.at(p),
            None => {
                let err_str = format!("Could not find column {} in row", column);
                Err(TableError::new(err_str.as_str()))
            }
        }
    }

    fn at(&self, pos :usize) -> Result<Value, TableError>;

    #[inline]
    fn width(&'a self) -> usize {
        self.columns().len()
    }

    fn columns(&'a self) -> &'a Vec<String>;
    fn iter(&self) -> ValueIterator;
}

impl <'a> Row<'a> for OwnedRow {
    /// Return the contents of a cell by column index.
    fn at(&self, pos :usize) -> Result<Value, TableError> {
        if pos >= self.row.len() {
            let err_str = format!("Index {} is greater than row width {}", pos, self.row.len());
            Err(TableError::new(err_str.as_str()))
        } else {
            Ok(self.row[pos].clone())
        }
    }

    fn columns(&'a self) -> &'a Vec<String> {
        self.columns.as_ref()
    }

    /// Return an `Iterator` over the values in the row.
    fn iter(&self) -> ValueIterator {
        ValueIterator{ iter: self.row.iter() }
    }
}

impl <'a> Row<'a> for RefRow<'a> {
    /// Return the contents of a cell by column index.
    fn at(&self, pos :usize) -> Result<Value, TableError> {
        if pos >= self.row.len() {
            let err_str = format!("Index {} is greater than row width {}", pos, self.row.len());
            Err(TableError::new(err_str.as_str()))
        } else {
            Ok(self.row[pos].clone())
        }
    }

    fn columns(&'a self) -> &'a Vec<String> {
        self.columns.as_ref()
    }

    /// Return an `Iterator` over the values in the row.
    fn iter(&self) -> ValueIterator {
        ValueIterator{ iter: self.row.iter() }
    }
}

impl <'a> Row<'a> for MutRefRow<'a> {
    /// Return the contents of a cell by column index.
    fn at(&self, pos :usize) -> Result<Value, TableError> {
        if pos >= self.row.len() {
            let err_str = format!("Index {} is greater than row width {}", pos, self.row.len());
            Err(TableError::new(err_str.as_str()))
        } else {
            Ok(self.row[pos].clone())
        }
    }

    fn columns(&'a self) -> &'a Vec<String> {
        self.columns.as_ref()
    }

    /// Return an `Iterator` over the values in the row.
    fn iter(&self) -> ValueIterator {
        ValueIterator{ iter: self.row.iter() }
    }
}

/// An iterator over the `Value`s in a `Row`.
pub struct ValueIterator<'a> {
    iter: core::slice::Iter<'a, Value>
}

impl <'a> Iterator for ValueIterator<'a> {
    type Item = &'a Value;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

