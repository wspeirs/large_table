use std::rc::Rc;
use std::cell::RefCell;
use std::sync::Arc;
use std::collections::HashMap;

use crate::value::Value;
use crate::table_error::TableError;
use crate::Table;


// playground: https://play.rust-lang.org/?version=stable&mode=debug&edition=2018&gist=fbac8bab1dc26bc89edf35e6d62b3170

// playground for Row & Iterators: https://play.rust-lang.org/?version=stable&mode=debug&edition=2018&gist=5b1ead8cdf0cbaac2941ec9e15a942d5

#[derive(Debug)]
pub struct RowSlice<T> {
    pub(crate) column_map: Rc<HashMap<String, usize>>,
    pub(crate) table: Rc<RefCell<T>>,
    pub(crate) row: usize
}

/// Operations that you can perform on a Row
pub trait Row {
    fn get(&self, column :&str) -> Result<&Value, TableError>;
    fn set(&mut self, column :&str, value :Value) -> Result<Value, TableError> {
        unimplemented!()
    }

    #[inline]
    fn width(&self) -> usize {
        self.columns().len()
    }

    fn columns(&self) -> Vec<String>;

    fn iter(&self) -> ValueIterator;
}


 /// An iterator over the `Value`s in a `Row`.
pub struct ValueIterator<'a> {
    iter: core::slice::Iter<'a, Value>
}

// TODO: Need to honor columns
impl <'a> Iterator for ValueIterator<'a> {
    type Item = &'a Value;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

