use std::rc::Rc;
use std::cell::RefCell;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;

use crate::value::Value;
use crate::table_error::TableError;
use crate::Table;


// playground: https://play.rust-lang.org/?version=stable&mode=debug&edition=2018&gist=fbac8bab1dc26bc89edf35e6d62b3170

// playground for Row & Iterators: https://play.rust-lang.org/?version=stable&mode=debug&edition=2018&gist=5b1ead8cdf0cbaac2941ec9e15a942d5

pub struct RowSlice<T> {
    pub(crate) column_map: Arc<Vec<(String, usize)>>,
    pub(crate) table: Arc<Mutex<T>>,
    pub(crate) row: usize
}

/// Operations that you can perform on a Row
pub trait Row {
    fn get(&self, column :&str) -> Value {
        self.get_checked(column).unwrap()
    }

    fn get_checked(&self, column :&str) -> Result<Value, TableError>;

    fn set(&mut self, column :&str, value :Value) -> Result<Value, TableError> {
        unimplemented!()
    }

    #[inline]
    fn width(&self) -> usize {
        self.columns().len()
    }

    fn columns(&self) -> Vec<String>;
}

