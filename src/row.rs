use crate::value::Value;
use crate::table_error::TableError;

// playground: https://play.rust-lang.org/?version=stable&mode=debug&edition=2018&gist=fbac8bab1dc26bc89edf35e6d62b3170

pub struct OwnedRow {
    pub(crate) columns: Vec<String>,
    pub(crate) values: Vec<Value>
}

/// A row in a `Table` or `TableSlice`.
pub struct Row<'a> {
    columns: &'a Vec<String>,
    values: Vec<&'a Value>
}

impl <'a> Row<'a> {
    /// Create a new Row given a list of columns and the list of values.
    pub fn new(columns :&'a Vec<String>, row :Vec<&'a Value>) -> Result<Row<'a>, TableError> {
        if columns.len() != row.len() {
            let err_str = format!("Length of columns does not match length of row: {} != {}", columns.len(), row.len());
            Err(TableError::new(err_str.as_str()))
        } else {
            Ok(Row { columns, values: row })
        }
    }

    /// Return the contents of a cell by column name.
    pub fn get(&'a self, column :&str) -> Result<&'a Value, TableError> {
        let pos = self.columns.iter().position(|c| c == column);

        match pos {
            Some(p) => Ok(&self.values[p]),
            None => {
                let err_str = format!("Could not find column {} in row", column);
                Err(TableError::new(err_str.as_str()))
            }
        }
    }

    /// Return the contents of a cell by column index.
    pub fn at(&self, index :usize) -> Result<&'a Value, TableError> {
        if index >= self.values.len() {
            let err_str = format!("Index {} is greater than row width {}", index, self.values.len());
            Err(TableError::new(err_str.as_str()))
        } else {
            Ok(&self.values[index])
        }
    }

    #[inline]
    pub fn width(&self) -> usize {
        self.values.len()
    }

    /// Return an `Iterator` over the values in the row.
    pub fn iter(&self) -> ValueIterator {
        unimplemented!()
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

//// Row-oriented iterators
//pub struct RowIter<'a, I> where I: Iterator<Item=&'a Row<'a>> {
//    iter: I
//}
//
//impl <'a, I: Iterator> Iterator for RowIter<'a, I> {
//    type Item = &'a Row<'a>;
//
//    fn next(&mut self) -> Option<Self::Item> {
//        self.iter.next()
//    }
//}

//impl <'a> DoubleEndedIterator for RowIter<'a> {
//    fn next_back(&mut self) -> Option<Self::Item> {
//        self.iter.next_back()
//    }
//}
//
//impl <'a> ExactSizeIterator for RowIter<'a> { }

/// `IntoIterator` for `Row`s.
pub struct RowIntoIter<'a>(pub(crate) Vec<Row<'a>>);

impl <'a> Iterator for RowIntoIter<'a> {
    type Item = Row<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.pop()
    }
}

impl <'a> ExactSizeIterator for RowIntoIter<'a> { }


//pub struct RowTableIterMut<'a> {
//    mut_iter: core::slice::IterMut<'a, Vec<Value>>
//}
//
//impl <'a> Iterator for RowTableIterMut<'a> {
//    type Item = &'a mut Vec<Value>;
//
//    fn next(&mut self) -> Option<Self::Item> {
//        self.mut_iter.next()
//    }
//}
