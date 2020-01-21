use std::path::Path;
use std::io::{Error as IOError};

use csv::Reader;
use rayon::prelude::*;

use crate::{Table, Value};

///
/// A table with row-oriented data
///
pub struct RowTable {
    columns: Vec<String>,
    rows: Vec<Vec<Option<Value>>>
}

impl Table for RowTable {
}

impl RowTable {
    fn new(columns :&[String]) -> impl Table {
        RowTable {
            columns: Vec::from(columns),
            rows: Vec::new()
        }
    }

    fn from_csv<P: AsRef<Path>>(path: P) -> Result<impl Table, IOError> {
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
                table_row.push(csv_row.get(c).map(|v| Value::new(v)));
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

    use crate::row_table::RowTable;
    use crate::LOGGER_INIT;

    #[test]
    fn from_csv() {
        LOGGER_INIT.call_once(|| simple_logger::init_with_level(Level::Debug).unwrap()); // this will panic on error

        let path = Path::new("/export/stock_stuff/199x.csv");

        let start = Instant::now();
        let table = RowTable::from_csv(path).expect("Error creating RowTable");
        let end = Instant::now();

        println!("DONE: {}s", (end-start).as_secs());
    }
}