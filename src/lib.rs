#[macro_use]
extern crate log;
//extern crate csv;
//extern crate chrono;
//extern crate dtparse;

use std::io::{Error as IOError, Read};
use std::path::Path;

use chrono::{Local};
use chrono::naive::{NaiveDateTime};
use dtparse::parse;

mod row_table;

///
/// The main interface into the mem_table library
///
pub trait Table {
}

///
/// Various types of values found in the cells of a Table
///
pub enum Value {
    String(String),
    DateTime(NaiveDateTime),
    Integer(i64),
    Float(f64)
}

impl Value {
    fn new(value :&str) -> Value {
//        debug!("Value: {}", value);

        // first attempt to parse as a DateTime
        if value.contains(":") || value.contains("-") {
            if let Ok((dt, _offset)) = parse(value) {
                return Value::DateTime(dt);
            }
        }

        // next attempt to parse as a float
        if let Ok(f) = value.parse::<f64>() {
            return Value::Float(f);
        }

        // next as an integer
        if let Ok(i) = value.parse::<i64>() {
            return Value::Integer(i);
        }

        // finally, just go with a string
        Value::String(String::from(value))
    }
}

// these are for tests
#[cfg(test)] extern crate simple_logger;
#[cfg(test)] extern crate rand;
#[cfg(test)] use std::sync::{Once};
#[cfg(test)] static LOGGER_INIT: Once = Once::new();

