use chrono::{Local};
use chrono::naive::{NaiveDateTime};
use dtparse::parse;
use ordered_float::OrderedFloat;


///
/// Various types of values found in the cells of a Table
///
#[derive(Debug, Clone, Hash, Ord, PartialOrd, Eq, PartialEq)]
pub enum Value {
    String(String),
    DateTime(NaiveDateTime),
    Integer(i64),
    Float(OrderedFloat<f64>),
    Empty
}

impl Value {
    pub fn new(value :&str) -> Value {
        // first check to see if it's empty
        if value.is_empty() {
            return Value::Empty;
        }

        // next attempt to parse as a DateTime
        if value.contains("-") || value.contains("/") || value.contains(":") {
            if let Ok((dt, _offset)) = parse(value) {
                return Value::DateTime(dt);
            }
        }

        // next attempt to parse as a float
        if value.contains(".") {
            if let Ok(f) = value.parse::<f64>() {
                return Value::Float(OrderedFloat(f));
            }
        }

        // next as an integer
        if let Ok(i) = value.parse::<i64>() {
            return Value::Integer(i);
        }

        // finally, just go with a string
        Value::String(String::from(value))
    }
}

impl From<Value> for String {
    fn from(value :Value) -> Self {
        match value {
            Value::String(s) => String::from(s),
            Value::DateTime(dt) => format!("{}", dt),
            Value::Float(f) => format!("{}", f),
            Value::Integer(i) => format!("{}", i),
            Value::Empty => String::new(),
        }
    }
}

impl From<&Value> for String {
    fn from(value :&Value) -> Self {
        match value {
            Value::String(s) => String::from(s),
            Value::DateTime(dt) => format!("{}", dt),
            Value::Float(f) => format!("{}", f),
            Value::Integer(i) => format!("{}", i),
            Value::Empty => String::new(),
        }
    }
}