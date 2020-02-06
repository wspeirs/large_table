use chrono::naive::{NaiveDateTime};
use dtparse::parse;
use ordered_float::OrderedFloat;
use std::fmt::{Display, Formatter, Error as FmtError};


/// Various types of values found in the cells of a [`Table`](trait.Table.html)
#[derive(Debug, Clone, Hash, Ord, PartialOrd, Eq, PartialEq)]
pub enum Value {
    String(String),
    DateTime(NaiveDateTime),
    Integer(i64),
    Float(OrderedFloat<f64>),
    Empty
}

impl Value {
    /// Constructs a new [`Value`] from a `&str`.
    ///
    /// The method constructs a [`Value`] using the following:
    /// 1. checks to see if the string is empty, then constructs `Value::Empty`
    /// 1. if the string contains `-`, `/`, or `:`, then attempts to parse as a [`DateTime`](#https://docs.rs/chrono/*/chrono/struct.DateTime.html)
    /// 1. if the string contains `.`, then attempts to parse as a `f64`
    /// 1. if the string can be parsed as a `i64`, then a `Value::Integer` is constructed
    /// 1. finally a `Value::String` is constructed using the string
    ///
    /// [`Value`]: enum.Value.html
    pub fn new(value :&str) -> Value {
        // first check to see if it's empty
        if value.is_empty() {
            return Value::Empty;
        }

        let dt_char_count = value.chars().try_fold(0i64, |sum, c| {
            if c == '-' || c == '/' || c == ':' {
                Some(sum + 1)
            } else if c.is_digit(10) || [' ', 'p', 'P', 'a', 'A', 'm', 'M', 'T', 'Z'].iter().any(|dt_char| c == *dt_char) {
                Some(sum)
            } else {
                None // make sure it's negative
            }
        });

        if dt_char_count.is_some() && dt_char_count.unwrap() > 0 {
            if let Ok((dt, _offset)) = parse(value) {
                return Value::DateTime(dt);
            }
        }

        let float_char_count = value.chars().try_fold(0i64, |sum, c| {
            if c == '.' {
                Some(sum + 1)
            } else if c.is_digit(10) || c == '-' {
                Some(sum)
            } else {
                None // make sure it's negative
            }
        });

        // next attempt to parse as a float
        if float_char_count.is_some() && float_char_count.unwrap() == 1 {
            if let Ok(f) = value.parse::<f64>() {
                return Value::Float(OrderedFloat(f));
            }
        }

        // next as an integer
        if value.chars().all(|c| c.is_digit(10) || c == '-') {
            if let Ok(i) = value.parse::<i64>() {
                return Value::Integer(i);
            }
        }

        // finally, just go with a string
        Value::String(String::from(value))
    }

    pub fn as_string(&self) -> Option<String> {
         if let Value::String(s) = self {
             Some(s.clone())
         } else {
             None
         }
    }

    pub fn as_date_time(&self) -> Option<NaiveDateTime> {
        if let Value::DateTime(dt) = self {
            Some(dt.clone())
        } else {
            None
        }
    }

    pub fn as_integer(&self) -> Option<i64> {
        match self {
            Value::Integer(i) => Some(*i),
            Value::Float(f) => Some(f.0 as i64),
            _ => None
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self {
            Value::Integer(i) => Some(*i as f64),
            Value::Float(f) => Some(f.0),
            _ => None
        }
    }

    pub fn old(value :&str) -> Value {
        // first check to see if it's empty
        if value.is_empty() {
            return Value::Empty;
        }

        // next attempt to parse as a DateTime
        if value.find(|c| c == '-' || c == '/' || c == ':').is_some() {
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

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match self {
            Value::String(s) => write!(f, "{}", s),
            Value::DateTime(d) => write!(f, "{}", d),
            Value::Integer(i) => write!(f, "{}", i),
            Value::Float(of) => write!(f, "{}", of),
            Value::Empty => write!(f, "")
        }
    }
}

#[cfg(test)]
mod test {
    use crate::Value;
    use dtparse::parse;
    use ordered_float::OrderedFloat;

    #[test]
    fn date_time() {
        let val = Value::new("12/23/56 05:07:08PM");

        assert_eq!(Value::DateTime(parse("12/23/56 05:07:08PM").unwrap().0), val);
    }

    #[test]
    fn float() {
        let val = Value::new("1.0");

        assert_eq!(Value::Float(OrderedFloat(1.0)), val);
    }

    #[test]
    fn integer() {
        let val = Value::new("235650708");

        assert_eq!(Value::Integer(235650708), val);
    }

//    #[test]
//    fn string() {
//        let val = Value::new("12/23/56 05:07:08PM");
//
//        assert_eq!(Value::DateTime(parse("12/23/56 05:07:08PM").unwrap().0), val);
//    }
}
