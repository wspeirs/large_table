use chrono::naive::{NaiveDateTime, NaiveDate, NaiveTime};
use dtparse::parse;
use ordered_float::OrderedFloat;
use std::fmt::{Display, Formatter, Error as FmtError};
use chrono::{Datelike, Timelike};


/// Various types of values found in the cells of a [`Table`](trait.Table.html)
#[derive(Debug, Clone, Hash, Ord, PartialOrd, Eq, PartialEq)]
pub enum Value {
    String(String),
    DateTime(NaiveDateTime),
    Date(NaiveDate),
    Time(NaiveTime),
    Integer(i64),
    Float(OrderedFloat<f64>),
    Empty
}

pub enum ValueType {
    String,
    DateTime,
    DateTimeFormat(String),  // format for the DateTime
    DateFormat(String),      // format for the Date
    TimeFormat(String),      // format for the Time
    Number,     // try to parse as Float first, then Integer
    Integer,
    Float,
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
                if dt.year() == 0 {
                    return Value::Time(dt.time());
                } else if dt.hour() == 0 {
                    return Value::Date(dt.date());
                } else {
                    return Value::DateTime(dt);
                }
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

    pub fn with_type(value :&str, value_type :&ValueType) -> Value {
        match value_type {
            ValueType::String => Value::String(value.to_string()),
            ValueType::DateTime => {
                let (dt, _offset) = dtparse::parse(value).unwrap();
                Value::DateTime(dt)
            },
            ValueType::DateTimeFormat(format) => Value::DateTime(NaiveDateTime::parse_from_str(value, format).expect(format!("Error parsing DateTime: {} using {}", value, format).as_str())),
            ValueType::DateFormat(format) => Value::Date(NaiveDate::parse_from_str(value, format).unwrap()),
            ValueType::TimeFormat(format) => Value::Time(NaiveTime::parse_from_str(value, format).unwrap()),
            ValueType::Number => {
                if let Ok(f) = value.parse::<f64>() {
                    Value::Float(OrderedFloat(f))
                } else {
                    Value::Integer(value.parse::<i64>().unwrap())
                }
            },
            ValueType::Integer => Value::Integer(value.parse::<i64>().unwrap()),
            ValueType::Float => Value::Float(OrderedFloat(value.parse::<f64>().unwrap())),
            ValueType::Empty => Value::Empty,
        }
    }

    pub fn as_string(&self) -> String {
         if let Value::String(s) = self {
             s.clone()
         } else {
             self.to_string()
         }
    }

    pub fn try_as_date_time(&self) -> Option<NaiveDateTime> {
        if let Value::DateTime(dt) = self {
            Some(dt.clone())
        } else {
            None
        }
    }

    pub fn as_date_time(&self) -> NaiveDateTime {
        self.try_as_date_time().unwrap()
    }

    pub fn try_as_date(&self) -> Option<NaiveDate> {
        if let Value::Date(d) = self {
            Some(d.clone())
        } else {
            None
        }
    }

    pub fn as_date(&self) -> NaiveDate {
        self.try_as_date().unwrap()
    }

    pub fn try_as_time(&self) -> Option<NaiveTime> {
        if let Value::Time(t) = self {
            Some(t.clone())
        } else {
            None
        }
    }

    pub fn as_time(&self) -> NaiveTime {
        self.try_as_time().unwrap()
    }

    pub fn try_as_integer(&self) -> Option<i64> {
        match self {
            Value::Integer(i) => Some(*i),
            Value::Float(f) => Some(f.0 as i64),
            _ => None
        }
    }

    pub fn as_integer(&self) -> i64 {
        self.try_as_integer().unwrap()
    }

    pub fn try_as_float(&self) -> Option<f64> {
        match self {
            Value::Integer(i) => Some(*i as f64),
            Value::Float(f) => Some(f.0),
            _ => None
        }
    }

    pub fn as_float(&self) -> f64 {
        self.try_as_float().unwrap()
    }

}

impl From<Value> for String {
    fn from(value :Value) -> Self {
        match value {
            Value::String(s) => String::from(s),
            Value::DateTime(dt) => format!("{}", dt),
            Value::Date(d) => format!("{}", d),
            Value::Time(t) => format!("{}", t),
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
            Value::Time(t) => format!("{}", t),
            Value::Date(d) => format!("{}", d),
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
            Value::Date(d) => write!(f, "{}", d),
            Value::Time(t) => write!(f, "{}", t),
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
