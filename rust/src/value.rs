//! Value type for zeno KV store.
//!
//! Equivalent to Zig's Value union, supporting:
//! - Null
//! - Boolean
//! - Integer (i64)
//! - Float (f64)
//! - String
//! - Bytes
//! - Array (Vec<Value>)
//! - Object (HashMap<String, Value>)

use std::collections::HashMap;

/// A value stored in the database.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<Value>),
    Object(HashMap<String, Value>),
}

impl Value {
    /// Create a null value.
    pub fn null() -> Self {
        Value::Null
    }

    /// Create a boolean value.
    pub fn bool(v: bool) -> Self {
        Value::Bool(v)
    }

    /// Create an integer value.
    pub fn int(v: i64) -> Self {
        Value::Int(v)
    }

    /// Create a float value.
    pub fn float(v: f64) -> Self {
        Value::Float(v)
    }

    /// Create a string value.
    pub fn string(v: impl Into<String>) -> Self {
        Value::String(v.into())
    }

    /// Create a bytes value.
    pub fn bytes(v: impl Into<Vec<u8>>) -> Self {
        Value::Bytes(v.into())
    }

    /// Create an array value.
    pub fn array(v: Vec<Value>) -> Self {
        Value::Array(v)
    }

    /// Create an object value.
    pub fn object(v: HashMap<String, Value>) -> Self {
        Value::Object(v)
    }

    /// Check if value is null.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Check if value is boolean.
    pub fn is_bool(&self) -> bool {
        matches!(self, Value::Bool(_))
    }

    /// Check if value is integer.
    pub fn is_int(&self) -> bool {
        matches!(self, Value::Int(_))
    }

    /// Check if value is float.
    pub fn is_float(&self) -> bool {
        matches!(self, Value::Float(_))
    }

    /// Check if value is string.
    pub fn is_string(&self) -> bool {
        matches!(self, Value::String(_))
    }

    /// Check if value is bytes.
    pub fn is_bytes(&self) -> bool {
        matches!(self, Value::Bytes(_))
    }

    /// Check if value is array.
    pub fn is_array(&self) -> bool {
        matches!(self, Value::Array(_))
    }

    /// Check if value is object.
    pub fn is_object(&self) -> bool {
        matches!(self, Value::Object(_))
    }

    /// Get boolean value.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(v) => Some(*v),
            _ => None,
        }
    }

    /// Get integer value.
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(v) => Some(*v),
            _ => None,
        }
    }

    /// Get float value.
    pub fn as_float(&self) -> Option<f64> {
        match self {
            Value::Float(v) => Some(*v),
            _ => None,
        }
    }

    /// Get string reference.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(v) => Some(v.as_str()),
            _ => None,
        }
    }

    /// Get bytes reference.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(v) => Some(v.as_slice()),
            _ => None,
        }
    }

    /// Get array reference.
    pub fn as_array(&self) -> Option<&Vec<Value>> {
        match self {
            Value::Array(v) => Some(v),
            _ => None,
        }
    }

    /// Get object reference.
    pub fn as_object(&self) -> Option<&HashMap<String, Value>> {
        match self {
            Value::Object(v) => Some(v),
            _ => None,
        }
    }
}

impl Default for Value {
    fn default() -> Self {
        Value::Null
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int(v)
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Int(v as i64)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float(v)
    }
}

impl From<f32> for Value {
    fn from(v: f32) -> Self {
        Value::Float(v as f64)
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_string())
    }
}

impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Value::Bytes(v)
    }
}

impl From<Vec<Value>> for Value {
    fn from(v: Vec<Value>) -> Self {
        Value::Array(v)
    }
}

impl From<HashMap<String, Value>> for Value {
    fn from(v: HashMap<String, Value>) -> Self {
        Value::Object(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null() {
        let v = Value::null();
        assert!(v.is_null());
        assert!(!v.is_bool());
    }

    #[test]
    fn test_bool() {
        let v = Value::bool(true);
        assert!(v.is_bool());
        assert_eq!(v.as_bool(), Some(true));

        let v = Value::bool(false);
        assert_eq!(v.as_bool(), Some(false));
    }

    #[test]
    fn test_int() {
        let v = Value::int(42);
        assert!(v.is_int());
        assert_eq!(v.as_int(), Some(42));
    }

    #[test]
    fn test_float() {
        let v = Value::float(3.14);
        assert!(v.is_float());
        assert_eq!(v.as_float(), Some(3.14));
    }

    #[test]
    fn test_string() {
        let v = Value::string("hello");
        assert!(v.is_string());
        assert_eq!(v.as_str(), Some("hello"));
    }

    #[test]
    fn test_bytes() {
        let v = Value::bytes(vec![1, 2, 3]);
        assert!(v.is_bytes());
        assert_eq!(v.as_bytes(), Some(vec![1, 2, 3].as_slice()));
    }

    #[test]
    fn test_array() {
        let v = Value::array(vec![Value::int(1), Value::int(2)]);
        assert!(v.is_array());
        assert_eq!(v.as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_object() {
        let mut map = HashMap::new();
        map.insert("key".to_string(), Value::string("value"));
        let v = Value::object(map);
        assert!(v.is_object());
        assert!(v.as_object().unwrap().contains_key("key"));
    }

    #[test]
    fn test_clone() {
        let v = Value::string("hello");
        let cloned = v.clone();
        assert_eq!(v, cloned);
    }

    #[test]
    fn test_from_conversions() {
        let v: Value = true.into();
        assert!(v.is_bool());

        let v: Value = 42i64.into();
        assert!(v.is_int());

        let v: Value = "hello".into();
        assert!(v.is_string());

        let v: Value = vec![Value::int(1)].into();
        assert!(v.is_array());
    }
}
