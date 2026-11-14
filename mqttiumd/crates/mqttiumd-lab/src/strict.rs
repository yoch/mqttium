//! Strict JSON reading (DEM-014).
//!
//! Duplicate keys are refused while parsing, before any map can overwrite a
//! value. Numbers with a fraction or an exponent, and integers beyond 64
//! bits, are refused too: version 1 formats hold small integers only and
//! carry every large quantity as a canonical decimal string.

use std::fmt;

use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Value};

use crate::diagnostic::{Diagnostic, code};

const DUPLICATE_MARK: &str = "mqttiumd duplicate key";
const NUMBER_MARK: &str = "mqttiumd non-integer number";

struct Strict(Value);

impl<'de> Deserialize<'de> for Strict {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(StrictVisitor).map(Strict)
    }
}

struct StrictVisitor;

impl<'de> Visitor<'de> for StrictVisitor {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Value, E> {
        Ok(Value::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Value, E> {
        Ok(Value::Number(value.into()))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Value, E> {
        Ok(Value::Number(value.into()))
    }

    fn visit_f64<E: de::Error>(self, value: f64) -> Result<Value, E> {
        Err(E::custom(format_args!("{NUMBER_MARK} {value}")))
    }

    fn visit_str<E>(self, value: &str) -> Result<Value, E> {
        Ok(Value::String(value.to_owned()))
    }

    fn visit_string<E>(self, value: String) -> Result<Value, E> {
        Ok(Value::String(value))
    }

    fn visit_unit<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut access: A) -> Result<Value, A::Error> {
        let mut items = Vec::new();
        while let Some(Strict(item)) = access.next_element()? {
            items.push(item);
        }
        Ok(Value::Array(items))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Value, A::Error> {
        let mut members = Map::new();
        while let Some(key) = access.next_key::<String>()? {
            if members.contains_key(&key) {
                return Err(de::Error::custom(format_args!("{DUPLICATE_MARK} {key:?}")));
            }
            let Strict(value) = access.next_value()?;
            members.insert(key, value);
        }
        Ok(Value::Object(members))
    }
}

/// Parses exactly one JSON value from `bytes`.
pub fn parse(bytes: &[u8]) -> Result<Value, Diagnostic> {
    serde_json::from_slice::<Strict>(bytes)
        .map(|Strict(value)| value)
        .map_err(|error| {
            let message = error.to_string();
            let code = if message.contains(DUPLICATE_MARK) {
                code::DUPLICATE_KEY
            } else if message.contains(NUMBER_MARK) {
                code::NUMBER_NOT_INTEGER
            } else {
                code::JSON_SYNTAX
            };
            Diagnostic::new(code, "", message)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn code_of(text: &str) -> &'static str {
        parse(text.as_bytes()).expect_err("must be refused").code
    }

    #[test]
    fn accepts_plain_documents() {
        let value =
            parse(br#"{"a": [1, -2, "x", true, null], "b": {"c": 65535}}"#).expect("valid JSON");
        assert_eq!(value["b"]["c"], 65535);
        assert_eq!(value["a"][1], -2);
    }

    #[test]
    fn refuses_duplicate_keys_at_any_depth() {
        assert_eq!(code_of(r#"{"a": 1, "a": 2}"#), code::DUPLICATE_KEY);
        assert_eq!(code_of(r#"{"a": 1, "a": 1}"#), code::DUPLICATE_KEY);
        assert_eq!(code_of(r#"{"x": [{"k": 1, "k": 2}]}"#), code::DUPLICATE_KEY);
        assert_eq!(code_of(r#"{"a": 1, "a": 2}"#), code::DUPLICATE_KEY);
    }

    #[test]
    fn refuses_numbers_that_are_not_64_bit_integers() {
        assert_eq!(code_of("7.0"), code::NUMBER_NOT_INTEGER);
        assert_eq!(code_of("7e0"), code::NUMBER_NOT_INTEGER);
        assert_eq!(code_of(r#"{"n": 1.5}"#), code::NUMBER_NOT_INTEGER);
        assert_eq!(code_of("18446744073709551616"), code::NUMBER_NOT_INTEGER);
        assert!(parse(b"18446744073709551615").is_ok());
    }

    #[test]
    fn refuses_anything_but_one_value() {
        assert_eq!(code_of(""), code::JSON_SYNTAX);
        assert_eq!(code_of("{} {}"), code::JSON_SYNTAX);
        assert_eq!(code_of(r#"{"a": 1,}"#), code::JSON_SYNTAX);
        assert_eq!(code_of("\u{feff}{}"), code::JSON_SYNTAX);
        assert_eq!(code_of(r#"{"a": 01}"#), code::JSON_SYNTAX);
        assert_eq!(code_of("NaN"), code::JSON_SYNTAX);
    }
}
