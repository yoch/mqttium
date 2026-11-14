//! Canonical decimal text for 64-bit quantities (DEM-005, DEM-014).
//!
//! JSON files carry every 64-bit identifier, sequence and counter as a
//! decimal string. Exactly one spelling is accepted per value: no sign, no
//! leading zero, no whitespace, no line break, no fraction.

/// Why a string is not the canonical decimal spelling of a `u64`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DecimalError {
    /// The string is empty.
    Empty,
    /// A byte is not an ASCII digit: sign, space, line break, dot, letter.
    NonDigit,
    /// More than one digit and the first one is `0`.
    LeadingZero,
    /// More than twenty digits.
    TooLong,
    /// The digits denote a value above `u64::MAX`.
    Overflow,
}

/// The longest canonical spelling: `u64::MAX` has twenty digits.
pub const MAX_DIGITS: usize = 20;

/// Parses the canonical decimal spelling of a `u64`, checking the whole string.
pub fn parse_u64(text: &str) -> Result<u64, DecimalError> {
    let bytes = text.as_bytes();
    if bytes.is_empty() {
        return Err(DecimalError::Empty);
    }
    if !bytes.iter().all(u8::is_ascii_digit) {
        return Err(DecimalError::NonDigit);
    }
    if bytes.len() > 1 && bytes[0] == b'0' {
        return Err(DecimalError::LeadingZero);
    }
    if bytes.len() > MAX_DIGITS {
        return Err(DecimalError::TooLong);
    }
    let mut value: u64 = 0;
    for digit in bytes {
        value = value
            .checked_mul(10)
            .and_then(|scaled| scaled.checked_add(u64::from(digit - b'0')))
            .ok_or(DecimalError::Overflow)?;
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_the_boundaries() {
        assert_eq!(parse_u64("0"), Ok(0));
        assert_eq!(parse_u64("1"), Ok(1));
        assert_eq!(parse_u64("18446744073709551614"), Ok(u64::MAX - 1));
        assert_eq!(parse_u64("18446744073709551615"), Ok(u64::MAX));
    }

    #[test]
    fn rejects_every_other_spelling() {
        assert_eq!(parse_u64(""), Err(DecimalError::Empty));
        assert_eq!(parse_u64("00"), Err(DecimalError::LeadingZero));
        assert_eq!(parse_u64("007"), Err(DecimalError::LeadingZero));
        assert_eq!(parse_u64("+7"), Err(DecimalError::NonDigit));
        assert_eq!(parse_u64("-7"), Err(DecimalError::NonDigit));
        assert_eq!(parse_u64("7.0"), Err(DecimalError::NonDigit));
        assert_eq!(parse_u64(" 7"), Err(DecimalError::NonDigit));
        assert_eq!(parse_u64("7 "), Err(DecimalError::NonDigit));
        assert_eq!(parse_u64("7\n"), Err(DecimalError::NonDigit));
        assert_eq!(parse_u64("7e3"), Err(DecimalError::NonDigit));
        assert_eq!(parse_u64("٧"), Err(DecimalError::NonDigit));
        assert_eq!(
            parse_u64("18446744073709551616"),
            Err(DecimalError::Overflow)
        );
        assert_eq!(
            parse_u64("99999999999999999999"),
            Err(DecimalError::Overflow)
        );
        assert_eq!(
            parse_u64("100000000000000000000"),
            Err(DecimalError::TooLong)
        );
    }

    #[test]
    fn display_of_u64_is_the_canonical_spelling() {
        for value in [0, 1, 9, 10, 65_535, u64::MAX - 1, u64::MAX] {
            assert_eq!(parse_u64(&value.to_string()), Ok(value));
        }
    }
}
