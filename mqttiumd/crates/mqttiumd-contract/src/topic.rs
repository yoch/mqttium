//! MQTT 5 topic filters, byte-exact UTF-8 levels, no normalization (ADR 0031).

/// Shared subscriptions belong to a separate unsupported profile.
#[must_use]
pub fn valid_filter(filter: &[u8]) -> bool {
    if filter.is_empty()
        || filter.len() > usize::from(u16::MAX)
        || filter.starts_with(b"$share/")
        || filter.contains(&0)
        || core::str::from_utf8(filter).is_err()
    {
        return false;
    }
    for (i, b) in filter.iter().enumerate() {
        let start = i == 0 || filter[i - 1] == b'/';
        if (*b == b'#' && (!start || i + 1 != filter.len()))
            || (*b == b'+' && (!start || (i + 1 != filter.len() && filter[i + 1] != b'/')))
        {
            return false;
        }
    }
    true
}

/// Linear level comparison without allocation, recursion or backtracking.
/// Callers validate stored filters before they become routes.
#[must_use]
pub fn matches(filter: &[u8], topic: &[u8]) -> bool {
    if topic.first() == Some(&b'$') && matches!(filter.first(), Some(b'+' | b'#')) {
        return false;
    }
    let mut filters = filter.split(|b| *b == b'/');
    let mut topics = topic.split(|b| *b == b'/');
    loop {
        match (filters.next(), topics.next()) {
            (Some(b"#"), _) => return true,
            (Some(f), Some(t)) if f == b"+" || f == t => {}
            (None, None) => return true,
            _ => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn independent_mqtt_table_and_empty_levels() {
        for (f, t, yes) in [
            ("a/#", "a", true),
            ("a/#", "a/", true),
            ("a/+", "a", false),
            ("a/+", "a/", true),
            ("+/+", "/x", true),
            ("#", "$SYS/x", false),
            ("$SYS/#", "$SYS/x", true),
            ("a/+/#", "a/b", true),
            ("é/+/温度", "é//温度", true),
            ("é/#", "e/x", false),
            ("a/+", "a/x/y", false),
            ("a/+/", "a//", true),
            ("a", "A", false),
            ("/", "/", true),
            ("+/+", "/", true),
        ] {
            assert_eq!(matches(f.as_bytes(), t.as_bytes()), yes, "{f} -> {t}");
        }
    }
    #[test]
    fn independent_filter_grammar() {
        for f in ["", "a#", "a/+x", "a/#/b", "a/++", "$share/g/a", "a\0b"] {
            assert!(!valid_filter(f.as_bytes()), "{f}");
        }
        for f in ["#", "+", "a/+/#", "/", "a//+", "$SYS/#", "温度/+"] {
            assert!(valid_filter(f.as_bytes()), "{f}");
        }
    }
}
