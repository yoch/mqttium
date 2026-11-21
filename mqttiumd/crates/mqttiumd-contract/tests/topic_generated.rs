//! Bounded grammar/expansion oracle independent of the production byte matcher.
//!
//! The expected language is constructed from symbolic levels. It never parses
//! a wire filter, splits a topic or calls the production matcher to determine
//! an expectation. The dictionary includes empty, system and distinct Unicode
//! spellings; all generated topics are valid, nonempty MQTT Topic Names.
use mqttiumd_contract::topic::{matches, valid_filter};
use std::collections::BTreeSet;

const LEVELS: [&str; 8] = ["", "a", "b", "$SYS", "$other", "é", "e\u{301}", "温度"];
const USER_ROOT_LEVELS: [&str; 6] = ["", "a", "b", "é", "e\u{301}", "温度"];
const MAX_LEVELS: usize = 3;

#[derive(Clone, Copy, Debug)]
enum Level {
    Literal(&'static str),
    One,
}
#[derive(Clone, Debug)]
struct Filter {
    prefix: Vec<Level>,
    many: bool,
}
impl Filter {
    fn wire(&self) -> String {
        let mut levels: Vec<&str> = self
            .prefix
            .iter()
            .map(|level| match level {
                Level::Literal(value) => *value,
                Level::One => "+",
            })
            .collect();
        if self.many {
            levels.push("#");
        }
        levels.join("/")
    }

    /// Substitute '+' from a finite level dictionary and enumerate the suffix
    /// lengths of '#'. Root wildcard expansions use the user-root dictionary,
    /// so system-topic exclusion is a grammar domain, not a matcher predicate.
    fn language(&self) -> BTreeSet<String> {
        let mut prefixes = vec![Vec::<&str>::new()];
        for (position, level) in self.prefix.iter().enumerate() {
            let alternatives: &[&str] = match level {
                Level::Literal(value) => std::slice::from_ref(value),
                Level::One if position == 0 => &USER_ROOT_LEVELS,
                Level::One => &LEVELS,
            };
            prefixes = extend(&prefixes, alternatives);
        }
        let mut words = BTreeSet::new();
        for prefix in prefixes {
            if self.many {
                for suffix_len in 0..=MAX_LEVELS - self.prefix.len() {
                    let mut expansions = vec![prefix.clone()];
                    for position in 0..suffix_len {
                        let alphabet: &[&str] = if prefix.is_empty() && position == 0 {
                            &USER_ROOT_LEVELS
                        } else {
                            &LEVELS
                        };
                        expansions = extend(&expansions, alphabet);
                    }
                    words.extend(expansions.into_iter().map(|levels| levels.join("/")));
                }
            } else {
                words.insert(prefix.join("/"));
            }
        }
        // A zero-level '#' expansion and one empty literal are not Topic Names.
        words.remove("");
        words
    }
}

fn extend<'a>(prefixes: &[Vec<&'a str>], alphabet: &[&'a str]) -> Vec<Vec<&'a str>> {
    prefixes
        .iter()
        .flat_map(|prefix| {
            alphabet.iter().map(move |level| {
                let mut extended = prefix.clone();
                extended.push(*level);
                extended
            })
        })
        .collect()
}

fn filters() -> Vec<Filter> {
    let mut all = Vec::new();
    let mut prefixes = vec![Vec::new()];
    for depth in 0..=MAX_LEVELS {
        if depth != 0 {
            for prefix in &prefixes {
                let filter = Filter {
                    prefix: prefix.clone(),
                    many: false,
                };
                if !filter.wire().is_empty() {
                    all.push(filter);
                }
            }
        }
        if depth < MAX_LEVELS {
            all.extend(prefixes.iter().map(|prefix| Filter {
                prefix: prefix.clone(),
                many: true,
            }));
        }
        prefixes = prefixes
            .into_iter()
            .flat_map(|prefix| {
                LEVELS
                    .into_iter()
                    .map(Level::Literal)
                    .chain([Level::One])
                    .map(move |level| {
                        let mut next = prefix.clone();
                        next.push(level);
                        next
                    })
            })
            .collect();
    }
    all
}

fn topics() -> BTreeSet<String> {
    let mut words = BTreeSet::new();
    let mut prefixes = vec![Vec::new()];
    for _ in 0..MAX_LEVELS {
        prefixes = extend(&prefixes, &LEVELS);
        words.extend(prefixes.iter().map(|levels| levels.join("/")));
    }
    words.remove("");
    words
}

#[test]
fn bounded_level_grammar_matches_its_independently_expanded_language() {
    let filters = filters();
    let topics = topics();
    assert_eq!(filters.len(), 909, "keep the generated workload finite");
    assert_eq!(topics.len(), 583, "keep the generated topic domain finite");
    let mut unique_filters = BTreeSet::new();
    for filter in filters {
        let wire = filter.wire();
        assert!(
            unique_filters.insert(wire.clone()),
            "generator alias: {wire}"
        );
        assert!(valid_filter(wire.as_bytes()), "grammar_rejected: {wire:?}");
        let language = filter.language();
        assert!(language.is_subset(&topics));
        for topic in &topics {
            assert_eq!(
                matches(wire.as_bytes(), topic.as_bytes()),
                language.contains(topic),
                "expanded_language_mismatch: filter={wire:?} topic={topic:?}"
            );
        }
    }
    assert_eq!(unique_filters.len(), 909);
}

#[test]
fn malformed_wildcard_placements_are_outside_the_level_grammar() {
    let mut cases = BTreeSet::from([String::new()]);
    for literal in ["a", "é", "温度", "$SYS"] {
        for invalid in [
            format!("{literal}+"),
            format!("+{literal}"),
            format!("{literal}#"),
            format!("#{literal}"),
            format!("{literal}/#/{literal}"),
            format!("{literal}/++"),
            format!("{literal}/##"),
            format!("{literal}/+{literal}"),
            format!("{literal}/{literal}#"),
            format!("{literal}\0/{literal}"),
        ] {
            cases.insert(invalid);
        }
    }
    assert_eq!(cases.len(), 41);
    for invalid in cases {
        assert!(
            !valid_filter(invalid.as_bytes()),
            "invalid_grammar_accepted: {invalid:?}"
        );
    }
    // Shared filters are a valid separate MQTT feature, outside this profile.
    for unsupported in ["$share/g/#", "$share/g/a/+", "$share/g/a"] {
        assert!(!valid_filter(unsupported.as_bytes()));
    }
    for invalid_utf8 in [b"\xff".as_slice(), b"\xed\xa0\x80", b"a/\xc0\xaf"] {
        assert!(!valid_filter(invalid_utf8));
    }
}
