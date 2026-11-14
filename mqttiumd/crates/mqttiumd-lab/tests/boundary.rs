//! The contract and the core cannot reach the outside world (DEM-003,
//! ADR 0007).
//!
//! Four layers enforce this, and each covers what the others cannot:
//!
//! 1. `#![no_std]` outside tests: files, sockets, clocks, threads and the
//!    randomly seeded `HashMap` do not exist for these crates. The compiler
//!    enforces it.
//! 2. `#![forbid(unsafe_code)]`: enforced by the compiler.
//! 3. No dependency beyond an explicit list, no build script, no feature:
//!    checked here through `cargo metadata`.
//! 4. Token rules, checked here on the lexed sources: no `unsafe`, `async`
//!    or `await`; no `extern` other than `extern crate alloc;`; conditional
//!    compilation only in three exact forms; no macro or attribute that
//!    pulls in a file or the build environment.
//!
//! The token rules work on Rust tokens, so comments, strings and spacing do
//! not matter. They are deliberately coarse: an identifier that merely
//! starts with `cfg` is refused.
//!
//! Not covered by any layer, and recorded as such in ADR 0007: `--cfg`
//! flags passed from outside the sources (the run manifest records the
//! flags), and global state held in a `static`.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::str::FromStr;

use proc_macro2::{Delimiter, TokenStream, TokenTree};
use serde_json::Value;

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// One Rust token, with groups flattened into their delimiters.
#[derive(Clone, PartialEq, Eq, Debug)]
enum Token {
    Ident(String),
    Punct(char),
    Literal,
    Open(char),
    Close(char),
}

fn flatten(stream: TokenStream, tokens: &mut Vec<Token>) {
    for tree in stream {
        match tree {
            TokenTree::Ident(ident) => tokens.push(Token::Ident(ident.to_string())),
            TokenTree::Punct(punct) => tokens.push(Token::Punct(punct.as_char())),
            TokenTree::Literal(_) => tokens.push(Token::Literal),
            TokenTree::Group(group) => {
                let (open, close) = match group.delimiter() {
                    Delimiter::Parenthesis => ('(', ')'),
                    Delimiter::Brace => ('{', '}'),
                    Delimiter::Bracket => ('[', ']'),
                    Delimiter::None => (' ', ' '),
                };
                tokens.push(Token::Open(open));
                flatten(group.stream(), tokens);
                tokens.push(Token::Close(close));
            }
        }
    }
}

/// Parses a compact description of tokens: identifiers and single
/// punctuation or delimiter characters separated by spaces.
fn pattern(description: &str) -> Vec<Token> {
    description
        .split_whitespace()
        .map(|word| match word {
            "(" | "[" | "{" => Token::Open(word.chars().next().expect("one character")),
            ")" | "]" | "}" => Token::Close(word.chars().next().expect("one character")),
            _ if word
                .chars()
                .all(|character| character.is_alphanumeric() || character == '_') =>
            {
                Token::Ident(word.to_owned())
            }
            _ => Token::Punct(word.chars().next().expect("one character")),
        })
        .collect()
}

/// Whether `tokens[start..]` begins with `expected`.
fn follows(tokens: &[Token], start: usize, expected: &[Token]) -> bool {
    tokens.get(start..start + expected.len()) == Some(expected)
}

/// Whether `tokens[..end]` ends with `expected`.
fn precedes(tokens: &[Token], end: usize, expected: &[Token]) -> bool {
    end >= expected.len() && &tokens[end - expected.len()..end] == expected
}

/// Whether the `cfg…` identifier at `index` is one of the three allowed
/// forms of conditional compilation.
fn allowed_cfg(tokens: &[Token], index: usize) -> bool {
    let Token::Ident(name) = &tokens[index] else {
        return false;
    };
    match name.as_str() {
        // `#![cfg_attr(not(test), no_std)]`, the crate attribute.
        "cfg_attr" => {
            precedes(tokens, index, &pattern("# ! ["))
                && follows(tokens, index + 1, &pattern("( not ( test ) , no_std ) ]"))
        }
        // `#[cfg(test)] mod` and `#[cfg(kani)] mod`: a whole module, never
        // an item or an expression that could swap a decision.
        "cfg" => {
            precedes(tokens, index, &pattern("# ["))
                && !precedes(tokens, index, &pattern("# ! ["))
                && (follows(tokens, index + 1, &pattern("( test ) ] mod"))
                    || follows(tokens, index + 1, &pattern("( kani ) ] mod")))
        }
        _ => false,
    }
}

/// Returns the reasons `source` breaks the token rules.
fn violations(source: &str) -> Vec<String> {
    let Ok(stream) = TokenStream::from_str(source) else {
        return vec!["does not lex as Rust".to_owned()];
    };
    let mut tokens = Vec::new();
    flatten(stream, &mut tokens);
    let mut found = Vec::new();
    for (index, token) in tokens.iter().enumerate() {
        let Token::Ident(name) = token else { continue };
        match name.as_str() {
            "unsafe" | "async" | "await" => found.push(format!("uses `{name}`")),
            "extern" => {
                if !follows(&tokens, index + 1, &pattern("crate alloc ;")) {
                    found.push("uses `extern` other than `extern crate alloc;`".to_owned());
                }
            }
            "include" | "include_str" | "include_bytes" | "env" | "option_env" => {
                if follows(&tokens, index + 1, &pattern("!")) {
                    found.push(format!("uses `{name}!`"));
                }
            }
            "path" => {
                if precedes(&tokens, index, &pattern("# ["))
                    || precedes(&tokens, index, &pattern("# ! ["))
                {
                    found.push("uses `#[path]`".to_owned());
                }
            }
            _ if name.starts_with("cfg") && !allowed_cfg(&tokens, index) => {
                found.push(format!("uses `{name}` outside the allowed forms"));
            }
            _ => {}
        }
    }
    found
}

/// Whether `source` holds the tokens of `attribute`.
fn has_attribute(source: &str, attribute: &str) -> bool {
    let mut tokens = Vec::new();
    if let Ok(stream) = TokenStream::from_str(source) {
        flatten(stream, &mut tokens);
    }
    let expected = pattern(attribute);
    (0..tokens.len()).any(|start| follows(&tokens, start, &expected))
}

fn sources(directory: &Path, found: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(directory).expect("source directory") {
        let path = entry.expect("entry").path();
        if path.is_dir() {
            sources(&path, found);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            found.push(path);
        }
    }
}

#[test]
fn the_allowed_forms_are_accepted() {
    for source in [
        "#![cfg_attr(not(test), no_std)]\n#![forbid(unsafe_code)]\nextern crate alloc;",
        "#[cfg(test)]\nmod tests {}",
        "#[cfg(kani)]\nmod proofs {}",
        "#[ cfg ( test ) ]\nmod tests { fn f() {} }",
        "fn f() { let r#async = 1; let r#unsafe = r#async; let _ = r#unsafe; }",
        "use alloc::vec::Vec;\npub fn f() -> Vec<u8> { Vec::new() }",
    ] {
        assert_eq!(
            violations(source),
            Vec::<String>::new(),
            "refused: {source}"
        );
    }
}

#[test]
fn comments_and_strings_are_not_code() {
    for source in [
        "// unsafe code and async fn are discussed here, with cfg(simulation)\nfn f() {}",
        "fn f() -> &'static str { \"extern crate std; .await unsafe \" }",
        "/// See `cfg(feature)` and `async move` in the guide.\nfn f() {}",
        "/* include!(\"x.rs\") and env!(\"HOME\") */ fn f() {}",
        "fn f() -> &'static str { r#\"#[path = \"x.rs\"] cfg!(test)\"# }",
    ] {
        assert_eq!(
            violations(source),
            Vec::<String>::new(),
            "refused: {source}"
        );
    }
}

#[test]
fn the_rules_see_the_original_spellings() {
    for source in [
        "fn decide() -> bool { cfg!(debug_assertions) }",
        "fn decide() -> bool { cfg ! (feature = \"sim\") }",
        "fn decide() -> bool { cfg!(test) }",
        "extern  crate std;",
        "extern\ncrate std;",
        "extern crate std;",
        "extern crate alloc as std;",
        "extern crate core;",
        "extern \"C\" { fn getpid() -> i32; }",
        "fn f() { unsafe{ core::hint::unreachable_unchecked() } }",
        "unsafe fn f() {}",
        "unsafe impl Send for X {}",
        "fn step() { other() . await }",
        "fn step() { other().await }",
        "pub async\nfn step() {}",
        "fn f() { let _ = async move { 1 }; }",
    ] {
        assert!(!violations(source).is_empty(), "accepted: {source}");
    }
}

#[test]
fn conditional_compilation_is_confined_to_three_forms() {
    for source in [
        "#[cfg (simulation)]\nfn decide() {}",
        "#[cfg(feature = \"sim\")]\nfn decide() {}",
        "#[cfg(not(test))]\nfn decide() {}",
        "#[cfg(test)]\nfn decide() {}",
        "#[cfg(test)]\npub mod tests {}",
        "#[cfg(kani)]\nconst BOUND: u8 = 1;",
        "#[cfg(any(test, kani))]\nmod tests {}",
        "#![cfg(test)]",
        "#[cfg_attr(test, allow(dead_code))]\nfn decide() {}",
        "#![cfg_attr(not(test), no_std, feature(x))]",
        "#![cfg_attr(feature = \"std\", no_std)]",
        "fn f() { let x = 1; #[cfg(test)] let x = 2; let _ = x; }",
        "cfg_select! { test => {} _ => {} }",
        "cfg_if::cfg_if! { if #[cfg(test)] {} }",
        "macro_rules! m { () => { #[cfg(simulation)] fn decide() {} }; }",
        "fn f() { let cfg_value = 1; let _ = cfg_value; }",
    ] {
        assert!(!violations(source).is_empty(), "accepted: {source}");
    }
}

#[test]
fn the_set_of_sources_is_closed() {
    for source in [
        "include!(\"../outside.rs\");",
        "const TEXT: &str = include_str!(\"/etc/hostname\");",
        "const BYTES: &[u8] = include_bytes ! (\"x\");",
        "const HOME: &str = env!(\"HOME\");",
        "const HOME: Option<&str> = option_env!(\"HOME\");",
        "#[path = \"../elsewhere.rs\"]\nmod elsewhere;",
        "#![path = \"x\"]",
        "fn broken( {",
    ] {
        assert!(!violations(source).is_empty(), "accepted: {source}");
    }
    // The same words as ordinary identifiers are not macros or attributes.
    assert_eq!(
        violations("fn f(path: u8, env: u8) -> u8 { path + env }"),
        Vec::<String>::new()
    );
}

#[test]
fn pure_crates_keep_their_source_rules() {
    for name in ["mqttiumd-contract", "mqttiumd-core"] {
        let directory = root().join("crates").join(name).join("src");
        let lib = fs::read_to_string(directory.join("lib.rs")).expect("lib.rs");
        assert!(
            has_attribute(&lib, "# ! [ cfg_attr ( not ( test ) , no_std ) ]"),
            "{name} must be no_std"
        );
        assert!(
            has_attribute(&lib, "# ! [ forbid ( unsafe_code ) ]"),
            "{name} must forbid unsafe"
        );
        let mut files = Vec::new();
        sources(&directory, &mut files);
        assert!(!files.is_empty());
        for file in files {
            let source = fs::read_to_string(&file).expect("source file");
            let found = violations(&source);
            assert!(found.is_empty(), "{}: {found:?}", file.display());
        }
    }
}

/// The dependencies each pure crate may have. A proof dependency may be
/// added here by an explicit decision record only (ADR 0007).
const ALLOWED_DEPENDENCIES: [(&str, &[&str]); 2] = [
    ("mqttiumd-contract", &[]),
    ("mqttiumd-core", &["mqttiumd-contract"]),
];

#[test]
fn pure_crates_have_no_unlisted_dependency_build_script_or_feature() {
    let output = Command::new(env!("CARGO"))
        .args([
            "metadata",
            "--format-version",
            "1",
            "--no-deps",
            "--offline",
        ])
        .current_dir(root())
        .output()
        .expect("cargo metadata runs");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let metadata: Value = serde_json::from_slice(&output.stdout).expect("metadata is JSON");
    for (name, allowed) in ALLOWED_DEPENDENCIES {
        let package = metadata["packages"]
            .as_array()
            .expect("packages")
            .iter()
            .find(|package| package["name"] == name)
            .unwrap_or_else(|| panic!("{name} is not in the workspace"));
        let dependencies: Vec<&str> = package["dependencies"]
            .as_array()
            .expect("dependencies")
            .iter()
            .map(|dependency| dependency["name"].as_str().expect("name"))
            .collect();
        assert_eq!(dependencies, allowed, "{name}: dependencies");
        let features = package["features"].as_object().expect("features");
        assert!(features.is_empty(), "{name}: features {features:?}");
        for target in package["targets"].as_array().expect("targets") {
            let kinds = target["kind"].as_array().expect("kind");
            assert!(
                !kinds
                    .iter()
                    .any(|kind| kind == "custom-build" || kind == "proc-macro"),
                "{name}: target {kinds:?}"
            );
        }
    }
}
