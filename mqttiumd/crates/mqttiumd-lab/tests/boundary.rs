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
//! 4. Token rules, checked here on the lexed sources. They are closed
//!    lists rather than lists of forbidden words:
//!    - a macro invocation must name a listed macro, or one the same file
//!      defines; an invocation through a macro variable is refused;
//!    - an attribute must have a listed name;
//!    - conditional compilation exists in three exact forms only;
//!    - a `use` declaration imports no macro that reads a file or the build
//!      environment, and renames nothing to the name of an allowed macro;
//!    - no `unsafe`, `async` or `await`; no `extern` other than
//!      `extern crate alloc;`.
//!
//! The token rules work on Rust tokens, so comments, strings and spacing do
//! not matter. A raw identifier designates the same name as the plain one:
//! `r#cfg!` is `cfg!` and `#[r#path]` is `#[path]`, and both are refused.
//! Only keywords are compared as written, because `r#async` is an ordinary
//! identifier. An alias cannot hide a macro: `use core::env as e; e!(…)`
//! is refused at the import and at the call, since `e` is not listed.
//!
//! The rules are deliberately coarse: an identifier that merely starts
//! with `cfg` is refused, and so is a macro nobody listed yet.
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

/// The standard macros a pure crate may invoke. Macros that a file defines
/// itself with `macro_rules!` are allowed as well: their bodies are scanned
/// like any other code. Extending this list is a decision (ADR 0007).
const ALLOWED_MACROS: [&str; 8] = [
    "assert",
    "assert_eq",
    "assert_ne",
    "debug_assert",
    "matches",
    "panic",
    "vec",
    "write",
];

/// The attributes a pure crate may use. `cfg` and `cfg_attr` are further
/// restricted to three exact forms.
const ALLOWED_ATTRIBUTES: [&str; 9] = [
    "allow", "cfg", "cfg_attr", "derive", "doc", "expect", "forbid", "must_use", "test",
];

/// Macros that read a file or the build environment. They are refused as
/// invocations by the allow-list; they are also refused in a `use`
/// declaration, so that none can be imported under another name.
const FORBIDDEN_IMPORTS: [&str; 5] = [
    "env",
    "option_env",
    "include",
    "include_str",
    "include_bytes",
];

/// Words that may stand before `!(` without being a macro name: in
/// `if !(a)` the `!` is a negation.
const KEYWORDS: [&str; 38] = [
    "as", "async", "await", "break", "const", "continue", "crate", "dyn", "else", "enum", "extern",
    "false", "fn", "for", "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub",
    "ref", "return", "self", "Self", "static", "struct", "super", "trait", "true", "type",
    "unsafe", "use", "where", "while",
];

/// The name an identifier designates. `r#cfg` designates `cfg`: the raw
/// prefix changes how a name is spelled, not what it names.
fn plain(name: &str) -> &str {
    name.strip_prefix("r#").unwrap_or(name)
}

fn is_punct(token: Option<&Token>, character: char) -> bool {
    token == Some(&Token::Punct(character))
}

/// Whether the `cfg…` identifier at `index` is one of the three allowed
/// forms of conditional compilation. The forms are matched on the tokens
/// as written, so a raw spelling is not one of them.
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

/// Names that the file defines with `macro_rules! name`.
fn local_macros(tokens: &[Token]) -> Vec<&str> {
    let mut defined = Vec::new();
    for (index, token) in tokens.iter().enumerate() {
        if *token == Token::Ident("macro_rules".to_owned())
            && is_punct(tokens.get(index + 1), '!')
            && let Some(Token::Ident(name)) = tokens.get(index + 2)
        {
            defined.push(plain(name));
        }
    }
    defined
}

/// Returns the reasons `source` breaks the token rules.
fn violations(source: &str) -> Vec<String> {
    let Ok(stream) = TokenStream::from_str(source) else {
        return vec!["does not lex as Rust".to_owned()];
    };
    let mut tokens = Vec::new();
    flatten(stream, &mut tokens);
    let local = local_macros(&tokens);
    let allowed_macro = |name: &str| ALLOWED_MACROS.contains(&name) || local.contains(&name);
    let mut found = Vec::new();

    for (index, token) in tokens.iter().enumerate() {
        // Attributes: `#[name …]` or `#![name …]`, with a listed name.
        if *token == Token::Punct('#') {
            let open = if is_punct(tokens.get(index + 1), '!') {
                index + 2
            } else {
                index + 1
            };
            if tokens.get(open) != Some(&Token::Open('[')) {
                continue;
            }
            match tokens.get(open + 1) {
                // A macro passes an attribute through: `#[$meta]`. The
                // attribute itself is checked where it is written.
                Some(Token::Punct('$')) => {}
                Some(Token::Ident(name)) => {
                    let qualified = is_punct(tokens.get(open + 2), ':');
                    if qualified || !ALLOWED_ATTRIBUTES.contains(&plain(name)) {
                        found.push(format!("uses the attribute `{name}`, which is not listed"));
                    }
                }
                _ => found.push("uses an attribute without a name".to_owned()),
            }
            continue;
        }

        let Token::Ident(name) = token else { continue };

        // Keywords are compared as written: `r#async` is an ordinary name.
        match name.as_str() {
            "unsafe" | "async" | "await" => found.push(format!("uses `{name}`")),
            "extern" => {
                if !follows(&tokens, index + 1, &pattern("crate alloc ;")) {
                    found.push("uses `extern` other than `extern crate alloc;`".to_owned());
                }
            }
            // A `use` declaration may not import a macro that reads a file
            // or the environment, nor rename anything to an allowed macro.
            "use" => {
                let end = tokens[index..]
                    .iter()
                    .position(|token| *token == Token::Punct(';'))
                    .map_or(tokens.len(), |offset| index + offset);
                for position in index + 1..end {
                    let Token::Ident(imported) = &tokens[position] else {
                        continue;
                    };
                    let imported = plain(imported);
                    if FORBIDDEN_IMPORTS.contains(&imported) {
                        found.push(format!("imports the macro `{imported}`"));
                    }
                    let renamed = tokens[position - 1] == Token::Ident("as".to_owned());
                    if renamed && allowed_macro(imported) {
                        found.push(format!("renames an item to the allowed macro `{imported}`"));
                    }
                }
            }
            _ => {}
        }

        // Conditional compilation, whatever the spelling of the name.
        if plain(name).starts_with("cfg") && !allowed_cfg(&tokens, index) {
            found.push(format!("uses `{name}` outside the allowed forms"));
        }

        // Macro invocations: `name!(…)`, `name![…]`, `name!{…}`. An
        // identifier followed by `!=` is a comparison, and a keyword
        // followed by `!(` is a negation.
        if is_punct(tokens.get(index + 1), '!')
            && matches!(tokens.get(index + 2), Some(Token::Open(_)))
            && !KEYWORDS.contains(&name.as_str())
            && !allowed_macro(plain(name))
        {
            found.push(format!("invokes the macro `{name}`, which is not listed"));
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
fn a_raw_identifier_does_not_hide_a_macro_or_an_attribute() {
    // Each of these compiles: `r#` does not change what a name designates.
    for source in [
        "pub fn decide() -> bool {\n    r#cfg!(debug_assertions)\n}",
        "pub const BUILD_DIRECTORY: &str =\n    r#env!(\"CARGO_MANIFEST_DIR\");",
        "#[r#path = \"other.rs\"]\nmod other;",
        "#[r#cfg(test)]\npub fn only_in_tests() {}",
        "const TEXT: &str = r#include_str!(\"/etc/hostname\");",
    ] {
        assert!(!violations(source).is_empty(), "accepted: {source}");
    }
}

#[test]
fn an_alias_does_not_hide_a_macro() {
    for source in [
        "use core::env as build_env;\n\npub const BUILD_DIRECTORY: &str =\n    build_env!(\"CARGO_MANIFEST_DIR\");",
        "use core::env as matches;\npub const D: &str = matches!(\"CARGO_MANIFEST_DIR\");",
        "use core::cfg as when;\npub fn decide() -> bool { when!(debug_assertions) }",
        "use core::{env as e};\npub const D: &str = e!(\"HOME\");",
        "pub const D: &str = core::env!(\"CARGO_MANIFEST_DIR\");",
        "macro_rules! call { ($m:ident) => { $m!(\"HOME\") }; }\npub const D: &str = call!(env);",
        "pub const WHERE: &str = file!();",
    ] {
        assert!(!violations(source).is_empty(), "accepted: {source}");
    }
}

#[test]
fn ordinary_code_is_still_accepted() {
    for source in [
        "fn f() { let r#async = 1; let r#unsafe = r#async; let _ = r#unsafe; }",
        "fn f(a: bool) -> bool { if !(a) { return !(a); } a != !a }",
        "fn never() -> ! { loop {} }",
        "use alloc::vec::Vec;\npub fn f() -> Vec<u8> { vec![1] }",
        "pub use mqttiumd_contract as contract;",
        "#[derive(Clone, Debug)]\n#[must_use]\npub struct S;",
        "fn f(x: u8) -> bool { matches!(x, 1 | 2) && { debug_assert!(x > 0); true } }",
        "macro_rules! twice { ($x:expr) => { $x + $x }; }\nfn f() -> u8 { twice!(1) }",
    ] {
        assert_eq!(
            violations(source),
            Vec::<String>::new(),
            "refused: {source}"
        );
    }
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
