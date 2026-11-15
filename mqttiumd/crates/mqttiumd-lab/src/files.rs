//! The only way this bench reads files.
//!
//! Every file is resolved first: symbolic links are followed, and the
//! resolved path must be a regular file below the resolved root. A document
//! is then loaded with a size bound; evidence, which may be large, is hashed
//! as a stream and never loaded.

use std::fs;
use std::io::Read;
use std::path::{Component, Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::diagnostic::code;

/// The largest document this reader loads.
pub const MAX_DOCUMENT_BYTES: u64 = 16 * 1024 * 1024;

/// Why a file is refused: a diagnostic code and a message.
pub type Refusal = (&'static str, String);

/// Bytes of a document and the resolved path they were read from.
pub struct Loaded {
    pub path: PathBuf,
    pub bytes: Vec<u8>,
}

/// Resolves `path` to a regular file below `canonical_root`.
fn resolve(canonical_root: &Path, path: &Path) -> Result<(PathBuf, u64), Refusal> {
    let shown = path.display();
    let unreadable = |error: std::io::Error| (code::FILE_UNREADABLE, format!("{shown}: {error}"));
    let resolved = path.canonicalize().map_err(unreadable)?;
    if !resolved.starts_with(canonical_root) {
        return Err((
            code::PATH_ESCAPE,
            format!("{shown} resolves outside the mqttiumd root"),
        ));
    }
    let metadata = fs::metadata(&resolved).map_err(unreadable)?;
    if !metadata.is_file() {
        return Err((
            code::FILE_UNREADABLE,
            format!("{shown} is not a regular file"),
        ));
    }
    Ok((resolved, metadata.len()))
}

/// Loads one document of at most [`MAX_DOCUMENT_BYTES`].
pub fn read_document(canonical_root: &Path, path: &Path) -> Result<Loaded, Refusal> {
    let shown = path.display();
    let (resolved, length) = resolve(canonical_root, path)?;
    let too_large = || {
        (
            code::FILE_TOO_LARGE,
            format!("{shown} is larger than {MAX_DOCUMENT_BYTES} bytes"),
        )
    };
    if length > MAX_DOCUMENT_BYTES {
        return Err(too_large());
    }
    // The size is checked again while reading: the file may have grown.
    let mut bytes = Vec::new();
    fs::File::open(&resolved)
        .and_then(|file| file.take(MAX_DOCUMENT_BYTES + 1).read_to_end(&mut bytes))
        .map_err(|error| (code::FILE_UNREADABLE, format!("{shown}: {error}")))?;
    if bytes.len() as u64 > MAX_DOCUMENT_BYTES {
        return Err(too_large());
    }
    Ok(Loaded {
        path: resolved,
        bytes,
    })
}

/// SHA-256 and length of everything `reader` yields, in constant memory.
pub fn hash_stream(mut reader: impl Read) -> std::io::Result<(String, u64)> {
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    let mut length = 0u64;
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        length += read as u64;
    }
    let digest = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Ok((digest, length))
}

/// SHA-256 and length of a file of any size below the root.
pub fn hash_file(canonical_root: &Path, path: &Path) -> Result<(String, u64), Refusal> {
    let (resolved, _) = resolve(canonical_root, path)?;
    fs::File::open(&resolved)
        .and_then(hash_stream)
        .map_err(|error| {
            (
                code::FILE_UNREADABLE,
                format!("{}: {error}", path.display()),
            )
        })
}

/// The contract directory and its schema directory, both relative to the
/// root with `/` separators: `contracts/v1` gives `contracts/schema/v1`.
/// A trailing separator and `.` segments do not matter.
pub fn contract_directories(contracts: &Path) -> Result<(String, String), String> {
    let mut segments = Vec::new();
    for component in contracts.components() {
        match component {
            Component::Normal(segment) => segments.push(segment.to_string_lossy().into_owned()),
            Component::CurDir => {}
            _ => {
                return Err(format!(
                    "{}: the contract directory must be a path below the root",
                    contracts.display()
                ));
            }
        }
    }
    let Some((version, parents)) = segments.split_last() else {
        return Err("the contract directory has no name".to_owned());
    };
    let mut schema = parents.to_vec();
    schema.push("schema".to_owned());
    schema.push(version.clone());
    Ok((segments.join("/"), schema.join("/")))
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{contract_directories, hash_stream};

    #[test]
    fn the_schema_directory_is_derived_in_one_place() {
        let expected = Ok(("contracts/v1".to_owned(), "contracts/schema/v1".to_owned()));
        for spelling in [
            "contracts/v1",
            "contracts/v1/",
            "./contracts/v1",
            "contracts/./v1//",
        ] {
            assert_eq!(
                contract_directories(Path::new(spelling)),
                expected,
                "{spelling}"
            );
        }
        assert_eq!(
            contract_directories(Path::new("v1")),
            Ok(("v1".to_owned(), "schema/v1".to_owned()))
        );
        for refused in [
            "",
            ".",
            "/abs/contracts/v1",
            "../contracts/v1",
            "contracts/../v1",
        ] {
            assert!(
                contract_directories(Path::new(refused)).is_err(),
                "{refused}"
            );
        }
    }

    #[test]
    fn a_stream_is_hashed_without_being_loaded() {
        let (digest, length) = hash_stream(&b"abc"[..]).expect("in-memory read");
        assert_eq!(
            digest,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(length, 3);
        // Longer than the buffer, so that several reads are chained.
        let long = vec![b'a'; 200_000];
        let (digest, length) = hash_stream(&long[..]).expect("in-memory read");
        assert_eq!(length, 200_000);
        assert_eq!(digest, crate::manifest::sha256_hex(&long));
    }
}
