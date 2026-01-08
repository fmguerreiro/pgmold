use sha2::{Digest, Sha256};
use std::path::Path;

pub fn compute_schema_hash(path: &Path) -> anyhow::Result<String> {
    let content = std::fs::read_to_string(path)?;
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    let result = hasher.finalize();
    Ok(format!("{result:x}"))
}

pub fn compute_path_hash(path: &Path) -> String {
    let canonical_path = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let path_str = canonical_path.to_string_lossy();
    let mut hasher = Sha256::new();
    hasher.update(path_str.as_bytes());
    let result = hasher.finalize();
    format!("{result:x}")
}

pub fn sanitize_db_error(error: &str) -> String {
    error
        .lines()
        .map(|line| {
            if line.contains("password") || line.contains("PASSWORD") {
                "Database connection failed (credentials redacted)".to_string()
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn compute_hash_returns_sha256() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "CREATE TABLE users (id INT);").unwrap();

        let hash = compute_schema_hash(file.path()).unwrap();

        assert_eq!(hash.len(), 64);
    }

    #[test]
    fn compute_hash_same_content_same_hash() {
        let mut file1 = NamedTempFile::new().unwrap();
        let mut file2 = NamedTempFile::new().unwrap();

        writeln!(file1, "CREATE TABLE users (id INT);").unwrap();
        writeln!(file2, "CREATE TABLE users (id INT);").unwrap();

        let hash1 = compute_schema_hash(file1.path()).unwrap();
        let hash2 = compute_schema_hash(file2.path()).unwrap();

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn compute_hash_different_content_different_hash() {
        let mut file1 = NamedTempFile::new().unwrap();
        let mut file2 = NamedTempFile::new().unwrap();

        writeln!(file1, "CREATE TABLE users (id INT);").unwrap();
        writeln!(file2, "CREATE TABLE posts (id INT);").unwrap();

        let hash1 = compute_schema_hash(file1.path()).unwrap();
        let hash2 = compute_schema_hash(file2.path()).unwrap();

        assert_ne!(hash1, hash2);
    }
}
