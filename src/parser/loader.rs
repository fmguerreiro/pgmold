use crate::model::Schema;
use crate::util::{Result, SchemaError};
use glob::glob;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// Load schemas from multiple sources (files, directories, glob patterns).
/// Returns a merged Schema or error on conflicts.
pub fn load_schema_sources(_sources: &[String]) -> Result<Schema> {
    // TODO: Implement in Task 4
    Ok(Schema::new())
}

/// Resolve a source pattern to a list of SQL file paths.
/// Handles: single files, directories (recursive *.sql), and glob patterns.
fn resolve_source(source: &str) -> Result<Vec<PathBuf>> {
    let path = Path::new(source);

    if path.is_file() {
        return Ok(vec![path.to_path_buf()]);
    }

    if path.is_dir() {
        let pattern = path.join("**/*.sql");
        return resolve_glob(pattern.to_str().unwrap_or(source));
    }

    resolve_glob(source)
}

fn resolve_glob(pattern: &str) -> Result<Vec<PathBuf>> {
    let entries = glob(pattern)
        .map_err(|e| SchemaError::ParseError(format!("Invalid glob pattern: {e}")))?;

    let mut files: Vec<PathBuf> = Vec::new();
    for entry in entries {
        let path = entry.map_err(|e| SchemaError::ParseError(format!("Glob error: {e}")))?;
        if path.is_file() {
            files.push(path);
        }
    }

    if files.is_empty() {
        return Err(SchemaError::ParseError(format!(
            "No SQL files found matching pattern: {pattern}"
        )));
    }

    files.sort();
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn resolve_single_file() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("schema.sql");
        fs::write(&file, "CREATE TABLE t (id INT);").unwrap();

        let result = resolve_source(file.to_str().unwrap()).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], file);
    }

    #[test]
    fn resolve_directory_finds_sql_files() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("a.sql"), "CREATE TABLE a (id INT);").unwrap();
        fs::write(dir.path().join("b.sql"), "CREATE TABLE b (id INT);").unwrap();
        fs::write(dir.path().join("readme.txt"), "not sql").unwrap();

        let result = resolve_source(dir.path().to_str().unwrap()).unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|p| p.extension().unwrap() == "sql"));
    }

    #[test]
    fn resolve_directory_recursive() {
        let dir = TempDir::new().unwrap();
        fs::create_dir(dir.path().join("subdir")).unwrap();
        fs::write(dir.path().join("root.sql"), "CREATE TABLE r (id INT);").unwrap();
        fs::write(
            dir.path().join("subdir/nested.sql"),
            "CREATE TABLE n (id INT);",
        )
        .unwrap();

        let result = resolve_source(dir.path().to_str().unwrap()).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn resolve_glob_pattern() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("users.sql"), "CREATE TABLE users (id INT);").unwrap();
        fs::write(dir.path().join("posts.sql"), "CREATE TABLE posts (id INT);").unwrap();

        let pattern = format!("{}/*.sql", dir.path().display());
        let result = resolve_source(&pattern).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn resolve_empty_pattern_errors() {
        let dir = TempDir::new().unwrap();
        let pattern = format!("{}/*.sql", dir.path().display());
        let result = resolve_source(&pattern);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No SQL files found"));
    }
}
