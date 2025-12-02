use super::parse_sql_file;
use crate::model::Schema;
use crate::util::{Result, SchemaError};
use glob::glob;
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

/// Load schemas from multiple sources (files, directories, glob patterns).
/// Returns a merged Schema or error on conflicts.
pub fn load_schema_sources(sources: &[String]) -> Result<Schema> {
    // Resolve all sources to file paths, deduplicating
    let mut all_files: Vec<PathBuf> = Vec::new();
    let mut seen: BTreeSet<PathBuf> = BTreeSet::new();

    for source in sources {
        let files = resolve_source(source)?;
        for file in files {
            let canonical = file
                .canonicalize()
                .map_err(|e| SchemaError::ParseError(format!("Cannot resolve path: {e}")))?;
            if seen.insert(canonical.clone()) {
                all_files.push(file);
            }
        }
    }

    if all_files.is_empty() {
        return Err(SchemaError::ParseError(
            "No SQL files found in provided sources".to_string(),
        ));
    }

    // Parse all files, tracking file paths for error messages
    let mut file_schemas: Vec<(PathBuf, Schema)> = Vec::new();
    for file in &all_files {
        let schema = parse_sql_file(file.to_str().unwrap_or_default())?;
        file_schemas.push((file.clone(), schema));
    }

    // Merge all schemas with conflict tracking
    let mut merged = Schema::new();
    let mut object_sources: HashMap<String, PathBuf> = HashMap::new();

    for (path, schema) in file_schemas {
        // Check tables
        for (name, table) in schema.tables {
            if let Some(existing_path) = object_sources.get(&format!("table:{name}")) {
                return Err(SchemaError::ParseError(format!(
                    "Duplicate table \"{name}\" defined in:\n  - {}\n  - {}",
                    existing_path.display(),
                    path.display()
                )));
            }
            object_sources.insert(format!("table:{name}"), path.clone());
            merged.tables.insert(name, table);
        }

        for (name, enum_type) in schema.enums {
            if let Some(existing_path) = object_sources.get(&format!("enum:{name}")) {
                return Err(SchemaError::ParseError(format!(
                    "Duplicate enum \"{name}\" defined in:\n  - {}\n  - {}",
                    existing_path.display(),
                    path.display()
                )));
            }
            object_sources.insert(format!("enum:{name}"), path.clone());
            merged.enums.insert(name, enum_type);
        }

        for (sig, func) in schema.functions {
            if let Some(existing_path) = object_sources.get(&format!("func:{sig}")) {
                return Err(SchemaError::ParseError(format!(
                    "Duplicate function \"{sig}\" defined in:\n  - {}\n  - {}",
                    existing_path.display(),
                    path.display()
                )));
            }
            object_sources.insert(format!("func:{sig}"), path.clone());
            merged.functions.insert(sig, func);
        }
    }

    Ok(merged)
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
    let entries =
        glob(pattern).map_err(|e| SchemaError::ParseError(format!("Invalid glob pattern: {e}")))?;

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

    /// Test helper: Merge two schemas, erroring on conflicts.
    fn merge_schema(
        mut base: Schema,
        other: Schema,
        base_path: &Path,
        other_path: &Path,
    ) -> Result<Schema> {
        for (name, table) in other.tables {
            if base.tables.contains_key(&name) {
                return Err(SchemaError::ParseError(format!(
                    "Duplicate table \"{}\" defined in:\n  - {}\n  - {}",
                    name,
                    base_path.display(),
                    other_path.display()
                )));
            }
            base.tables.insert(name, table);
        }

        for (name, enum_type) in other.enums {
            if base.enums.contains_key(&name) {
                return Err(SchemaError::ParseError(format!(
                    "Duplicate enum \"{}\" defined in:\n  - {}\n  - {}",
                    name,
                    base_path.display(),
                    other_path.display()
                )));
            }
            base.enums.insert(name, enum_type);
        }

        for (sig, func) in other.functions {
            if base.functions.contains_key(&sig) {
                return Err(SchemaError::ParseError(format!(
                    "Duplicate function \"{}\" defined in:\n  - {}\n  - {}",
                    sig,
                    base_path.display(),
                    other_path.display()
                )));
            }
            base.functions.insert(sig, func);
        }

        Ok(base)
    }

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
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("No SQL files found"));
    }

    #[test]
    fn merge_schemas_no_conflict() {
        let mut base = Schema::new();
        base.tables.insert(
            "users".to_string(),
            crate::model::Table {
                name: "users".to_string(),
                schema: "public".to_string(),
                columns: std::collections::BTreeMap::new(),
                indexes: Vec::new(),
                primary_key: None,
                foreign_keys: Vec::new(),
                check_constraints: Vec::new(),
                comment: None,
                row_level_security: false,
                policies: Vec::new(),
            },
        );

        let mut other = Schema::new();
        other.tables.insert(
            "posts".to_string(),
            crate::model::Table {
                name: "posts".to_string(),
                schema: "public".to_string(),
                columns: std::collections::BTreeMap::new(),
                indexes: Vec::new(),
                primary_key: None,
                foreign_keys: Vec::new(),
                check_constraints: Vec::new(),
                comment: None,
                row_level_security: false,
                policies: Vec::new(),
            },
        );

        let result = merge_schema(base, other, Path::new("a.sql"), Path::new("b.sql"));
        assert!(result.is_ok());
        let merged = result.unwrap();
        assert_eq!(merged.tables.len(), 2);
        assert!(merged.tables.contains_key("users"));
        assert!(merged.tables.contains_key("posts"));
    }

    #[test]
    fn merge_schemas_duplicate_table_errors() {
        let mut base = Schema::new();
        base.tables.insert(
            "users".to_string(),
            crate::model::Table {
                name: "users".to_string(),
                schema: "public".to_string(),
                columns: std::collections::BTreeMap::new(),
                indexes: Vec::new(),
                primary_key: None,
                foreign_keys: Vec::new(),
                check_constraints: Vec::new(),
                comment: None,
                row_level_security: false,
                policies: Vec::new(),
            },
        );

        let mut other = Schema::new();
        other.tables.insert(
            "users".to_string(),
            crate::model::Table {
                name: "users".to_string(),
                schema: "public".to_string(),
                columns: std::collections::BTreeMap::new(),
                indexes: Vec::new(),
                primary_key: None,
                foreign_keys: Vec::new(),
                check_constraints: Vec::new(),
                comment: None,
                row_level_security: false,
                policies: Vec::new(),
            },
        );

        let result = merge_schema(base, other, Path::new("a.sql"), Path::new("b.sql"));
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("users"));
        assert!(err.contains("a.sql"));
        assert!(err.contains("b.sql"));
    }

    #[test]
    fn merge_schemas_duplicate_enum_errors() {
        let mut base = Schema::new();
        base.enums.insert(
            "status".to_string(),
            crate::model::EnumType {
                name: "status".to_string(),
                schema: "public".to_string(),
                values: vec!["active".to_string()],
            },
        );

        let mut other = Schema::new();
        other.enums.insert(
            "status".to_string(),
            crate::model::EnumType {
                name: "status".to_string(),
                schema: "public".to_string(),
                values: vec!["inactive".to_string()],
            },
        );

        let result = merge_schema(base, other, Path::new("a.sql"), Path::new("b.sql"));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("status"));
    }

    #[test]
    fn merge_schemas_duplicate_function_errors() {
        let mut base = Schema::new();
        base.functions.insert(
            "my_func()".to_string(),
            crate::model::Function {
                name: "my_func".to_string(),
                schema: "public".to_string(),
                arguments: Vec::new(),
                return_type: "void".to_string(),
                language: "sql".to_string(),
                body: "SELECT 1".to_string(),
                volatility: crate::model::Volatility::Volatile,
                security: crate::model::SecurityType::Invoker,
            },
        );

        let mut other = Schema::new();
        other.functions.insert(
            "my_func()".to_string(),
            crate::model::Function {
                name: "my_func".to_string(),
                schema: "public".to_string(),
                arguments: Vec::new(),
                return_type: "void".to_string(),
                language: "sql".to_string(),
                body: "SELECT 2".to_string(),
                volatility: crate::model::Volatility::Volatile,
                security: crate::model::SecurityType::Invoker,
            },
        );

        let result = merge_schema(base, other, Path::new("a.sql"), Path::new("b.sql"));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("my_func"));
    }

    #[test]
    fn load_multiple_files() {
        let dir = TempDir::new().unwrap();
        fs::write(
            dir.path().join("enums.sql"),
            "CREATE TYPE status AS ENUM ('active', 'inactive');",
        )
        .unwrap();
        fs::write(
            dir.path().join("users.sql"),
            "CREATE TABLE users (id BIGINT PRIMARY KEY, status status);",
        )
        .unwrap();

        let sources = vec![format!("{}/*.sql", dir.path().display())];
        let schema = load_schema_sources(&sources).unwrap();

        assert_eq!(schema.enums.len(), 1);
        assert!(schema.enums.contains_key("status"));
        assert_eq!(schema.tables.len(), 1);
        assert!(schema.tables.contains_key("users"));
    }

    #[test]
    fn load_detects_conflicts_across_files() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("a.sql"), "CREATE TABLE users (id INT);").unwrap();
        fs::write(dir.path().join("b.sql"), "CREATE TABLE users (name TEXT);").unwrap();

        let sources = vec![format!("{}/*.sql", dir.path().display())];
        let result = load_schema_sources(&sources);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Duplicate table"));
    }
}
