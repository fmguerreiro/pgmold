use super::{
    extract_function_references, extract_table_references, parse_sql_file, topological_sort,
};
use crate::model::Schema;
use crate::util::{Result, SchemaError};
use glob::glob;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};

/// Extract all object references from a schema.
/// Returns a set of qualified names (schema.name) that this schema depends on.
fn extract_schema_dependencies(schema: &Schema) -> HashSet<String> {
    let mut deps = HashSet::new();

    // Extract dependencies from functions
    for func in schema.functions.values() {
        let func_refs = extract_function_references(&func.body, &func.schema);
        let table_refs = extract_table_references(&func.body, &func.schema);

        for r in func_refs {
            deps.insert(format!("{}.{}", r.schema, r.name));
        }
        for r in table_refs {
            deps.insert(format!("{}.{}", r.schema, r.name));
        }
    }

    // Extract dependencies from views
    for view in schema.views.values() {
        let table_refs = extract_table_references(&view.query, &view.schema);
        let func_refs = extract_function_references(&view.query, &view.schema);

        for r in table_refs {
            deps.insert(format!("{}.{}", r.schema, r.name));
        }
        for r in func_refs {
            deps.insert(format!("{}.{}", r.schema, r.name));
        }
    }

    // Extract dependencies from triggers (function reference)
    for trigger in schema.triggers.values() {
        deps.insert(format!(
            "{}.{}",
            trigger.function_schema, trigger.function_name
        ));
        deps.insert(format!("{}.{}", trigger.target_schema, trigger.target_name));
    }

    deps
}

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
        let file_str = file.to_str().ok_or_else(|| {
            SchemaError::ParseError(format!("Path contains invalid UTF-8: {}", file.display()))
        })?;
        let schema = parse_sql_file(file_str)?;
        file_schemas.push((file.clone(), schema));
    }

    // Sort files topologically based on dependencies
    file_schemas = topological_sort(
        file_schemas,
        |item| item.0.to_string_lossy().to_string(),
        |item| extract_schema_dependencies(&item.1),
    )
    .map_err(|e| SchemaError::ParseError(format!("Dependency resolution failed: {e}")))?;

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

        for (name, view) in schema.views {
            if let Some(existing_path) = object_sources.get(&format!("view:{name}")) {
                return Err(SchemaError::ParseError(format!(
                    "Duplicate view \"{name}\" defined in:\n  - {}\n  - {}",
                    existing_path.display(),
                    path.display()
                )));
            }
            object_sources.insert(format!("view:{name}"), path.clone());
            merged.views.insert(name, view);
        }

        for (name, trigger) in schema.triggers {
            if let Some(existing_path) = object_sources.get(&format!("trigger:{name}")) {
                return Err(SchemaError::ParseError(format!(
                    "Duplicate trigger \"{name}\" defined in:\n  - {}\n  - {}",
                    existing_path.display(),
                    path.display()
                )));
            }
            object_sources.insert(format!("trigger:{name}"), path.clone());
            merged.triggers.insert(name, trigger);
        }

        for (name, sequence) in schema.sequences {
            if let Some(existing_path) = object_sources.get(&format!("sequence:{name}")) {
                return Err(SchemaError::ParseError(format!(
                    "Duplicate sequence \"{name}\" defined in:\n  - {}\n  - {}",
                    existing_path.display(),
                    path.display()
                )));
            }
            object_sources.insert(format!("sequence:{name}"), path.clone());
            merged.sequences.insert(name, sequence);
        }

        for (name, domain) in schema.domains {
            if let Some(existing_path) = object_sources.get(&format!("domain:{name}")) {
                return Err(SchemaError::ParseError(format!(
                    "Duplicate domain \"{name}\" defined in:\n  - {}\n  - {}",
                    existing_path.display(),
                    path.display()
                )));
            }
            object_sources.insert(format!("domain:{name}"), path.clone());
            merged.domains.insert(name, domain);
        }

        for (name, extension) in schema.extensions {
            if let Some(existing_path) = object_sources.get(&format!("extension:{name}")) {
                return Err(SchemaError::ParseError(format!(
                    "Duplicate extension \"{name}\" defined in:\n  - {}\n  - {}",
                    existing_path.display(),
                    path.display()
                )));
            }
            object_sources.insert(format!("extension:{name}"), path.clone());
            merged.extensions.insert(name, extension);
        }

        for (name, pg_schema) in schema.schemas {
            if let Some(existing_path) = object_sources.get(&format!("schema:{name}")) {
                return Err(SchemaError::ParseError(format!(
                    "Duplicate schema \"{name}\" defined in:\n  - {}\n  - {}",
                    existing_path.display(),
                    path.display()
                )));
            }
            object_sources.insert(format!("schema:{name}"), path.clone());
            merged.schemas.insert(name, pg_schema);
        }

        for (name, partition) in schema.partitions {
            if let Some(existing_path) = object_sources.get(&format!("partition:{name}")) {
                return Err(SchemaError::ParseError(format!(
                    "Duplicate partition \"{name}\" defined in:\n  - {}\n  - {}",
                    existing_path.display(),
                    path.display()
                )));
            }
            object_sources.insert(format!("partition:{name}"), path.clone());
            merged.partitions.insert(name, partition);
        }

        // Collect pending policies and owners for cross-file resolution
        merged.pending_policies.extend(schema.pending_policies);
        merged.pending_owners.extend(schema.pending_owners);
    }

    // Finalize: associate all pending policies with their tables and apply pending ownership.
    // This handles policies and ownership defined in separate files from their objects.
    merged.finalize().map_err(SchemaError::ParseError)?;

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
        let pattern_str = pattern.to_str().ok_or_else(|| {
            SchemaError::ParseError(format!(
                "Path contains invalid UTF-8: {}",
                pattern.display()
            ))
        })?;
        return resolve_glob(pattern_str);
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
                partition_by: None,

                owner: None,
                grants: Vec::new(),
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
                partition_by: None,

                owner: None,
                grants: Vec::new(),
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
                partition_by: None,

                owner: None,
                grants: Vec::new(),
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
                partition_by: None,

                owner: None,
                grants: Vec::new(),
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

                owner: None,
                grants: Vec::new(),
            },
        );

        let mut other = Schema::new();
        other.enums.insert(
            "status".to_string(),
            crate::model::EnumType {
                name: "status".to_string(),
                schema: "public".to_string(),
                values: vec!["inactive".to_string()],

                owner: None,
                grants: Vec::new(),
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
                config_params: vec![],
                owner: None,
                grants: Vec::new(),
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
                config_params: vec![],
                owner: None,
                grants: Vec::new(),
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
        assert!(schema.enums.contains_key("public.status"));
        assert_eq!(schema.tables.len(), 1);
        assert!(schema.tables.contains_key("public.users"));
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

    #[test]
    fn load_merges_triggers() {
        // Bug: triggers were not being merged from file schemas to the merged schema
        let dir = TempDir::new().unwrap();
        fs::write(
            dir.path().join("functions.sql"),
            r#"
CREATE FUNCTION auth.on_auth_user_created() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
"#,
        )
        .unwrap();
        fs::write(
            dir.path().join("triggers.sql"),
            r#"
CREATE TRIGGER "on_auth_user_created" AFTER INSERT ON "auth"."users" FOR EACH ROW EXECUTE FUNCTION "auth"."on_auth_user_created"();
"#,
        )
        .unwrap();

        let sources = vec![format!("{}/*.sql", dir.path().display())];
        let schema = load_schema_sources(&sources).unwrap();

        assert_eq!(
            schema.triggers.len(),
            1,
            "Should have loaded 1 trigger, but got triggers: {:?}",
            schema.triggers.keys().collect::<Vec<_>>()
        );
        assert!(schema
            .triggers
            .contains_key("auth.users.on_auth_user_created"));
    }

    #[test]
    fn load_merges_views() {
        let dir = TempDir::new().unwrap();
        fs::write(
            dir.path().join("tables.sql"),
            "CREATE TABLE users (id INT, name TEXT);",
        )
        .unwrap();
        fs::write(
            dir.path().join("views.sql"),
            "CREATE VIEW active_users AS SELECT id, name FROM users WHERE id > 0;",
        )
        .unwrap();

        let sources = vec![format!("{}/*.sql", dir.path().display())];
        let schema = load_schema_sources(&sources).unwrap();

        assert_eq!(schema.views.len(), 1);
        assert!(schema.views.contains_key("public.active_users"));
    }

    #[test]
    fn load_merges_extensions() {
        let dir = TempDir::new().unwrap();
        fs::write(
            dir.path().join("extensions.sql"),
            "CREATE EXTENSION pgcrypto;",
        )
        .unwrap();
        fs::write(dir.path().join("other.sql"), "CREATE EXTENSION uuid_ossp;").unwrap();

        let sources = vec![format!("{}/*.sql", dir.path().display())];
        let schema = load_schema_sources(&sources).unwrap();

        assert_eq!(schema.extensions.len(), 2);
        assert!(schema.extensions.contains_key("pgcrypto"));
        assert!(schema.extensions.contains_key("uuid_ossp"));
    }

    #[test]
    fn load_merges_domains() {
        let dir = TempDir::new().unwrap();
        fs::write(
            dir.path().join("domains.sql"),
            "CREATE DOMAIN email AS TEXT CHECK (VALUE ~ '@');",
        )
        .unwrap();

        let sources = vec![format!("{}/*.sql", dir.path().display())];
        let schema = load_schema_sources(&sources).unwrap();

        assert_eq!(schema.domains.len(), 1);
        assert!(schema.domains.contains_key("public.email"));
    }

    #[test]
    fn load_merges_sequences() {
        let dir = TempDir::new().unwrap();
        fs::write(
            dir.path().join("sequences.sql"),
            "CREATE SEQUENCE user_id_seq;",
        )
        .unwrap();

        let sources = vec![format!("{}/*.sql", dir.path().display())];
        let schema = load_schema_sources(&sources).unwrap();

        assert_eq!(schema.sequences.len(), 1);
        assert!(schema.sequences.contains_key("public.user_id_seq"));
    }

    #[test]
    fn load_merges_all_schema_types() {
        // Comprehensive test to ensure all schema types are merged
        let dir = TempDir::new().unwrap();
        fs::write(
            dir.path().join("00_schemas.sql"),
            "CREATE SCHEMA IF NOT EXISTS auth;",
        )
        .unwrap();
        fs::write(
            dir.path().join("01_extensions.sql"),
            "CREATE EXTENSION pgcrypto;",
        )
        .unwrap();
        fs::write(
            dir.path().join("02_domains.sql"),
            "CREATE DOMAIN email AS TEXT;",
        )
        .unwrap();
        fs::write(
            dir.path().join("03_enums.sql"),
            "CREATE TYPE status AS ENUM ('active', 'inactive');",
        )
        .unwrap();
        fs::write(
            dir.path().join("04_sequences.sql"),
            "CREATE SEQUENCE counter_seq;",
        )
        .unwrap();
        fs::write(
            dir.path().join("05_tables.sql"),
            "CREATE TABLE users (id INT PRIMARY KEY, email email, status status);",
        )
        .unwrap();
        fs::write(
            dir.path().join("06_functions.sql"),
            "CREATE FUNCTION my_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;",
        )
        .unwrap();
        fs::write(
            dir.path().join("07_views.sql"),
            "CREATE VIEW active_users AS SELECT id FROM users;",
        )
        .unwrap();
        fs::write(
            dir.path().join("08_triggers.sql"),
            r#"CREATE TRIGGER user_audit AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION my_fn();"#,
        )
        .unwrap();

        let sources = vec![format!("{}/*.sql", dir.path().display())];
        let schema = load_schema_sources(&sources).unwrap();

        assert_eq!(schema.schemas.len(), 1, "Should have 1 schema");
        assert!(
            schema.schemas.contains_key("auth"),
            "Should have auth schema"
        );
        assert_eq!(schema.extensions.len(), 1, "Should have 1 extension");
        assert_eq!(schema.domains.len(), 1, "Should have 1 domain");
        assert_eq!(schema.enums.len(), 1, "Should have 1 enum");
        assert_eq!(schema.sequences.len(), 1, "Should have 1 sequence");
        assert_eq!(schema.tables.len(), 1, "Should have 1 table");
        assert_eq!(schema.functions.len(), 1, "Should have 1 function");
        assert_eq!(schema.views.len(), 1, "Should have 1 view");
        assert_eq!(schema.triggers.len(), 1, "Should have 1 trigger");
    }

    #[test]
    fn load_merges_policies_across_files() {
        // Bug fix: policies in separate files should be associated with tables
        let temp = TempDir::new().unwrap();

        // Tables defined in one file
        fs::write(
            temp.path().join("tables.sql"),
            r#"
            CREATE TABLE users (id BIGINT PRIMARY KEY, role TEXT);
            ALTER TABLE users ENABLE ROW LEVEL SECURITY;
        "#,
        )
        .unwrap();

        // Policies defined in a separate file
        fs::write(
            temp.path().join("policies.sql"),
            r#"
            CREATE POLICY admin_policy ON users FOR ALL TO "authenticated" USING (true);
            CREATE POLICY user_select ON users FOR SELECT USING (id > 0);
        "#,
        )
        .unwrap();

        let sources = vec![format!("{}/*.sql", temp.path().display())];
        let schema = load_schema_sources(&sources).unwrap();

        let table = schema.tables.get("public.users").unwrap();
        assert_eq!(
            table.policies.len(),
            2,
            "Both policies from separate file should be associated with table"
        );

        let names: Vec<&str> = table.policies.iter().map(|p| p.name.as_str()).collect();
        assert!(names.contains(&"admin_policy"));
        assert!(names.contains(&"user_select"));
    }

    #[test]
    fn load_errors_on_orphan_policy_in_cross_file() {
        // Policies referencing non-existent tables should error after merge
        let temp = TempDir::new().unwrap();

        fs::write(
            temp.path().join("tables.sql"),
            r#"
            CREATE TABLE users (id BIGINT PRIMARY KEY);
        "#,
        )
        .unwrap();

        // Policy references a table that doesn't exist
        fs::write(
            temp.path().join("policies.sql"),
            r#"
            CREATE POLICY orphan_policy ON nonexistent_table FOR ALL USING (true);
        "#,
        )
        .unwrap();

        let sources = vec![format!("{}/*.sql", temp.path().display())];
        let result = load_schema_sources(&sources);
        assert!(result.is_err(), "Should error on orphan policy");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("nonexistent_table"),
            "Error should mention the missing table: {err}"
        );
    }

    #[test]
    fn cross_file_ownership_resolution() {
        let temp = TempDir::new().unwrap();

        // Table defined in one file
        fs::write(
            temp.path().join("01_tables.sql"),
            r#"
            CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);
            CREATE VIEW user_emails AS SELECT email FROM users;
            CREATE SEQUENCE user_id_seq;
            CREATE TYPE user_status AS ENUM ('active', 'inactive');
            CREATE DOMAIN email_address AS TEXT;
            CREATE FUNCTION get_user() RETURNS void LANGUAGE sql AS $$ SELECT 1; $$;
        "#,
        )
        .unwrap();

        // Ownership defined in separate file
        fs::write(
            temp.path().join("02_ownership.sql"),
            r#"
            ALTER TABLE users OWNER TO app_owner;
            ALTER VIEW user_emails OWNER TO view_owner;
            ALTER SEQUENCE user_id_seq OWNER TO seq_owner;
            ALTER TYPE user_status OWNER TO type_owner;
            ALTER DOMAIN email_address OWNER TO domain_owner;
            ALTER FUNCTION get_user() OWNER TO func_owner;
        "#,
        )
        .unwrap();

        let sources = vec![format!("{}/*.sql", temp.path().display())];
        let schema = load_schema_sources(&sources).unwrap();

        // Verify cross-file ownership was applied
        let table = schema.tables.get("public.users").unwrap();
        assert_eq!(
            table.owner,
            Some("app_owner".to_string()),
            "Table owner should be applied from separate file"
        );

        let view = schema.views.get("public.user_emails").unwrap();
        assert_eq!(
            view.owner,
            Some("view_owner".to_string()),
            "View owner should be applied from separate file"
        );

        let seq = schema.sequences.get("public.user_id_seq").unwrap();
        assert_eq!(
            seq.owner,
            Some("seq_owner".to_string()),
            "Sequence owner should be applied from separate file"
        );

        let enum_type = schema.enums.get("public.user_status").unwrap();
        assert_eq!(
            enum_type.owner,
            Some("type_owner".to_string()),
            "Enum owner should be applied from separate file"
        );

        let domain = schema.domains.get("public.email_address").unwrap();
        assert_eq!(
            domain.owner,
            Some("domain_owner".to_string()),
            "Domain owner should be applied from separate file"
        );

        let func = schema.functions.get("public.get_user()").unwrap();
        assert_eq!(
            func.owner,
            Some("func_owner".to_string()),
            "Function owner should be applied from separate file"
        );
    }

    #[test]
    fn language_sql_functions_ordered_by_dependencies() {
        // Regression test for GitHub issue:
        // LANGUAGE sql functions are validated at CREATE time, so dependencies
        // must be created first regardless of alphabetical file ordering.
        //
        // This test verifies:
        // 1. is_admin() depends on is_admin_jwt()
        // 2. Files are named to fail with alphabetical ordering:
        //    - is_admin.sql < is_admin_jwt.sql alphabetically
        //    - But is_admin_jwt() must be created FIRST
        // 3. Topological sort resolves this correctly

        let temp = TempDir::new().unwrap();

        // File that would come FIRST alphabetically
        fs::write(
            temp.path().join("is_admin.sql"),
            r#"
            CREATE OR REPLACE FUNCTION auth.is_admin() RETURNS boolean
            LANGUAGE sql
            STABLE
            AS $$
                SELECT auth.is_admin_jwt()
            $$;
        "#,
        )
        .unwrap();

        // File that would come SECOND alphabetically but must be created FIRST
        fs::write(
            temp.path().join("is_admin_jwt.sql"),
            r#"
            CREATE SCHEMA IF NOT EXISTS auth;

            CREATE OR REPLACE FUNCTION auth.is_admin_jwt() RETURNS boolean
            LANGUAGE sql
            STABLE
            AS $$
                SELECT true
            $$;
        "#,
        )
        .unwrap();

        let sources = vec![format!("{}/*.sql", temp.path().display())];
        let result = load_schema_sources(&sources);

        // Should succeed - topological sort ensures is_admin_jwt is loaded first
        assert!(
            result.is_ok(),
            "Topological sort should resolve function dependencies. Error: {:?}",
            result.err()
        );

        let schema = result.unwrap();

        // Verify both functions were loaded
        assert!(
            schema.functions.contains_key("auth.is_admin()"),
            "is_admin() should be loaded"
        );
        assert!(
            schema.functions.contains_key("auth.is_admin_jwt()"),
            "is_admin_jwt() should be loaded"
        );
    }
}
