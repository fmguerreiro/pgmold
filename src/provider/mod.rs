mod drizzle;

use crate::model::{dedup_overlong_identifiers, Schema};
use crate::parser::load_schema_sources;
use crate::util::SchemaError;

pub use drizzle::load_drizzle_schema;

type Result<T> = std::result::Result<T, SchemaError>;

pub fn load_schema_from_sources(sources: &[String]) -> Result<Schema> {
    if sources.is_empty() {
        return Err(SchemaError::ParseError(
            "No schema sources provided".to_string(),
        ));
    }

    let schemas: Vec<Schema> = sources
        .iter()
        .map(|source| load_single_source(source))
        .collect::<Result<_>>()?;

    merge_schemas(schemas)
}

fn load_single_source(source: &str) -> Result<Schema> {
    if let Some(path) = source.strip_prefix("sql:") {
        load_sql_source(path)
    } else if let Some(path) = source.strip_prefix("drizzle:") {
        load_drizzle_schema(path)
    } else {
        Err(SchemaError::ParseError(format!(
            "Unknown schema source prefix: {source}. \
             Use 'sql:' for SQL files/directories or 'drizzle:' for Drizzle ORM configs."
        )))
    }
}

fn load_sql_source(path: &str) -> Result<Schema> {
    load_schema_sources(&[path.to_string()])
}

fn merge_schemas(schemas: Vec<Schema>) -> Result<Schema> {
    if schemas.is_empty() {
        return Err(SchemaError::ParseError("No schemas to merge".to_string()));
    }

    if schemas.len() == 1 {
        let mut schema = schemas.into_iter().next().unwrap();
        schema.finalize().map_err(SchemaError::ParseError)?;
        return Ok(schema);
    }

    let mut merged = Schema::new();

    for schema in schemas {
        merged.merge_from(schema, |kind, name, already_present| {
            if already_present {
                Err(SchemaError::ParseError(format!(
                    "Duplicate {kind} \"{name}\" from multiple sources"
                )))
            } else {
                Ok(())
            }
        })?;
    }

    dedup_overlong_identifiers(&mut merged.overlong_identifiers);

    merged.finalize().map_err(SchemaError::ParseError)?;

    Ok(merged)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

    fn write_sql_file(directory: &TempDir, filename: &str, content: &[u8]) -> PathBuf {
        let path = directory.path().join(filename);
        std::fs::write(&path, content).unwrap();
        path
    }

    fn sql_source(path: &Path) -> String {
        format!("sql:{}", path.display())
    }

    #[test]
    fn unknown_prefix_error() {
        let result = load_schema_from_sources(&["unknown:foo.sql".to_string()]);
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Unknown schema source prefix"));
    }

    #[test]
    fn empty_sources_error() {
        let result = load_schema_from_sources(&[]);
        let err = result.unwrap_err().to_string();
        assert!(err.contains("No schema sources provided"));
    }

    #[test]
    fn orphan_policy_errors_single_source() {
        let dir = TempDir::new().unwrap();
        let file = write_sql_file(
            &dir,
            "orphan.sql",
            b"CREATE POLICY orphan_policy ON nonexistent_table FOR ALL USING (true);",
        );

        let result = load_schema_from_sources(&[sql_source(&file)]);
        let err = result.unwrap_err().to_string();
        assert!(err.contains("nonexistent_table"));
    }

    #[test]
    fn orphan_policy_errors_at_provider_level() {
        let dir1 = TempDir::new().unwrap();
        let dir2 = TempDir::new().unwrap();

        let table_file = write_sql_file(
            &dir1,
            "tables.sql",
            b"CREATE TABLE public.users (id serial PRIMARY KEY);",
        );
        let policy_file = write_sql_file(
            &dir2,
            "policies.sql",
            b"CREATE POLICY orphan_policy ON nonexistent_table FOR ALL USING (true);",
        );

        let result = load_schema_from_sources(&[sql_source(&table_file), sql_source(&policy_file)]);
        let err = result.unwrap_err().to_string();
        assert!(err.contains("nonexistent_table"));
    }

    #[test]
    fn overlong_identifiers_merge_across_multiple_sources() {
        let dir1 = TempDir::new().unwrap();
        let dir2 = TempDir::new().unwrap();
        let long_a = "a".repeat(64);
        let long_b = "b".repeat(64);

        let file_a = write_sql_file(
            &dir1,
            "a.sql",
            format!("CREATE TABLE \"{long_a}\" (id INT);").as_bytes(),
        );
        let file_b = write_sql_file(
            &dir2,
            "b.sql",
            format!("CREATE TABLE \"{long_b}\" (id INT);").as_bytes(),
        );

        let merged = load_schema_from_sources(&[sql_source(&file_a), sql_source(&file_b)]).unwrap();

        let mut names: Vec<&str> = merged
            .overlong_identifiers
            .iter()
            .map(|o| o.name.as_str())
            .collect();
        names.sort_unstable();
        assert_eq!(names, vec![long_a.as_str(), long_b.as_str()]);
    }

    #[test]
    fn ownership_from_secondary_source_applied() {
        let dir1 = TempDir::new().unwrap();
        let dir2 = TempDir::new().unwrap();

        let table_file = write_sql_file(
            &dir1,
            "tables.sql",
            b"CREATE TABLE public.users (id serial PRIMARY KEY);",
        );
        let ownership_file = write_sql_file(
            &dir2,
            "ownership.sql",
            b"ALTER TABLE public.users OWNER TO app_user;",
        );

        let merged =
            load_schema_from_sources(&[sql_source(&table_file), sql_source(&ownership_file)])
                .unwrap();
        assert_eq!(
            merged.tables["public.users"].owner,
            Some("app_user".to_string())
        );
    }

    #[test]
    fn grant_from_secondary_source_applied() {
        let dir1 = TempDir::new().unwrap();
        let dir2 = TempDir::new().unwrap();

        let table_file = write_sql_file(
            &dir1,
            "tables.sql",
            b"CREATE TABLE public.users (id serial PRIMARY KEY);",
        );
        let grant_file = write_sql_file(
            &dir2,
            "grants.sql",
            b"GRANT SELECT, INSERT ON TABLE public.users TO readonly_user;",
        );

        let merged =
            load_schema_from_sources(&[sql_source(&table_file), sql_source(&grant_file)]).unwrap();
        let grants = &merged.tables["public.users"].grants;
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].grantee, "readonly_user");
        assert_eq!(
            grants[0].privileges,
            std::collections::BTreeSet::from([
                crate::model::Privilege::Select,
                crate::model::Privilege::Insert,
            ])
        );
    }

    #[test]
    fn revoke_from_secondary_source_applied() {
        let dir1 = TempDir::new().unwrap();
        let dir2 = TempDir::new().unwrap();

        let table_file = write_sql_file(
            &dir1,
            "tables.sql",
            b"CREATE TABLE public.users (id serial PRIMARY KEY);\n\
              GRANT SELECT, INSERT ON TABLE public.users TO app_user;",
        );
        let revoke_file = write_sql_file(
            &dir2,
            "revokes.sql",
            b"REVOKE INSERT ON TABLE public.users FROM app_user;",
        );

        let merged =
            load_schema_from_sources(&[sql_source(&table_file), sql_source(&revoke_file)]).unwrap();
        let grants = &merged.tables["public.users"].grants;
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].grantee, "app_user");
        assert_eq!(
            grants[0].privileges,
            std::collections::BTreeSet::from([crate::model::Privilege::Select])
        );
    }
}
