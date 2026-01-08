use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tf_provider::{
    schema::{Attribute, AttributeConstraint, AttributeType, Block, Description, Schema},
    value::ValueEmpty,
    AttributePath, Diagnostics, Resource,
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MigrationResourceState {
    pub id: String,
    pub schema_file: String,
    pub database_url: Option<String>,
    pub output_dir: String,
    pub prefix: Option<String>,
    pub target_schemas: Option<Vec<String>>,
    pub schema_hash: Option<String>,
    pub migration_file: Option<String>,
    pub migration_number: Option<u32>,
    pub operations: Option<Vec<String>>,
}

pub struct MigrationResource;

#[async_trait]
impl Resource for MigrationResource {
    type State<'a> = MigrationResourceState;
    type PrivateState<'a> = ValueEmpty;
    type ProviderMetaState<'a> = ValueEmpty;

    fn schema(&self, _diags: &mut Diagnostics) -> Option<Schema> {
        Some(Schema {
            version: 1,
            block: Block {
                version: 1,
                description: Description::plain("Generates numbered migration files"),
                attributes: [
                    (
                        "id",
                        Attribute {
                            description: Description::plain("Resource identifier"),
                            attr_type: AttributeType::String,
                            constraint: AttributeConstraint::Computed,
                            ..Default::default()
                        },
                    ),
                    (
                        "schema_file",
                        Attribute {
                            description: Description::plain("Path to SQL schema file"),
                            attr_type: AttributeType::String,
                            constraint: AttributeConstraint::Required,
                            ..Default::default()
                        },
                    ),
                    (
                        "database_url",
                        Attribute {
                            description: Description::plain("PostgreSQL connection URL"),
                            attr_type: AttributeType::String,
                            constraint: AttributeConstraint::Required,
                            sensitive: true,
                            ..Default::default()
                        },
                    ),
                    (
                        "output_dir",
                        Attribute {
                            description: Description::plain("Directory to write migration files"),
                            attr_type: AttributeType::String,
                            constraint: AttributeConstraint::Required,
                            ..Default::default()
                        },
                    ),
                    (
                        "prefix",
                        Attribute {
                            description: Description::plain("Optional prefix like 'V' for Flyway"),
                            attr_type: AttributeType::String,
                            constraint: AttributeConstraint::Optional,
                            ..Default::default()
                        },
                    ),
                    (
                        "target_schemas",
                        Attribute {
                            description: Description::plain(
                                "PostgreSQL schemas to introspect (default: public)",
                            ),
                            attr_type: AttributeType::List(Box::new(AttributeType::String)),
                            constraint: AttributeConstraint::Optional,
                            ..Default::default()
                        },
                    ),
                    (
                        "schema_hash",
                        Attribute {
                            description: Description::plain("SHA256 hash of schema file"),
                            attr_type: AttributeType::String,
                            constraint: AttributeConstraint::Computed,
                            ..Default::default()
                        },
                    ),
                    (
                        "migration_file",
                        Attribute {
                            description: Description::plain("Path to generated migration file"),
                            attr_type: AttributeType::String,
                            constraint: AttributeConstraint::Computed,
                            ..Default::default()
                        },
                    ),
                    (
                        "migration_number",
                        Attribute {
                            description: Description::plain("Auto-incremented migration number"),
                            attr_type: AttributeType::Number,
                            constraint: AttributeConstraint::Computed,
                            ..Default::default()
                        },
                    ),
                    (
                        "operations",
                        Attribute {
                            description: Description::plain("List of migration operations"),
                            attr_type: AttributeType::List(Box::new(AttributeType::String)),
                            constraint: AttributeConstraint::Computed,
                            ..Default::default()
                        },
                    ),
                ]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
                ..Default::default()
            },
        })
    }

    async fn read<'a>(
        &self,
        _diags: &mut Diagnostics,
        state: Self::State<'a>,
        private_state: Self::PrivateState<'a>,
        _provider_meta_state: Self::ProviderMetaState<'a>,
    ) -> Option<(Self::State<'a>, Self::PrivateState<'a>)> {
        Some((state, private_state))
    }

    async fn plan_create<'a>(
        &self,
        diags: &mut Diagnostics,
        proposed_state: Self::State<'a>,
        _config_state: Self::State<'a>,
        _provider_meta_state: Self::ProviderMetaState<'a>,
    ) -> Option<(Self::State<'a>, Self::PrivateState<'a>)> {
        if proposed_state.database_url.is_none() {
            diags.root_error_short("database_url is required");
            return None;
        }

        let schema_path = std::path::Path::new(&proposed_state.schema_file);
        if !schema_path.exists() {
            diags.root_error_short(format!(
                "schema_file not found: {}",
                proposed_state.schema_file
            ));
            return None;
        }

        let output_dir = std::path::Path::new(&proposed_state.output_dir);
        if let Some(parent) = output_dir.parent() {
            if !parent.exists() {
                diags.root_error_short(format!(
                    "output_dir parent does not exist: {}",
                    parent.display()
                ));
                return None;
            }
        }

        let schema_hash = match crate::util::compute_schema_hash(schema_path) {
            Ok(h) => h,
            Err(e) => {
                diags.root_error_short(format!("Failed to read schema file: {e}"));
                return None;
            }
        };

        let mut state = proposed_state;
        state.id = format!("pgmold-migration-{}", &schema_hash[..8]);
        state.schema_hash = Some(schema_hash);

        Some((state, Default::default()))
    }

    async fn plan_update<'a>(
        &self,
        _diags: &mut Diagnostics,
        _prior_state: Self::State<'a>,
        proposed_state: Self::State<'a>,
        _config_state: Self::State<'a>,
        _prior_private_state: Self::PrivateState<'a>,
        _provider_meta_state: Self::ProviderMetaState<'a>,
    ) -> Option<(Self::State<'a>, Self::PrivateState<'a>, Vec<AttributePath>)> {
        Some((proposed_state, Default::default(), vec![]))
    }

    async fn plan_destroy<'a>(
        &self,
        _diags: &mut Diagnostics,
        _prior_state: Self::State<'a>,
        prior_private_state: Self::PrivateState<'a>,
        _provider_meta_state: Self::ProviderMetaState<'a>,
    ) -> Option<Self::PrivateState<'a>> {
        Some(prior_private_state)
    }

    async fn create<'a>(
        &self,
        diags: &mut Diagnostics,
        planned_state: Self::State<'a>,
        _config_state: Self::State<'a>,
        _planned_private_state: Self::PrivateState<'a>,
        _provider_meta_state: Self::ProviderMetaState<'a>,
    ) -> Option<(Self::State<'a>, Self::PrivateState<'a>)> {
        let db_url = planned_state.database_url.as_ref()?;
        let output_dir = std::path::Path::new(&planned_state.output_dir);

        let connection = match pgmold::pg::connection::PgConnection::new(db_url).await {
            Ok(c) => c,
            Err(e) => {
                let sanitized = crate::util::sanitize_db_error(&format!("{e}"));
                diags.root_error_short(format!("Failed to connect to database: {sanitized}"));
                return None;
            }
        };

        let target_schemas = planned_state
            .target_schemas
            .clone()
            .unwrap_or_else(|| vec!["public".to_string()]);

        let current =
            match pgmold::pg::introspect::introspect_schema(&connection, &target_schemas, false)
                .await
            {
                Ok(s) => s,
                Err(e) => {
                    diags.root_error_short(format!("Failed to introspect database: {e}"));
                    return None;
                }
            };

        let target = match pgmold::parser::parse_sql_file(&planned_state.schema_file) {
            Ok(s) => s,
            Err(e) => {
                diags.root_error_short(format!("Failed to parse schema file: {e}"));
                return None;
            }
        };

        let operations = pgmold::diff::compute_diff(&current, &target);

        if operations.is_empty() {
            let mut state = planned_state;
            state.operations = Some(vec![]);
            return Some((state, Default::default()));
        }

        let lint_results = pgmold::lint::lint_migration_plan(
            &operations,
            &pgmold::lint::LintOptions {
                allow_destructive: false,
                is_production: false,
            },
        );

        if pgmold::lint::has_errors(&lint_results) {
            for lint in &lint_results {
                if lint.severity == pgmold::lint::LintSeverity::Error {
                    diags.root_error_short(lint.message.to_string());
                }
            }
            return None;
        }

        let migration_number =
            find_next_migration_number(output_dir, planned_state.prefix.as_deref());

        let sql = pgmold::pg::sqlgen::generate_sql(&operations);
        let op_summaries: Vec<String> = operations.iter().map(|op| format!("{op:?}")).collect();

        if let Err(e) = std::fs::create_dir_all(output_dir) {
            diags.root_error_short(format!("Failed to create output directory: {e}"));
            return None;
        }

        let prefix = planned_state.prefix.as_deref().unwrap_or("");
        let timestamp = chrono::Utc::now().format("%Y%m%d%H%M%S");
        let filename = format!("{prefix}{migration_number:04}_{timestamp}.sql");
        let filepath = output_dir.join(&filename);

        if let Err(e) = std::fs::write(&filepath, sql.join("\n")) {
            diags.root_error_short(format!("Failed to write migration file: {e}"));
            return None;
        }

        let mut state = planned_state;
        state.migration_file = Some(filepath.to_string_lossy().to_string());
        state.migration_number = Some(migration_number);
        state.operations = Some(op_summaries);

        Some((state, Default::default()))
    }

    async fn update<'a>(
        &self,
        diags: &mut Diagnostics,
        prior_state: Self::State<'a>,
        planned_state: Self::State<'a>,
        _config_state: Self::State<'a>,
        _planned_private_state: Self::PrivateState<'a>,
        _provider_meta_state: Self::ProviderMetaState<'a>,
    ) -> Option<(Self::State<'a>, Self::PrivateState<'a>)> {
        let db_url = planned_state.database_url.as_ref()?;
        let output_dir = std::path::Path::new(&planned_state.output_dir);

        let connection = match pgmold::pg::connection::PgConnection::new(db_url).await {
            Ok(c) => c,
            Err(e) => {
                let sanitized = crate::util::sanitize_db_error(&format!("{e}"));
                diags.root_error_short(format!("Failed to connect to database: {sanitized}"));
                return None;
            }
        };

        let target_schemas = planned_state
            .target_schemas
            .clone()
            .unwrap_or_else(|| vec!["public".to_string()]);

        let current =
            match pgmold::pg::introspect::introspect_schema(&connection, &target_schemas, false)
                .await
            {
                Ok(s) => s,
                Err(e) => {
                    diags.root_error_short(format!("Failed to introspect database: {e}"));
                    return None;
                }
            };

        let target = match pgmold::parser::parse_sql_file(&planned_state.schema_file) {
            Ok(s) => s,
            Err(e) => {
                diags.root_error_short(format!("Failed to parse schema file: {e}"));
                return None;
            }
        };

        let operations = pgmold::diff::compute_diff(&current, &target);

        if operations.is_empty() {
            let mut state = planned_state;
            state.operations = Some(vec![]);
            return Some((state, Default::default()));
        }

        let lint_results = pgmold::lint::lint_migration_plan(
            &operations,
            &pgmold::lint::LintOptions {
                allow_destructive: false,
                is_production: false,
            },
        );

        if pgmold::lint::has_errors(&lint_results) {
            for lint in &lint_results {
                if lint.severity == pgmold::lint::LintSeverity::Error {
                    diags.root_error_short(lint.message.to_string());
                }
            }
            return None;
        }

        if let Some(old_file) = &prior_state.migration_file {
            if std::path::Path::new(old_file).exists() {
                let _ = std::fs::remove_file(old_file);
            }
        }

        let migration_number =
            find_next_migration_number(output_dir, planned_state.prefix.as_deref());

        let sql = pgmold::pg::sqlgen::generate_sql(&operations);
        let op_summaries: Vec<String> = operations.iter().map(|op| format!("{op:?}")).collect();

        if let Err(e) = std::fs::create_dir_all(output_dir) {
            diags.root_error_short(format!("Failed to create output directory: {e}"));
            return None;
        }

        let prefix = planned_state.prefix.as_deref().unwrap_or("");
        let timestamp = chrono::Utc::now().format("%Y%m%d%H%M%S");
        let filename = format!("{prefix}{migration_number:04}_{timestamp}.sql");
        let filepath = output_dir.join(&filename);

        if let Err(e) = std::fs::write(&filepath, sql.join("\n")) {
            diags.root_error_short(format!("Failed to write migration file: {e}"));
            return None;
        }

        let mut state = planned_state;
        state.migration_file = Some(filepath.to_string_lossy().to_string());
        state.migration_number = Some(migration_number);
        state.operations = Some(op_summaries);

        Some((state, Default::default()))
    }

    async fn destroy<'a>(
        &self,
        _diags: &mut Diagnostics,
        _prior_state: Self::State<'a>,
        _prior_private_state: Self::PrivateState<'a>,
        _provider_meta_state: Self::ProviderMetaState<'a>,
    ) -> Option<()> {
        Some(())
    }
}

fn find_next_migration_number(output_dir: &std::path::Path, prefix: Option<&str>) -> u32 {
    let prefix = prefix.unwrap_or("");
    let pattern = format!(r"{}(\d{{4}})_.*\.sql$", regex::escape(prefix));
    let re = regex::Regex::new(&pattern).unwrap();

    std::fs::read_dir(output_dir)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            re.captures(&name)
                .and_then(|c| c.get(1))
                .and_then(|m| m.as_str().parse::<u32>().ok())
        })
        .max()
        .map(|n| n + 1)
        .unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn migration_state_has_default_empty_prefix() {
        let state = MigrationResourceState::default();
        assert!(state.prefix.is_none());
    }

    #[test]
    fn find_next_migration_number_empty_dir() {
        let dir = TempDir::new().unwrap();
        assert_eq!(find_next_migration_number(dir.path(), None), 1);
    }

    #[test]
    fn find_next_migration_number_with_existing() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("0001_20240101.sql"), "").unwrap();
        std::fs::write(dir.path().join("0002_20240102.sql"), "").unwrap();
        assert_eq!(find_next_migration_number(dir.path(), None), 3);
    }

    #[test]
    fn find_next_migration_number_with_prefix() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("V0001_20240101.sql"), "").unwrap();
        std::fs::write(dir.path().join("V0005_20240102.sql"), "").unwrap();
        assert_eq!(find_next_migration_number(dir.path(), Some("V")), 6);
    }

    #[test]
    fn find_next_migration_number_ignores_non_matching() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("0001_20240101.sql"), "").unwrap();
        std::fs::write(dir.path().join("README.md"), "").unwrap();
        std::fs::write(dir.path().join("schema.sql"), "").unwrap();
        assert_eq!(find_next_migration_number(dir.path(), None), 2);
    }

    #[test]
    fn find_next_migration_number_skips_gaps() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("0001_20240101.sql"), "").unwrap();
        std::fs::write(dir.path().join("0003_20240103.sql"), "").unwrap();
        std::fs::write(dir.path().join("0007_20240107.sql"), "").unwrap();
        assert_eq!(find_next_migration_number(dir.path(), None), 8);
    }

    #[tokio::test]
    async fn migration_resource_has_required_attributes() {
        let resource = MigrationResource;
        let mut diags = Diagnostics::default();
        let schema = resource.schema(&mut diags).expect("schema should exist");

        for name in ["schema_file", "database_url", "output_dir"] {
            assert!(
                schema.block.attributes.contains_key(name),
                "missing: {name}"
            );
        }
    }

    #[tokio::test]
    async fn migration_resource_has_computed_attributes() {
        let resource = MigrationResource;
        let mut diags = Diagnostics::default();
        let schema = resource.schema(&mut diags).expect("schema should exist");

        for name in [
            "id",
            "schema_hash",
            "migration_file",
            "migration_number",
            "operations",
        ] {
            assert!(
                schema.block.attributes.contains_key(name),
                "missing: {name}"
            );
        }
    }

    #[tokio::test]
    async fn plan_create_computes_schema_hash() {
        let mut schema_file = tempfile::NamedTempFile::new().unwrap();
        writeln!(schema_file, "CREATE TABLE users (id INT PRIMARY KEY);").unwrap();

        let resource = MigrationResource;
        let mut diags = Diagnostics::default();

        let proposed = MigrationResourceState {
            schema_file: schema_file.path().to_string_lossy().to_string(),
            database_url: Some("postgres://test".to_string()),
            output_dir: "/tmp/migrations".to_string(),
            ..Default::default()
        };

        let result = resource
            .plan_create(
                &mut diags,
                proposed.clone(),
                proposed,
                ValueEmpty::default(),
            )
            .await;

        assert!(result.is_some(), "plan_create should return Some");
        let (state, _) = result.unwrap();
        assert!(
            state.schema_hash.is_some(),
            "schema_hash should be computed"
        );
        assert_eq!(state.schema_hash.unwrap().len(), 64);
    }

    #[tokio::test]
    async fn plan_create_fails_without_database_url() {
        let mut schema_file = tempfile::NamedTempFile::new().unwrap();
        writeln!(schema_file, "CREATE TABLE users (id INT);").unwrap();

        let resource = MigrationResource;
        let mut diags = Diagnostics::default();

        let proposed = MigrationResourceState {
            schema_file: schema_file.path().to_string_lossy().to_string(),
            database_url: None,
            output_dir: "/tmp/migrations".to_string(),
            ..Default::default()
        };

        let result = resource
            .plan_create(
                &mut diags,
                proposed.clone(),
                proposed,
                ValueEmpty::default(),
            )
            .await;

        assert!(result.is_none() || !diags.errors.is_empty());
    }

    #[tokio::test]
    async fn plan_create_fails_with_nonexistent_schema_file() {
        let resource = MigrationResource;
        let mut diags = Diagnostics::default();

        let proposed = MigrationResourceState {
            schema_file: "/nonexistent/schema.sql".to_string(),
            database_url: Some("postgres://test".to_string()),
            output_dir: "/tmp/migrations".to_string(),
            ..Default::default()
        };

        let result = resource
            .plan_create(
                &mut diags,
                proposed.clone(),
                proposed,
                ValueEmpty::default(),
            )
            .await;

        assert!(result.is_none() || !diags.errors.is_empty());
    }
}
