use std::borrow::Cow;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tf_provider::{
    map,
    schema::{Attribute, AttributeConstraint, AttributeType, Block, Description, Schema},
    value::{Value, ValueBool, ValueEmpty, ValueList, ValueNumber, ValueString},
    AttributePath, Diagnostics, Resource,
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SchemaResourceState<'a> {
    #[serde(borrow)]
    pub id: ValueString<'a>,
    #[serde(borrow)]
    pub schema_file: ValueString<'a>,
    #[serde(borrow)]
    pub database_url: ValueString<'a>,
    #[serde(borrow)]
    pub target_schemas: ValueList<ValueString<'a>>,
    pub allow_destructive: ValueBool,
    pub zero_downtime: ValueBool,
    #[serde(borrow)]
    pub schema_hash: ValueString<'a>,
    #[serde(borrow)]
    pub applied_at: ValueString<'a>,
    pub migration_count: ValueNumber,
}

pub struct SchemaResource;

#[async_trait]
impl Resource for SchemaResource {
    type State<'a> = SchemaResourceState<'a>;
    type PrivateState<'a> = ValueEmpty;
    type ProviderMetaState<'a> = ValueEmpty;

    fn schema(&self, _diags: &mut Diagnostics) -> Option<Schema> {
        Some(Schema {
            version: 1,
            block: Block {
                version: 1,
                description: Description::plain("Manages PostgreSQL schema declaratively"),
                attributes: map! {
                    "id" => Attribute {
                        description: Description::plain("Resource identifier"),
                        attr_type: AttributeType::String,
                        constraint: AttributeConstraint::Computed,
                        ..Default::default()
                    },
                    "schema_file" => Attribute {
                        description: Description::plain("Path to SQL schema file"),
                        attr_type: AttributeType::String,
                        constraint: AttributeConstraint::Required,
                        ..Default::default()
                    },
                    "database_url" => Attribute {
                        description: Description::plain("PostgreSQL connection URL"),
                        attr_type: AttributeType::String,
                        constraint: AttributeConstraint::Optional,
                        sensitive: true,
                        ..Default::default()
                    },
                    "target_schemas" => Attribute {
                        description: Description::plain("PostgreSQL schemas to manage"),
                        attr_type: AttributeType::List(Box::new(AttributeType::String)),
                        constraint: AttributeConstraint::Optional,
                        ..Default::default()
                    },
                    "allow_destructive" => Attribute {
                        description: Description::plain("Allow destructive operations"),
                        attr_type: AttributeType::Bool,
                        constraint: AttributeConstraint::Optional,
                        ..Default::default()
                    },
                    "zero_downtime" => Attribute {
                        description: Description::plain("Use expand/contract pattern"),
                        attr_type: AttributeType::Bool,
                        constraint: AttributeConstraint::Optional,
                        ..Default::default()
                    },
                    "schema_hash" => Attribute {
                        description: Description::plain("SHA256 hash of schema file"),
                        attr_type: AttributeType::String,
                        constraint: AttributeConstraint::Computed,
                        ..Default::default()
                    },
                    "applied_at" => Attribute {
                        description: Description::plain("Timestamp of last migration"),
                        attr_type: AttributeType::String,
                        constraint: AttributeConstraint::Computed,
                        ..Default::default()
                    },
                    "migration_count" => Attribute {
                        description: Description::plain("Number of operations applied"),
                        attr_type: AttributeType::Number,
                        constraint: AttributeConstraint::Computed,
                        ..Default::default()
                    }
                },
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
        if proposed_state.database_url.is_null() {
            diags.root_error_short(
                "database_url is required (either at resource or provider level)",
            );
            return None;
        }

        let schema_file_str = proposed_state.schema_file.as_str();
        let schema_path = std::path::Path::new(schema_file_str);
        if !schema_path.exists() {
            diags.root_error_short(format!("schema_file not found: {schema_file_str}"));
            return None;
        }

        let schema_hash = match crate::util::compute_schema_hash(schema_path) {
            Ok(h) => h,
            Err(e) => {
                diags.root_error_short(format!("Failed to read schema file: {e}"));
                return None;
            }
        };

        let path_hash = crate::util::compute_path_hash(schema_path);
        let id = format!("pgmold-{}", &path_hash[..8]);

        let mut state = proposed_state;
        state.id = Value::Value(Cow::Owned(id));
        state.schema_hash = Value::Value(Cow::Owned(schema_hash));
        // Mark computed fields as Unknown during plan so Terraform knows they'll be set during apply
        state.applied_at = Value::Unknown;
        state.migration_count = Value::Unknown;

        Some((state, Default::default()))
    }

    async fn plan_update<'a>(
        &self,
        diags: &mut Diagnostics,
        _prior_state: Self::State<'a>,
        proposed_state: Self::State<'a>,
        _config_state: Self::State<'a>,
        _prior_private_state: Self::PrivateState<'a>,
        _provider_meta_state: Self::ProviderMetaState<'a>,
    ) -> Option<(Self::State<'a>, Self::PrivateState<'a>, Vec<AttributePath>)> {
        let schema_file_str = proposed_state.schema_file.as_str();
        let schema_path = std::path::Path::new(schema_file_str);
        if !schema_path.exists() {
            diags.root_error_short(format!("schema_file not found: {schema_file_str}"));
            return None;
        }

        let schema_hash = match crate::util::compute_schema_hash(schema_path) {
            Ok(h) => h,
            Err(e) => {
                diags.root_error_short(format!("Failed to read schema file: {e}"));
                return None;
            }
        };

        let path_hash = crate::util::compute_path_hash(schema_path);
        let id = format!("pgmold-{}", &path_hash[..8]);

        let mut state = proposed_state;
        state.id = Value::Value(Cow::Owned(id));
        state.schema_hash = Value::Value(Cow::Owned(schema_hash));
        // Mark computed fields as Unknown during plan so Terraform knows they'll be set during apply
        state.applied_at = Value::Unknown;
        state.migration_count = Value::Unknown;

        Some((state, Default::default(), vec![]))
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
        let db_url = planned_state.database_url.as_str();

        let connection = match pgmold::pg::connection::PgConnection::new(db_url).await {
            Ok(c) => c,
            Err(e) => {
                let sanitized = crate::util::sanitize_db_error(&format!("{e}"));
                diags.root_error_short(format!("Failed to connect to database: {sanitized}"));
                return None;
            }
        };

        let schema_file = planned_state.schema_file.as_str().to_string();
        let allow_destructive = planned_state.allow_destructive.unwrap_or(false);

        let result = match pgmold::apply::apply_migration(
            &[schema_file],
            &connection,
            pgmold::apply::ApplyOptions {
                dry_run: false,
                allow_destructive,
            },
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                diags.root_error_short(format!("Migration failed: {e}"));
                return None;
            }
        };

        if pgmold::lint::has_errors(&result.lint_results) {
            for lint in &result.lint_results {
                if lint.severity == pgmold::lint::LintSeverity::Error {
                    diags.root_error_short(lint.message.to_string());
                }
            }
            return None;
        }

        let mut state = planned_state;
        state.applied_at = Value::Value(Cow::Owned(chrono::Utc::now().to_rfc3339()));
        state.migration_count = Value::Value(result.operations.len() as i64);

        Some((state, Default::default()))
    }

    async fn update<'a>(
        &self,
        diags: &mut Diagnostics,
        _prior_state: Self::State<'a>,
        planned_state: Self::State<'a>,
        _config_state: Self::State<'a>,
        _planned_private_state: Self::PrivateState<'a>,
        _provider_meta_state: Self::ProviderMetaState<'a>,
    ) -> Option<(Self::State<'a>, Self::PrivateState<'a>)> {
        let db_url = planned_state.database_url.as_str();

        let connection = match pgmold::pg::connection::PgConnection::new(db_url).await {
            Ok(c) => c,
            Err(e) => {
                let sanitized = crate::util::sanitize_db_error(&format!("{e}"));
                diags.root_error_short(format!("Failed to connect to database: {sanitized}"));
                return None;
            }
        };

        let schema_file = planned_state.schema_file.as_str().to_string();
        let allow_destructive = planned_state.allow_destructive.unwrap_or(false);

        let result = match pgmold::apply::apply_migration(
            &[schema_file],
            &connection,
            pgmold::apply::ApplyOptions {
                dry_run: false,
                allow_destructive,
            },
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                diags.root_error_short(format!("Migration failed: {e}"));
                return None;
            }
        };

        if pgmold::lint::has_errors(&result.lint_results) {
            for lint in &result.lint_results {
                if lint.severity == pgmold::lint::LintSeverity::Error {
                    diags.root_error_short(lint.message.to_string());
                }
            }
            return None;
        }

        let mut state = planned_state;
        state.applied_at = Value::Value(Cow::Owned(chrono::Utc::now().to_rfc3339()));
        state.migration_count = Value::Value(result.operations.len() as i64);

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;
    use tf_provider::value::ValueEmpty;

    #[test]
    fn schema_state_defaults_allow_destructive_null() {
        let state = SchemaResourceState::default();
        assert!(state.allow_destructive.is_null());
    }

    #[test]
    fn schema_state_defaults_zero_downtime_null() {
        let state = SchemaResourceState::default();
        assert!(state.zero_downtime.is_null());
    }

    #[test]
    fn schema_resource_has_required_attributes() {
        let resource = SchemaResource;
        let mut diags = Diagnostics::default();
        let schema = resource.schema(&mut diags).expect("schema should exist");

        assert!(schema.block.attributes.contains_key("schema_file"));
    }

    #[test]
    fn schema_resource_has_optional_attributes() {
        let resource = SchemaResource;
        let mut diags = Diagnostics::default();
        let schema = resource.schema(&mut diags).expect("schema should exist");

        for name in [
            "database_url",
            "target_schemas",
            "allow_destructive",
            "zero_downtime",
        ] {
            assert!(
                schema.block.attributes.contains_key(name),
                "missing: {name}"
            );
        }
    }

    #[tokio::test]
    async fn plan_create_computes_schema_hash() {
        let mut schema_file = NamedTempFile::new().unwrap();
        writeln!(schema_file, "CREATE TABLE users (id INT PRIMARY KEY);").unwrap();

        let resource = SchemaResource;
        let mut diags = Diagnostics::default();

        let proposed = SchemaResourceState {
            schema_file: Value::Value(Cow::Owned(schema_file.path().to_string_lossy().to_string())),
            database_url: Value::Value(Cow::Borrowed("postgres://test")),
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
            state.schema_hash.is_value(),
            "schema_hash should be computed"
        );
        assert_eq!(state.schema_hash.as_str().len(), 64);
    }

    #[tokio::test]
    async fn plan_create_fails_without_database_url() {
        let mut schema_file = NamedTempFile::new().unwrap();
        writeln!(schema_file, "CREATE TABLE users (id INT);").unwrap();

        let resource = SchemaResource;
        let mut diags = Diagnostics::default();

        let proposed = SchemaResourceState {
            schema_file: Value::Value(Cow::Owned(schema_file.path().to_string_lossy().to_string())),
            database_url: Value::Null,
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
