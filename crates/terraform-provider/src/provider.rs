use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tf_provider::{
    schema::{Attribute, AttributeConstraint, AttributeType, Block, Description, Schema},
    Diagnostics, DynamicResource, Provider,
};
use tokio::sync::RwLock;

use crate::resources::{MigrationResource, SchemaResource};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ProviderConfig {
    pub database_url: Option<String>,
    pub target_schemas: Option<Vec<String>>,
}

#[derive(Debug, Default, Clone)]
pub struct PgmoldProvider {
    pub config: Arc<RwLock<Option<ProviderConfig>>>,
}

#[async_trait]
impl Provider for PgmoldProvider {
    type Config<'a> = ProviderConfig;
    type MetaState<'a> = ();

    fn schema(&self, _diags: &mut Diagnostics) -> Option<Schema> {
        let mut attributes = HashMap::new();

        attributes.insert(
            "database_url".to_string(),
            Attribute {
                description: Description::plain("PostgreSQL connection URL"),
                attr_type: AttributeType::String,
                constraint: AttributeConstraint::Optional,
                sensitive: true,
                ..Default::default()
            },
        );

        attributes.insert(
            "target_schemas".to_string(),
            Attribute {
                description: Description::plain("PostgreSQL schemas to manage (default: public)"),
                attr_type: AttributeType::List(Box::new(AttributeType::String)),
                constraint: AttributeConstraint::Optional,
                ..Default::default()
            },
        );

        Some(Schema {
            version: 1,
            block: Block {
                version: 1,
                description: Description::plain("pgmold PostgreSQL schema management provider"),
                attributes,
                ..Default::default()
            },
        })
    }

    async fn configure<'a>(
        &self,
        _diags: &mut Diagnostics,
        _terraform_version: String,
        config: Self::Config<'a>,
    ) -> Option<()> {
        let mut guard = self.config.write().await;
        *guard = Some(config);
        Some(())
    }

    fn get_resources(
        &self,
        _diags: &mut Diagnostics,
    ) -> Option<HashMap<String, Box<dyn DynamicResource>>> {
        let mut resources: HashMap<String, Box<dyn DynamicResource>> = HashMap::new();
        resources.insert("schema".to_string(), Box::new(SchemaResource));
        resources.insert("migration".to_string(), Box::new(MigrationResource));
        Some(resources)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_schema_has_database_url() {
        let provider = PgmoldProvider::default();
        let mut diags = Diagnostics::default();
        let schema = provider.schema(&mut diags).expect("schema should exist");

        let attr = schema
            .block
            .attributes
            .get("database_url")
            .expect("database_url attribute should exist");

        assert!(attr.sensitive, "database_url should be sensitive");
    }

    #[test]
    fn provider_schema_has_target_schemas() {
        let provider = PgmoldProvider::default();
        let mut diags = Diagnostics::default();
        let schema = provider.schema(&mut diags).expect("schema should exist");

        let attr = schema
            .block
            .attributes
            .get("target_schemas")
            .expect("target_schemas attribute should exist");

        assert!(matches!(attr.attr_type, AttributeType::List(_)));
    }

    #[test]
    fn provider_returns_schema_resource() {
        let provider = PgmoldProvider::default();
        let mut diags = Diagnostics::default();

        let resources = provider.get_resources(&mut diags);

        assert!(resources.is_some());
        let resources = resources.unwrap();
        assert!(
            resources.contains_key("schema"),
            "should have schema resource"
        );
    }

    #[test]
    fn provider_returns_migration_resource() {
        let provider = PgmoldProvider::default();
        let mut diags = Diagnostics::default();

        let resources = provider.get_resources(&mut diags);

        assert!(resources.is_some());
        let resources = resources.unwrap();
        assert!(
            resources.contains_key("migration"),
            "should have migration resource"
        );
    }
}
