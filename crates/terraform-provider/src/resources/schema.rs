use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tf_provider::{
    schema::{Attribute, AttributeConstraint, AttributeType, Block, Description, Schema},
    AttributePath, Diagnostics, Resource,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaResourceState {
    pub id: String,
    pub schema_file: String,
    pub database_url: Option<String>,
    pub target_schemas: Option<Vec<String>>,
    pub allow_destructive: bool,
    pub zero_downtime: bool,
    pub schema_hash: Option<String>,
    pub applied_at: Option<String>,
    pub migration_count: Option<u32>,
}

impl Default for SchemaResourceState {
    fn default() -> Self {
        Self {
            id: String::new(),
            schema_file: String::new(),
            database_url: None,
            target_schemas: None,
            allow_destructive: false,
            zero_downtime: false,
            schema_hash: None,
            applied_at: None,
            migration_count: None,
        }
    }
}

pub struct SchemaResource;

#[async_trait]
impl Resource for SchemaResource {
    type State<'a> = SchemaResourceState;
    type PrivateState<'a> = ();
    type ProviderMetaState<'a> = ();

    fn schema(&self, _diags: &mut Diagnostics) -> Option<Schema> {
        Some(Schema {
            version: 1,
            block: Block {
                description: Description::plain("Manages PostgreSQL schema declaratively"),
                attributes: [
                    ("id", Attribute {
                        description: Description::plain("Resource identifier"),
                        attr_type: AttributeType::String,
                        constraint: AttributeConstraint::Computed,
                        ..Default::default()
                    }),
                    ("schema_file", Attribute {
                        description: Description::plain("Path to SQL schema file"),
                        attr_type: AttributeType::String,
                        constraint: AttributeConstraint::Required,
                        ..Default::default()
                    }),
                    ("database_url", Attribute {
                        description: Description::plain("PostgreSQL connection URL"),
                        attr_type: AttributeType::String,
                        constraint: AttributeConstraint::Optional,
                        sensitive: true,
                        ..Default::default()
                    }),
                    ("target_schemas", Attribute {
                        description: Description::plain("PostgreSQL schemas to manage"),
                        attr_type: AttributeType::List(Box::new(AttributeType::String)),
                        constraint: AttributeConstraint::Optional,
                        ..Default::default()
                    }),
                    ("allow_destructive", Attribute {
                        description: Description::plain("Allow destructive operations"),
                        attr_type: AttributeType::Bool,
                        constraint: AttributeConstraint::Optional,
                        ..Default::default()
                    }),
                    ("zero_downtime", Attribute {
                        description: Description::plain("Use expand/contract pattern"),
                        attr_type: AttributeType::Bool,
                        constraint: AttributeConstraint::Optional,
                        ..Default::default()
                    }),
                    ("schema_hash", Attribute {
                        description: Description::plain("SHA256 hash of schema file"),
                        attr_type: AttributeType::String,
                        constraint: AttributeConstraint::Computed,
                        ..Default::default()
                    }),
                    ("applied_at", Attribute {
                        description: Description::plain("Timestamp of last migration"),
                        attr_type: AttributeType::String,
                        constraint: AttributeConstraint::Computed,
                        ..Default::default()
                    }),
                    ("migration_count", Attribute {
                        description: Description::plain("Number of operations applied"),
                        attr_type: AttributeType::Number,
                        constraint: AttributeConstraint::Computed,
                        ..Default::default()
                    }),
                ].into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
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
        _diags: &mut Diagnostics,
        proposed_state: Self::State<'a>,
        _config_state: Self::State<'a>,
        _provider_meta_state: Self::ProviderMetaState<'a>,
    ) -> Option<(Self::State<'a>, Self::PrivateState<'a>)> {
        Some((proposed_state, ()))
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
        Some((proposed_state, (), vec![]))
    }

    async fn plan_destroy<'a>(
        &self,
        _diags: &mut Diagnostics,
        _prior_state: Self::State<'a>,
        _prior_private_state: Self::PrivateState<'a>,
        _provider_meta_state: Self::ProviderMetaState<'a>,
    ) -> Option<()> {
        Some(())
    }

    async fn create<'a>(
        &self,
        _diags: &mut Diagnostics,
        planned_state: Self::State<'a>,
        _config_state: Self::State<'a>,
        _planned_private_state: Self::PrivateState<'a>,
        _provider_meta_state: Self::ProviderMetaState<'a>,
    ) -> Option<(Self::State<'a>, Self::PrivateState<'a>)> {
        Some((planned_state, ()))
    }

    async fn update<'a>(
        &self,
        _diags: &mut Diagnostics,
        _prior_state: Self::State<'a>,
        planned_state: Self::State<'a>,
        _config_state: Self::State<'a>,
        _planned_private_state: Self::PrivateState<'a>,
        _provider_meta_state: Self::ProviderMetaState<'a>,
    ) -> Option<(Self::State<'a>, Self::PrivateState<'a>)> {
        Some((planned_state, ()))
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

    #[test]
    fn schema_state_defaults_allow_destructive_false() {
        let state = SchemaResourceState::default();
        assert!(!state.allow_destructive);
    }

    #[test]
    fn schema_state_defaults_zero_downtime_false() {
        let state = SchemaResourceState::default();
        assert!(!state.zero_downtime);
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

        for name in ["database_url", "target_schemas", "allow_destructive", "zero_downtime"] {
            assert!(schema.block.attributes.contains_key(name), "missing: {name}");
        }
    }
}
