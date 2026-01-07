use serde::{Deserialize, Serialize};

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
}
