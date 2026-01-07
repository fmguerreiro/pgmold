use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationResourceState {
    pub id: String,
    pub schema_file: String,
    pub database_url: Option<String>,
    pub output_dir: String,
    pub prefix: Option<String>,
    pub schema_hash: Option<String>,
    pub migration_file: Option<String>,
    pub migration_number: Option<u32>,
    pub operations: Option<Vec<String>>,
}

impl Default for MigrationResourceState {
    fn default() -> Self {
        Self {
            id: String::new(),
            schema_file: String::new(),
            database_url: None,
            output_dir: String::new(),
            prefix: None,
            schema_hash: None,
            migration_file: None,
            migration_number: None,
            operations: None,
        }
    }
}

pub struct MigrationResource;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_state_has_default_empty_prefix() {
        let state = MigrationResourceState::default();
        assert!(state.prefix.is_none());
    }
}
