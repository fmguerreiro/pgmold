mod drizzle;

use crate::model::Schema;
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

    let mut schemas: Vec<Schema> = Vec::new();

    for source in sources {
        let schema = load_single_source(source)?;
        schemas.push(schema);
    }

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
        return Ok(schemas.into_iter().next().unwrap());
    }

    let mut merged = Schema::new();

    for schema in schemas {
        for (name, table) in schema.tables {
            if merged.tables.contains_key(&name) {
                return Err(SchemaError::ParseError(format!(
                    "Duplicate table \"{name}\" from multiple sources"
                )));
            }
            merged.tables.insert(name, table);
        }

        for (name, enum_type) in schema.enums {
            if merged.enums.contains_key(&name) {
                return Err(SchemaError::ParseError(format!(
                    "Duplicate enum \"{name}\" from multiple sources"
                )));
            }
            merged.enums.insert(name, enum_type);
        }

        for (sig, func) in schema.functions {
            if merged.functions.contains_key(&sig) {
                return Err(SchemaError::ParseError(format!(
                    "Duplicate function \"{sig}\" from multiple sources"
                )));
            }
            merged.functions.insert(sig, func);
        }

        for (name, view) in schema.views {
            if merged.views.contains_key(&name) {
                return Err(SchemaError::ParseError(format!(
                    "Duplicate view \"{name}\" from multiple sources"
                )));
            }
            merged.views.insert(name, view);
        }

        for (name, trigger) in schema.triggers {
            if merged.triggers.contains_key(&name) {
                return Err(SchemaError::ParseError(format!(
                    "Duplicate trigger \"{name}\" from multiple sources"
                )));
            }
            merged.triggers.insert(name, trigger);
        }

        for (name, sequence) in schema.sequences {
            if merged.sequences.contains_key(&name) {
                return Err(SchemaError::ParseError(format!(
                    "Duplicate sequence \"{name}\" from multiple sources"
                )));
            }
            merged.sequences.insert(name, sequence);
        }

        for (name, domain) in schema.domains {
            if merged.domains.contains_key(&name) {
                return Err(SchemaError::ParseError(format!(
                    "Duplicate domain \"{name}\" from multiple sources"
                )));
            }
            merged.domains.insert(name, domain);
        }

        for (name, extension) in schema.extensions {
            if merged.extensions.contains_key(&name) {
                return Err(SchemaError::ParseError(format!(
                    "Duplicate extension \"{name}\" from multiple sources"
                )));
            }
            merged.extensions.insert(name, extension);
        }

        for (name, pg_schema) in schema.schemas {
            if merged.schemas.contains_key(&name) {
                return Err(SchemaError::ParseError(format!(
                    "Duplicate schema \"{name}\" from multiple sources"
                )));
            }
            merged.schemas.insert(name, pg_schema);
        }

        for (name, partition) in schema.partitions {
            if merged.partitions.contains_key(&name) {
                return Err(SchemaError::ParseError(format!(
                    "Duplicate partition \"{name}\" from multiple sources"
                )));
            }
            merged.partitions.insert(name, partition);
        }

        merged.pending_policies.extend(schema.pending_policies);
        merged.pending_owners.extend(schema.pending_owners);
        merged.pending_grants.extend(schema.pending_grants);
        merged.pending_revokes.extend(schema.pending_revokes);
    }

    merged.finalize().map_err(SchemaError::ParseError)?;

    Ok(merged)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_prefix_error() {
        let result = load_schema_from_sources(&["unknown:foo.sql".to_string()]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Unknown schema source prefix"));
    }

    #[test]
    fn empty_sources_error() {
        let result = load_schema_from_sources(&[]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("No schema sources provided"));
    }
}
