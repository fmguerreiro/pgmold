use std::collections::HashSet;

use crate::diff::{compute_diff_with_flags, planner::plan_migration_checked, MigrationOp};
use crate::filter::{filter_by_target_schemas, filter_schema, Filter};
use crate::model::Schema;
use crate::pg::connection::PgConnection;
use crate::pg::introspect::introspect_schema;
use crate::provider::load_schema_from_sources;
use crate::util::{Result, SchemaError};

/// The resolved schemas and computed migration operations from a plan pass.
///
/// Carries both the ops and the filtered schemas so callers can pass them
/// directly to validation or apply steps without re-introspecting.
#[derive(Debug)]
pub struct MigrationPlan {
    pub ops: Vec<MigrationOp>,
    /// The filtered current database schema.
    pub current_schema: Schema,
    /// The filtered target (desired) schema.
    pub target_schema: Schema,
}

/// Options that control how the diff is computed.
#[derive(Debug, Default)]
pub struct PlanOptions {
    pub manage_ownership: bool,
    pub manage_grants: bool,
    pub excluded_grant_roles: HashSet<String>,
    pub include_extension_objects: bool,
    pub exclude_unmanaged_partitions: bool,
}

/// Load the desired schema from `schema_sources`, introspect the current
/// database state, apply the given `filter` and `target_schemas` constraints,
/// then compute and return the ordered migration operations.
///
/// This covers the shared sequence used by both `plan` and `apply` CLI commands.
pub async fn compute_migration_plan(
    schema_sources: &[String],
    connection: &PgConnection,
    target_schemas: &[String],
    filter: &Filter,
    options: &PlanOptions,
) -> Result<MigrationPlan> {
    let raw_target = load_schema_from_sources(schema_sources)?;
    let target_schema = filter_schema(
        &filter_by_target_schemas(&raw_target, target_schemas),
        filter,
    );

    let raw_current = introspect_schema(
        connection,
        target_schemas,
        options.include_extension_objects,
    )
    .await?;
    let current_schema = filter_schema(&raw_current, filter);
    let current_schema = if options.exclude_unmanaged_partitions {
        crate::filter::exclude_unmanaged_partitions(&current_schema, &target_schema)
    } else {
        current_schema
    };

    let ops = plan_migration_checked(compute_diff_with_flags(
        &current_schema,
        &target_schema,
        options.manage_ownership,
        options.manage_grants,
        &options.excluded_grant_roles,
    ))
    .map_err(|e| SchemaError::ValidationError(e.to_string()))?;

    Ok(MigrationPlan {
        ops,
        current_schema,
        target_schema,
    })
}

#[cfg(test)]
mod tests {
    use crate::diff::MigrationOp;

    use super::*;

    #[test]
    fn migration_plan_exposes_ops_and_schemas() {
        let plan = MigrationPlan {
            ops: vec![MigrationOp::DropTable("t".to_string())],
            current_schema: Schema::default(),
            target_schema: Schema::default(),
        };
        assert_eq!(plan.ops.len(), 1);
        assert!(matches!(plan.ops[0], MigrationOp::DropTable(_)));
    }

    #[test]
    fn plan_options_default_disables_ownership_and_grants() {
        let options = PlanOptions::default();
        assert!(!options.manage_ownership);
        assert!(!options.manage_grants);
        assert!(options.excluded_grant_roles.is_empty());
        assert!(!options.include_extension_objects);
    }
}
