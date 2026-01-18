use crate::diff::MigrationOp;
use crate::model::{versioned_schema_name, ColumnMapping, Schema, Table, VersionView};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Phase {
    Expand,
    Backfill,
    Contract,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhasedOp {
    pub phase: Phase,
    pub op: MigrationOp,
    pub rationale: String,
}

#[derive(Debug, Clone, Default)]
pub struct ExpandContractPlan {
    pub expand_ops: Vec<PhasedOp>,
    pub backfill_ops: Vec<PhasedOp>,
    pub contract_ops: Vec<PhasedOp>,
}

impl ExpandContractPlan {
    pub fn new() -> Self {
        Self::default()
    }
}

pub fn expand_operations(ops: Vec<MigrationOp>) -> ExpandContractPlan {
    let mut plan = ExpandContractPlan::new();

    for op in ops {
        match op {
            MigrationOp::AddColumn { table, column } => {
                if !column.nullable {
                    let mut nullable_column = column.clone();
                    nullable_column.nullable = true;

                    plan.expand_ops.push(PhasedOp {
                        phase: Phase::Expand,
                        op: MigrationOp::AddColumn {
                            table: table.clone(),
                            column: nullable_column,
                        },
                        rationale: format!(
                            "Add column '{}' as nullable to allow existing rows to have NULL values",
                            column.name
                        ),
                    });

                    plan.backfill_ops.push(PhasedOp {
                        phase: Phase::Backfill,
                        op: MigrationOp::BackfillHint {
                            table: table.clone(),
                            column: column.name.clone(),
                            hint: format!(
                                "UPDATE {} SET {} = <value> WHERE {} IS NULL;",
                                table, column.name, column.name
                            ),
                        },
                        rationale: format!(
                            "Backfill values for column '{}' before adding NOT NULL constraint",
                            column.name
                        ),
                    });

                    plan.contract_ops.push(PhasedOp {
                        phase: Phase::Contract,
                        op: MigrationOp::SetColumnNotNull {
                            table: table.clone(),
                            column: column.name.clone(),
                        },
                        rationale: format!(
                            "Add NOT NULL constraint to column '{}' after backfill is complete",
                            column.name
                        ),
                    });
                } else {
                    plan.expand_ops.push(PhasedOp {
                        phase: Phase::Expand,
                        op: MigrationOp::AddColumn { table, column },
                        rationale: "Add nullable column directly".to_string(),
                    });
                }
            }
            _ => {
                plan.expand_ops.push(PhasedOp {
                    phase: Phase::Expand,
                    op,
                    rationale: "Direct operation".to_string(),
                });
            }
        }
    }

    plan
}


/// Generate a VersionView for a single table.
///
/// # Important: Column Ordering
///
/// **Columns are ordered alphabetically by name** (due to BTreeMap), NOT by their
/// original table definition order. Applications MUST use explicit column lists
/// in queries (not `SELECT *`) to avoid column order dependencies.
///
/// ```sql
/// -- WRONG: Column order may differ from base table
/// SELECT * FROM public_v0001.users;
///
/// -- CORRECT: Explicit column list
/// SELECT id, name, email FROM public_v0001.users;
/// ```
///
/// # Arguments
/// * `table` - The base table to create a view for
/// * `version` - Version identifier (e.g., "v0001" or "add-email-column")
/// * `column_overrides` - Map of virtual_name -> physical_name for columns that differ
///
/// # Panics
/// Panics if the table has no columns (cannot create a view with no columns).
pub fn generate_version_view(
    table: &Table,
    version: &str,
    column_overrides: &BTreeMap<String, String>,
) -> VersionView {
    assert!(
        !table.columns.is_empty(),
        "Cannot create version view for table '{}' with no columns",
        table.name
    );

    let column_mappings: Vec<ColumnMapping> = table
        .columns
        .values()
        .map(|col| {
            let physical_name = column_overrides
                .get(&col.name)
                .cloned()
                .unwrap_or_else(|| col.name.clone());
            ColumnMapping {
                virtual_name: col.name.clone(),
                physical_name,
            }
        })
        .collect();

    VersionView {
        name: table.name.clone(),
        base_schema: table.schema.clone(),
        version_schema: versioned_schema_name(&table.schema, version),
        base_table: table.name.clone(),
        column_mappings,
        security_invoker: true,
        owner: table.owner.clone(),
    }
}

/// Generate MigrationOps to create a version schema with views for all tables.
///
/// # Arguments
/// * `schema` - The full schema containing tables
/// * `base_schema` - The schema to version (e.g., "public")
/// * `version` - Version identifier
/// * `column_overrides` - Per-table column overrides: table_name -> (virtual -> physical)
pub fn generate_version_schema_ops(
    schema: &Schema,
    base_schema: &str,
    version: &str,
    column_overrides: &BTreeMap<String, BTreeMap<String, String>>,
) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    ops.push(MigrationOp::CreateVersionSchema {
        base_schema: base_schema.to_string(),
        version: version.to_string(),
    });

    for (_qualified_name, table) in &schema.tables {
        if table.schema != base_schema {
            continue;
        }

        let table_overrides = column_overrides
            .get(&table.name)
            .cloned()
            .unwrap_or_default();

        let view = generate_version_view(table, version, &table_overrides);
        ops.push(MigrationOp::CreateVersionView { view });
    }

    ops
}

/// Generate MigrationOps to drop a version schema (and its views via CASCADE).
pub fn generate_drop_version_schema_ops(base_schema: &str, version: &str) -> Vec<MigrationOp> {
    vec![MigrationOp::DropVersionSchema {
        base_schema: base_schema.to_string(),
        version: version.to_string(),
    }]
}

/// Expand operations with version schema support for zero-downtime migrations.
///
/// This creates version views in the expand phase and drops old version schemas
/// in the contract phase.
///
/// # Arguments
/// * `ops` - Migration operations to expand
/// * `schema` - The full schema containing tables
/// * `new_version` - New version identifier to create
/// * `old_version` - Previous version to drop (if any)
/// * `base_schema` - The schema to version (e.g., "public")
pub fn expand_operations_with_versioning(
    ops: Vec<MigrationOp>,
    schema: &Schema,
    new_version: &str,
    old_version: Option<&str>,
    base_schema: &str,
) -> ExpandContractPlan {
    let mut plan = expand_operations(ops);

    let version_ops = generate_version_schema_ops(schema, base_schema, new_version, &BTreeMap::new());

    let mut version_phased: Vec<PhasedOp> = version_ops
        .into_iter()
        .map(|op| PhasedOp {
            phase: Phase::Expand,
            op,
            rationale: format!(
                "Create version schema {} for zero-downtime migration",
                new_version
            ),
        })
        .collect();

    version_phased.append(&mut plan.expand_ops);
    plan.expand_ops = version_phased;

    if let Some(old_ver) = old_version {
        let drop_ops = generate_drop_version_schema_ops(base_schema, old_ver);
        for op in drop_ops {
            plan.contract_ops.push(PhasedOp {
                phase: Phase::Contract,
                op,
                rationale: format!("Drop old version schema {} after migration complete", old_ver),
            });
        }
    }

    plan
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Column, PgType, Schema, Table};

    #[test]
    fn empty_operations_produce_empty_plan() {
        let plan = expand_operations(vec![]);
        assert!(plan.expand_ops.is_empty());
        assert!(plan.backfill_ops.is_empty());
        assert!(plan.contract_ops.is_empty());
    }

    #[test]
    fn add_not_null_column_expands_to_three_phases() {
        let column = Column {
            name: "email".to_string(),
            data_type: PgType::Text,
            nullable: false,
            default: None,
            comment: None,
        };

        let ops = vec![MigrationOp::AddColumn {
            table: "users".to_string(),
            column,
        }];

        let plan = expand_operations(ops);

        assert_eq!(plan.expand_ops.len(), 1);
        assert_eq!(plan.backfill_ops.len(), 1);
        assert_eq!(plan.contract_ops.len(), 1);

        match &plan.expand_ops[0].op {
            MigrationOp::AddColumn { table, column } => {
                assert_eq!(table, "users");
                assert_eq!(column.name, "email");
                assert!(column.nullable);
            }
            _ => panic!("Expected AddColumn in expand phase"),
        }

        match &plan.backfill_ops[0].op {
            MigrationOp::BackfillHint { table, column, .. } => {
                assert_eq!(table, "users");
                assert_eq!(column, "email");
            }
            _ => panic!("Expected BackfillHint in backfill phase"),
        }

        match &plan.contract_ops[0].op {
            MigrationOp::SetColumnNotNull { table, column } => {
                assert_eq!(table, "users");
                assert_eq!(column, "email");
            }
            _ => panic!("Expected SetColumnNotNull in contract phase"),
        }
    }

    #[test]
    fn add_nullable_column_stays_in_expand_only() {
        let column = Column {
            name: "bio".to_string(),
            data_type: PgType::Text,
            nullable: true,
            default: None,
            comment: None,
        };

        let ops = vec![MigrationOp::AddColumn {
            table: "users".to_string(),
            column,
        }];

        let plan = expand_operations(ops);

        assert_eq!(plan.expand_ops.len(), 1);
        assert_eq!(plan.backfill_ops.len(), 0);
        assert_eq!(plan.contract_ops.len(), 0);

        match &plan.expand_ops[0].op {
            MigrationOp::AddColumn { table, column } => {
                assert_eq!(table, "users");
                assert_eq!(column.name, "bio");
                assert!(column.nullable);
            }
            _ => panic!("Expected AddColumn in expand phase"),
        }
    }


    fn make_table(name: &str, schema: &str) -> Table {
        let mut columns = BTreeMap::new();
        columns.insert(
            "id".to_string(),
            Column {
                name: "id".to_string(),
                data_type: PgType::Integer,
                nullable: false,
                default: None,
                comment: None,
            },
        );
        Table {
            name: name.to_string(),
            schema: schema.to_string(),
            columns,
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
        }
    }

    #[test]
    fn generate_version_view_creates_identity_mappings() {
        let mut table = make_table("users", "public");
        table.columns.insert(
            "id".to_string(),
            Column {
                name: "id".to_string(),
                data_type: PgType::Integer,
                nullable: false,
                default: None,
                comment: None,
            },
        );
        table.columns.insert(
            "name".to_string(),
            Column {
                name: "name".to_string(),
                data_type: PgType::Text,
                nullable: true,
                default: None,
                comment: None,
            },
        );

        let view = generate_version_view(&table, "v0001", &BTreeMap::new());

        assert_eq!(view.name, "users");
        assert_eq!(view.base_schema, "public");
        assert_eq!(view.version_schema, "public_v0001");
        assert_eq!(view.base_table, "users");
        assert!(view.security_invoker);
        assert_eq!(view.column_mappings.len(), 2);
    }

    #[test]
    fn generate_version_view_uses_column_overrides() {
        let mut table = make_table("users", "public");
        table.columns.insert(
            "description".to_string(),
            Column {
                name: "description".to_string(),
                data_type: PgType::Text,
                nullable: true,
                default: None,
                comment: None,
            },
        );

        let mut overrides = BTreeMap::new();
        overrides.insert(
            "description".to_string(),
            "_pgroll_new_description".to_string(),
        );

        let view = generate_version_view(&table, "v0002", &overrides);

        // Table has 2 columns: "id" (from make_table) and "description" (added above)
        assert_eq!(view.column_mappings.len(), 2);
        // Find the description mapping (may be in any position due to BTreeMap ordering)
        let description_mapping = view
            .column_mappings
            .iter()
            .find(|m| m.virtual_name == "description")
            .expect("description mapping should exist");
        assert_eq!(description_mapping.physical_name, "_pgroll_new_description");
    }

    #[test]
    fn generate_version_schema_ops_creates_schema_and_views() {
        let mut schema = Schema::default();
        let mut table = make_table("users", "public");
        table.columns.insert(
            "id".to_string(),
            Column {
                name: "id".to_string(),
                data_type: PgType::Integer,
                nullable: false,
                default: None,
                comment: None,
            },
        );
        schema.tables.insert("public.users".to_string(), table);

        let ops = generate_version_schema_ops(&schema, "public", "v0001", &BTreeMap::new());

        assert_eq!(ops.len(), 2);
        assert!(matches!(
            &ops[0],
            MigrationOp::CreateVersionSchema {
                base_schema,
                version
            } if base_schema == "public" && version == "v0001"
        ));
        assert!(matches!(
            &ops[1],
            MigrationOp::CreateVersionView { view } if view.name == "users"
        ));
    }

    #[test]
    fn generate_version_schema_ops_filters_by_base_schema() {
        let mut schema = Schema::default();
        let public_table = make_table("users", "public");
        let other_table = make_table("logs", "audit");
        schema
            .tables
            .insert("public.users".to_string(), public_table);
        schema.tables.insert("audit.logs".to_string(), other_table);

        let ops = generate_version_schema_ops(&schema, "public", "v0001", &BTreeMap::new());

        assert_eq!(ops.len(), 2);
        assert!(matches!(
            &ops[1],
            MigrationOp::CreateVersionView { view } if view.name == "users"
        ));
    }

    #[test]
    fn generate_drop_version_schema_ops_creates_drop_op() {
        let ops = generate_drop_version_schema_ops("public", "v0001");

        assert_eq!(ops.len(), 1);
        assert!(matches!(
            &ops[0],
            MigrationOp::DropVersionSchema {
                base_schema,
                version
            } if base_schema == "public" && version == "v0001"
        ));
    }

    #[test]
    fn expand_with_versioning_prepends_version_ops() {
        let mut schema = Schema::default();
        let mut table = make_table("users", "public");
        table.columns.insert(
            "id".to_string(),
            Column {
                name: "id".to_string(),
                data_type: PgType::Integer,
                nullable: false,
                default: None,
                comment: None,
            },
        );
        schema.tables.insert("public.users".to_string(), table);

        let column = Column {
            name: "email".to_string(),
            data_type: PgType::Text,
            nullable: true,
            default: None,
            comment: None,
        };

        let ops = vec![MigrationOp::AddColumn {
            table: "public.users".to_string(),
            column,
        }];

        let plan = expand_operations_with_versioning(ops, &schema, "v0002", None, "public");

        assert!(plan.expand_ops.iter().any(|p| matches!(
            &p.op,
            MigrationOp::CreateVersionSchema { version, .. } if version == "v0002"
        )));
        assert!(plan.expand_ops.iter().any(|p| matches!(
            &p.op,
            MigrationOp::CreateVersionView { view } if view.name == "users"
        )));
    }

    #[test]
    fn expand_with_versioning_drops_old_version_in_contract() {
        let schema = Schema::default();
        let plan =
            expand_operations_with_versioning(vec![], &schema, "v0002", Some("v0001"), "public");

        assert!(plan.contract_ops.iter().any(|p| matches!(
            &p.op,
            MigrationOp::DropVersionSchema { version, .. } if version == "v0001"
        )));
    }

    #[test]
    #[should_panic(expected = "Cannot create version view for table")]
    fn generate_version_view_panics_on_empty_columns() {
        let empty_table = Table {
            name: "empty".to_string(),
            schema: "public".to_string(),
            columns: BTreeMap::new(),
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
        };
        generate_version_view(&empty_table, "v0001", &BTreeMap::new());
    }
}
