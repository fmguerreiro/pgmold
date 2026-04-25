#![warn(clippy::wildcard_enum_match_arm)]
//! Enums from the upstream `sqlparser` crate (`Statement`, `ObjectType`,
//! `AlterTableOperation`, etc.) must never be matched with a bare `_ => ...`
//! wildcard. When sqlparser adds a variant we want a compile-time warning
//! forcing explicit triage, not silent data loss. See ARCHITECTURE.md §
//! "Match arm discipline".

mod comments;
mod dependencies;
mod functions;
mod grants;
mod loader;
mod ownership;
mod preprocess;
mod sequences;
mod tables;
mod unrecognized;
mod util;

#[cfg(test)]
mod tests;

pub use dependencies::{
    extract_function_references, extract_rowtype_references, extract_table_references,
    topological_sort, ObjectRef,
};
pub use loader::load_schema_sources;
pub use unrecognized::{find_unrecognized_statements, UnrecognizedStatement};

use crate::model::*;
use crate::pg::sqlgen::strip_ident_quotes;
use crate::util::{normalize_sql_whitespace, Result, SchemaError};
use sqlparser::ast::{
    AlterFunction, AlterFunctionKind, AlterFunctionOperation, AlterIndexOperation, AlterTable,
    AlterTableOperation, AlterType, AlterTypeAddValue, AlterTypeAddValuePosition,
    AlterTypeOperation, CreateAggregate, CreateAggregateOption, CreateDomain, CreateExtension,
    CreateFunction, CreateServerStatement, CreateTrigger, CreateView, DeferrableInitial,
    DropDomain, DropExtension, DropFunction, DropTrigger, FunctionParallel, ObjectType, Owner,
    RenameTableNameKind, SchemaName, Statement, TableConstraint, TriggerEvent as SqlTriggerEvent,
    TriggerPeriod, TriggerReferencingType, UserDefinedTypeRepresentation,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::fs;

use comments::parse_comment_statements;
use functions::parse_create_function;
use grants::{parse_alter_default_privileges, parse_grant_statements, parse_revoke_statements};
use ownership::parse_owner_statements;
use preprocess::preprocess_sql;
use sequences::parse_create_sequence;
use tables::{
    apply_primary_key, parse_column_with_serial, parse_create_table, parse_referential_action,
};
use util::{
    extract_qualified_name, normalize_expr, parse_data_type, parse_for_values,
    parse_for_values_required, parse_policy_command, truncate_identifier, unquote_ident,
};

pub fn parse_sql_file(path: &str) -> Result<Schema> {
    let content = fs::read_to_string(path)
        .map_err(|e| SchemaError::ParseError(format!("Failed to read file: {e}")))?;
    parse_sql_string(&content)
}

/// Returns `true` when the parser should treat unrecognized top-level
/// statements as errors instead of warnings. Controlled via the
/// `PGMOLD_STRICT` environment variable; set to `1` by the CLI's
/// `--strict` flag.
fn strict_mode_from_env() -> bool {
    matches!(std::env::var("PGMOLD_STRICT").as_deref(), Ok("1"))
}

pub fn parse_sql_string(sql: &str) -> Result<Schema> {
    parse_sql_string_with_strict(sql, strict_mode_from_env())
}

/// Parses SQL with an explicit strict flag. Callers that need deterministic
/// strict behavior (tests, library consumers that do not want to mutate
/// process-wide env vars) should prefer this over `parse_sql_string`.
pub fn parse_sql_string_with_strict(sql: &str, strict: bool) -> Result<Schema> {
    let schema = parse_sql_string_inner(sql)?;
    let unrecognized = find_unrecognized_statements(sql);
    for finding in &unrecognized {
        eprintln!("{}", finding.warning_message());
    }
    if strict && !unrecognized.is_empty() {
        let summary = unrecognized
            .iter()
            .map(|f| format!("  line {}: {}", f.line, f.snippet))
            .collect::<Vec<_>>()
            .join("\n");
        return Err(SchemaError::ParseError(format!(
            "{} unrecognized top-level statement(s) under --strict:\n{summary}",
            unrecognized.len()
        )));
    }
    Ok(schema)
}

fn parse_sql_string_inner(sql: &str) -> Result<Schema> {
    let preprocessed_sql = preprocess_sql(sql);
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, &preprocessed_sql)
        .map_err(|e| SchemaError::ParseError(format!("SQL parse error: {e}")))?;

    let mut schema = Schema::new();

    for statement in statements {
        match statement {
            Statement::CreateTable(ct) => {
                let (table_schema, table_name) = extract_qualified_name(&ct.name);

                if let Some(ref parent_table) = ct.partition_of {
                    let (parent_schema, parent_name) = extract_qualified_name(parent_table);
                    let bound = parse_for_values(&ct.for_values)?;
                    let partition = Partition {
                        schema: table_schema.clone(),
                        name: table_name.clone(),
                        parent_schema,
                        parent_name,
                        bound,
                        indexes: Vec::new(),
                        check_constraints: Vec::new(),

                        owner: None,
                    };
                    let key = qualified_name(&table_schema, &table_name);
                    schema.partitions.insert(key, partition);
                } else {
                    let parsed = parse_create_table(
                        &table_schema,
                        &table_name,
                        &ct.columns,
                        &ct.constraints,
                        ct.partition_by.as_deref(),
                    )?;
                    let key = qualified_name(&table_schema, &table_name);
                    schema.tables.insert(key, parsed.table);
                    for seq in parsed.sequences {
                        let seq_key = qualified_name(&seq.schema, &seq.name);
                        schema.sequences.insert(seq_key, seq);
                    }
                }
            }
            Statement::CreateIndex(ci) => {
                let idx_name = ci
                    .name
                    .map(|n| unquote_ident(&n.to_string()).to_string())
                    .ok_or_else(|| SchemaError::ParseError("Index must have name".into()))?;
                let (tbl_schema, tbl_name) = extract_qualified_name(&ci.table_name);
                let tbl_key = qualified_name(&tbl_schema, &tbl_name);

                if let Some(table) = schema.tables.get_mut(&tbl_key) {
                    let index_type = match ci.using {
                        Some(sqlparser::ast::IndexType::BTree) | None => IndexType::BTree,
                        Some(sqlparser::ast::IndexType::GiST) => IndexType::Gist,
                        Some(sqlparser::ast::IndexType::GIN) => IndexType::Gin,
                        Some(sqlparser::ast::IndexType::Hash) => IndexType::Hash,
                        Some(using) => panic!("unsupported index type: {using:?}"),
                    };
                    table.indexes.push(Index {
                        name: idx_name,
                        columns: ci
                            .columns
                            .iter()
                            .map(|c| unquote_ident(&c.column.expr.to_string()).to_string())
                            .collect(),
                        unique: ci.unique,
                        index_type,
                        predicate: ci.predicate.as_ref().map(|p| p.to_string()),
                        is_constraint: false,
                    });
                    table.indexes.sort();
                }
            }
            Statement::CreateType {
                name,
                representation: Some(UserDefinedTypeRepresentation::Enum { labels }),
                ..
            } => {
                let (enum_schema, enum_name) = extract_qualified_name(&name);
                let enum_type = EnumType {
                    schema: enum_schema.clone(),
                    name: enum_name.clone(),
                    values: labels
                        .iter()
                        .map(|l| l.to_string().trim_matches('\'').to_string())
                        .collect(),
                    owner: None,
                    grants: Vec::new(),
                    comment: None,
                };
                let key = qualified_name(&enum_schema, &enum_name);
                schema.enums.insert(key, enum_type);
            }
            Statement::CreatePolicy(sqlparser::ast::CreatePolicy {
                name,
                table_name,
                command,
                to,
                using,
                with_check,
                ..
            }) => {
                let (tbl_schema, tbl_name) = extract_qualified_name(&table_name);
                let policy = Policy {
                    name: unquote_ident(&name.to_string()).to_string(),
                    table_schema: tbl_schema,
                    table: tbl_name,
                    command: parse_policy_command(&command),
                    roles: {
                        let parsed_roles: Vec<String> = to
                            .iter()
                            .flat_map(|owners: &Vec<sqlparser::ast::Owner>| {
                                owners.iter().map(|o| strip_ident_quotes(&o.to_string()))
                            })
                            .collect();
                        if parsed_roles.is_empty() {
                            vec!["public".to_string()]
                        } else {
                            parsed_roles
                        }
                    },
                    using_expr: using.as_ref().map(|e: &sqlparser::ast::Expr| normalize_expr(&e.to_string())),
                    check_expr: with_check.as_ref().map(|e: &sqlparser::ast::Expr| normalize_expr(&e.to_string())),
                };
                schema.pending_policies.push(policy);
            }
            Statement::AlterTable(AlterTable {
                name, operations, ..
            }) => {
                let (tbl_schema, tbl_name) = extract_qualified_name(&name);
                let tbl_key = qualified_name(&tbl_schema, &tbl_name);
                for op in operations {
                    match op {
                        AlterTableOperation::EnableRowLevelSecurity => {
                            if let Some(table) = schema.tables.get_mut(&tbl_key) {
                                table.row_level_security = true;
                            }
                        }
                        AlterTableOperation::DisableRowLevelSecurity => {
                            if let Some(table) = schema.tables.get_mut(&tbl_key) {
                                table.row_level_security = false;
                            }
                        }
                        AlterTableOperation::ForceRowLevelSecurity => {
                            if let Some(table) = schema.tables.get_mut(&tbl_key) {
                                table.force_row_level_security = true;
                            }
                        }
                        AlterTableOperation::NoForceRowLevelSecurity => {
                            if let Some(table) = schema.tables.get_mut(&tbl_key) {
                                table.force_row_level_security = false;
                            }
                        }
                        AlterTableOperation::EnableTrigger { name: trig_name } => {
                            let key = make_trigger_key(&tbl_schema, &tbl_name, &trig_name.value);
                            if let Some(trigger) = schema.triggers.get_mut(&key) {
                                trigger.enabled = TriggerEnabled::Origin;
                            }
                        }
                        AlterTableOperation::DisableTrigger { name: trig_name } => {
                            let key = make_trigger_key(&tbl_schema, &tbl_name, &trig_name.value);
                            if let Some(trigger) = schema.triggers.get_mut(&key) {
                                trigger.enabled = TriggerEnabled::Disabled;
                            }
                        }
                        AlterTableOperation::EnableReplicaTrigger { name: trig_name } => {
                            let key = make_trigger_key(&tbl_schema, &tbl_name, &trig_name.value);
                            if let Some(trigger) = schema.triggers.get_mut(&key) {
                                trigger.enabled = TriggerEnabled::Replica;
                            }
                        }
                        AlterTableOperation::EnableAlwaysTrigger { name: trig_name } => {
                            let key = make_trigger_key(&tbl_schema, &tbl_name, &trig_name.value);
                            if let Some(trigger) = schema.triggers.get_mut(&key) {
                                trigger.enabled = TriggerEnabled::Always;
                            }
                        }
                        AlterTableOperation::AddConstraint { constraint, .. } => {
                            if let Some(table) = schema.tables.get_mut(&tbl_key) {
                                match constraint {
                                    TableConstraint::PrimaryKey(pk) => {
                                        apply_primary_key(table, &pk);
                                    }
                                    TableConstraint::ForeignKey(fk) => {
                                        let fk_name = fk
                                            .name
                                            .as_ref()
                                            .map(|n| unquote_ident(&n.to_string()).to_string())
                                            .unwrap_or_else(|| {
                                                format!(
                                                    "{}_{}_fkey",
                                                    tbl_name,
                                                    unquote_ident(&fk.columns[0].to_string())
                                                )
                                            });
                                        let (ref_schema, ref_table) =
                                            extract_qualified_name(&fk.foreign_table);
                                        table.foreign_keys.push(ForeignKey {
                                            name: truncate_identifier(&fk_name),
                                            columns: fk
                                                .columns
                                                .iter()
                                                .map(|c| unquote_ident(&c.to_string()).to_string())
                                                .collect(),
                                            referenced_schema: ref_schema,
                                            referenced_table: ref_table,
                                            referenced_columns: fk
                                                .referred_columns
                                                .iter()
                                                .map(|c| unquote_ident(&c.to_string()).to_string())
                                                .collect(),
                                            on_delete: parse_referential_action(&fk.on_delete),
                                            on_update: parse_referential_action(&fk.on_update),
                                        });
                                    }
                                    TableConstraint::Check(chk) => {
                                        let constraint_name = chk
                                            .name
                                            .as_ref()
                                            .map(|n| unquote_ident(&n.to_string()).to_string())
                                            .unwrap_or_else(|| format!("{tbl_name}_check"));

                                        table.check_constraints.push(CheckConstraint {
                                            name: constraint_name,
                                            expression: normalize_expr(&chk.expr.to_string()),
                                        });
                                        table.check_constraints.sort();
                                    }
                                    TableConstraint::Unique(uniq) => {
                                        let constraint_name = uniq
                                            .name
                                            .as_ref()
                                            .map(|n| unquote_ident(&n.to_string()).to_string())
                                            .unwrap_or_else(|| format!("{tbl_name}_unique"));

                                        table.indexes.push(Index {
                                            name: constraint_name,
                                            columns: uniq
                                                .columns
                                                .iter()
                                                .map(|c| {
                                                    unquote_ident(&c.column.expr.to_string())
                                                        .to_string()
                                                })
                                                .collect(),
                                            unique: true,
                                            index_type: IndexType::BTree,
                                            predicate: None,
                                            is_constraint: true,
                                        });
                                        table.indexes.sort();
                                    }
                                    // PostgreSQL emits `PRIMARY KEY USING INDEX <idx>` /
                                    // `UNIQUE USING INDEX <idx>` when a standalone unique index
                                    // is being promoted to a constraint. Recording the promotion
                                    // would require `Table.primary_key` (and the index model) to
                                    // carry the source index name. Until that model change lands,
                                    // fail loudly rather than silently dropping the constraint —
                                    // a silent drop would cause sqlgen to emit a CREATE TABLE
                                    // without the PK/UNIQUE, and downstream FKs targeting those
                                    // columns would fail to apply.
                                    TableConstraint::PrimaryKeyUsingIndex(pk) => {
                                        let name = pk
                                            .name
                                            .as_ref()
                                            .map(|n| unquote_ident(&n.to_string()).to_string())
                                            .unwrap_or_else(|| format!("{tbl_name}_pkey"));
                                        return Err(SchemaError::ParseError(format!(
                                            "ALTER TABLE {tbl_key} ADD CONSTRAINT {name} PRIMARY KEY USING INDEX is not yet supported"
                                        )));
                                    }
                                    TableConstraint::UniqueUsingIndex(uniq) => {
                                        let name = uniq
                                            .name
                                            .as_ref()
                                            .map(|n| unquote_ident(&n.to_string()).to_string())
                                            .unwrap_or_else(|| format!("{tbl_name}_unique"));
                                        return Err(SchemaError::ParseError(format!(
                                            "ALTER TABLE {tbl_key} ADD CONSTRAINT {name} UNIQUE USING INDEX is not yet supported"
                                        )));
                                    }
                                    // ALTER TABLE ADD CONSTRAINT does not accept EXCLUDE in
                                    // PostgreSQL in the same shape as inline EXCLUDE in CREATE
                                    // TABLE — sqlparser still surfaces the variant, so we listed
                                    // it explicitly to force an upstream review if this changes.
                                    TableConstraint::Exclusion(_)
                                    // MySQL-specific: no PostgreSQL equivalent in ALTER TABLE
                                    // ADD CONSTRAINT. Listed explicitly so adding a new
                                    // TableConstraint variant upstream forces a compile-time
                                    // review here instead of silent fallthrough.
                                    | TableConstraint::Index(_)
                                    | TableConstraint::FulltextOrSpatial(_) => {}
                                }
                            }
                        }
                        AlterTableOperation::AddColumn { column_def, .. } => {
                            if let Some(table) = schema.tables.get_mut(&tbl_key) {
                                let (column, seq_opt) =
                                    parse_column_with_serial(&tbl_schema, &tbl_name, &column_def)?;
                                table.columns.insert(column.name.clone(), column);
                                if let Some(seq) = seq_opt {
                                    let seq_key = qualified_name(&seq.schema, &seq.name);
                                    schema.sequences.insert(seq_key, seq);
                                }
                            }
                        }
                        AlterTableOperation::DropColumn { column_names, .. } => {
                            if let Some(table) = schema.tables.get_mut(&tbl_key) {
                                let names_to_drop: Vec<String> = column_names
                                    .iter()
                                    .map(|n| unquote_ident(&n.value).to_string())
                                    .collect();
                                table
                                    .columns
                                    .retain(|name, _| !names_to_drop.contains(name));
                            }
                        }
                        AlterTableOperation::RenameTable { table_name } => {
                            let new_name = match table_name {
                                RenameTableNameKind::As(obj) | RenameTableNameKind::To(obj) => {
                                    let (new_schema, new_tbl) = extract_qualified_name(&obj);
                                    let effective_schema = if obj.0.len() == 1 {
                                        tbl_schema.clone()
                                    } else {
                                        new_schema
                                    };
                                    (effective_schema, new_tbl)
                                }
                            };
                            let new_key = qualified_name(&new_name.0, &new_name.1);

                            if let Some(mut table) = schema.tables.remove(&tbl_key) {
                                table.schema = new_name.0.clone();
                                table.name = new_name.1.clone();
                                schema.tables.insert(new_key, table);
                            }
                        }
                        AlterTableOperation::RenameColumn {
                            old_column_name,
                            new_column_name,
                        } => {
                            if let Some(table) = schema.tables.get_mut(&tbl_key) {
                                let old_name = unquote_ident(&old_column_name.value).to_string();
                                let new_name = unquote_ident(&new_column_name.value).to_string();

                                if let Some(mut column) = table.columns.remove(&old_name) {
                                    column.name = new_name.clone();
                                    table.columns.insert(new_name, column);
                                }
                            }
                        }
                        AlterTableOperation::RenameConstraint { old_name, new_name } => {
                            let old_constraint_name = unquote_ident(&old_name.value).to_string();
                            let new_constraint_name = unquote_ident(&new_name.value).to_string();

                            if let Some(table) = schema.tables.get_mut(&tbl_key) {
                                for idx in &mut table.indexes {
                                    if idx.name == old_constraint_name {
                                        idx.name = new_constraint_name.clone();
                                    }
                                }

                                for fk in &mut table.foreign_keys {
                                    if fk.name == old_constraint_name {
                                        fk.name = new_constraint_name.clone();
                                    }
                                }

                                for cc in &mut table.check_constraints {
                                    if cc.name == old_constraint_name {
                                        cc.name = new_constraint_name.clone();
                                    }
                                }
                            }
                        }
                        AlterTableOperation::AttachPartitionOf {
                            partition_name,
                            partition_bound,
                        } => {
                            let (child_schema, child_name) =
                                extract_qualified_name(&partition_name);
                            let child_key = qualified_name(&child_schema, &child_name);
                            let bound = parse_for_values_required(&partition_bound)?;
                            let owner = schema.tables.remove(&child_key).and_then(|t| t.owner);
                            let partition = Partition {
                                schema: child_schema,
                                name: child_name,
                                parent_schema: tbl_schema.clone(),
                                parent_name: tbl_name.clone(),
                                bound,
                                indexes: Vec::new(),
                                check_constraints: Vec::new(),
                                owner,
                            };
                            schema.partitions.insert(child_key, partition);
                        }
                        AlterTableOperation::DetachPartitionOf {
                            partition_name,
                            concurrently: _,
                            finalize: _,
                        } => {
                            let (child_schema, child_name) =
                                extract_qualified_name(&partition_name);
                            let child_key = qualified_name(&child_schema, &child_name);
                            // TODO: PostgreSQL promotes a detached partition to a standalone
                            // table; re-insert into schema.tables to model that.
                            schema.partitions.remove(&child_key);
                        }
                        AlterTableOperation::OwnerTo { new_owner } => {
                            if let Owner::Ident(ident) = new_owner {
                                schema.pending_owners.push(PendingOwner {
                                    object_type: PendingOwnerObjectType::Table,
                                    object_key: tbl_key.clone(),
                                    owner: ident.value.clone(),
                                });
                            }
                        }
                        // PostgreSQL `ALTER TABLE` variants pgmold does not yet
                        // consume. Tracked as future work; listed explicitly
                        // so an upstream addition to `AlterTableOperation`
                        // does not silently slip past.
                        AlterTableOperation::AlterColumn { .. }
                        | AlterTableOperation::DropConstraint { .. }
                        | AlterTableOperation::ValidateConstraint { .. }
                        | AlterTableOperation::DropPrimaryKey { .. }
                        | AlterTableOperation::ReplicaIdentity { .. }
                        | AlterTableOperation::SetOptionsParens { .. }
                        | AlterTableOperation::EnableRule { .. }
                        | AlterTableOperation::DisableRule { .. }
                        | AlterTableOperation::EnableAlwaysRule { .. }
                        | AlterTableOperation::EnableReplicaRule { .. }
                        // ClickHouse-specific: projections and partition ops
                        // have no PostgreSQL equivalent.
                        | AlterTableOperation::AddProjection { .. }
                        | AlterTableOperation::DropProjection { .. }
                        | AlterTableOperation::MaterializeProjection { .. }
                        | AlterTableOperation::ClearProjection { .. }
                        | AlterTableOperation::AttachPartition { .. }
                        | AlterTableOperation::DetachPartition { .. }
                        | AlterTableOperation::FreezePartition { .. }
                        | AlterTableOperation::UnfreezePartition { .. }
                        | AlterTableOperation::AddPartitions { .. }
                        | AlterTableOperation::DropPartitions { .. }
                        | AlterTableOperation::RenamePartitions { .. }
                        // MySQL-specific: no PostgreSQL equivalent.
                        | AlterTableOperation::DropForeignKey { .. }
                        | AlterTableOperation::DropIndex { .. }
                        | AlterTableOperation::ChangeColumn { .. }
                        | AlterTableOperation::ModifyColumn { .. }
                        | AlterTableOperation::Algorithm { .. }
                        | AlterTableOperation::Lock { .. }
                        | AlterTableOperation::AutoIncrement { .. }
                        // Snowflake-specific: dynamic-table and clustering ops.
                        | AlterTableOperation::SwapWith { .. }
                        | AlterTableOperation::SetTblProperties { .. }
                        | AlterTableOperation::ClusterBy { .. }
                        | AlterTableOperation::DropClusteringKey
                        | AlterTableOperation::SuspendRecluster
                        | AlterTableOperation::ResumeRecluster
                        | AlterTableOperation::Refresh { .. }
                        | AlterTableOperation::Suspend
                        | AlterTableOperation::Resume
                        | AlterTableOperation::AlterSortKey { .. }
                        // Changing tablespace is a storage move, not a
                        // schema change — pgmold does not track tablespaces.
                        | AlterTableOperation::SetTablespace { .. } => {}
                    }
                }
            }
            Statement::AlterType(AlterType { name, operation }) => {
                let (enum_schema, enum_name) = extract_qualified_name(&name);
                let key = qualified_name(&enum_schema, &enum_name);

                match operation {
                    AlterTypeOperation::AddValue(AlterTypeAddValue {
                        if_not_exists,
                        value,
                        position,
                    }) => {
                        let new_value = value.value.clone();

                        let enum_type =
                            schema.enums.get_mut(&key).ok_or_else(|| {
                                SchemaError::ParseError(format!(
                                    "ALTER TYPE: enum '{key}' not declared in schema"
                                ))
                            })?;

                        if if_not_exists && enum_type.values.contains(&new_value) {
                            continue;
                        }

                        match position {
                            None => {
                                enum_type.values.push(new_value);
                            }
                            Some(AlterTypeAddValuePosition::Before(neighbor)) => {
                                let neighbor_value = neighbor.value.clone();
                                let pos = enum_type
                                    .values
                                    .iter()
                                    .position(|v| v == &neighbor_value)
                                    .ok_or_else(|| {
                                        SchemaError::ParseError(format!(
                                            "ALTER TYPE: value '{neighbor_value}' not found in enum '{key}'"
                                        ))
                                    })?;
                                enum_type.values.insert(pos, new_value);
                            }
                            Some(AlterTypeAddValuePosition::After(neighbor)) => {
                                let neighbor_value = neighbor.value.clone();
                                let pos = enum_type
                                    .values
                                    .iter()
                                    .position(|v| v == &neighbor_value)
                                    .ok_or_else(|| {
                                        SchemaError::ParseError(format!(
                                            "ALTER TYPE: value '{neighbor_value}' not found in enum '{key}'"
                                        ))
                                    })?;
                                enum_type.values.insert(pos + 1, new_value);
                            }
                        }
                    }
                    AlterTypeOperation::Rename(_)
                    | AlterTypeOperation::RenameValue(_)
                    // PostgreSQL ALTER TYPE shapes pgmold does not yet model. Listed
                    // explicitly so an upstream addition forces a compile-time review
                    // here instead of silent fallthrough.
                    | AlterTypeOperation::OwnerTo { .. }
                    | AlterTypeOperation::SetSchema { .. }
                    | AlterTypeOperation::AddAttribute { .. }
                    | AlterTypeOperation::DropAttribute { .. }
                    | AlterTypeOperation::AlterAttribute { .. }
                    | AlterTypeOperation::RenameAttribute { .. } => {}
                }
            }
            Statement::CreateFunction(CreateFunction {
                name,
                args,
                return_type,
                function_body,
                language,
                behavior,
                security,
                set_params,
                ..
            }) => {
                let (func_schema, func_name) = extract_qualified_name(&name);
                let func = parse_create_function(
                    &func_schema,
                    &func_name,
                    args.as_deref(),
                    return_type.as_ref(),
                    function_body.as_ref(),
                    language.as_ref(),
                    behavior.as_ref(),
                    security.as_ref(),
                    &set_params,
                )?;
                let key = qualified_name(&func_schema, &func.signature());
                schema.functions.insert(key, func);
            }
            Statement::CreateView(CreateView {
                name,
                query,
                materialized,
                ..
            }) => {
                let (view_schema, view_name) = extract_qualified_name(&name);
                let view = View {
                    schema: view_schema.clone(),
                    name: view_name.clone(),
                    query: normalize_sql_whitespace(&query.to_string()),
                    materialized,
                    owner: None,
                    grants: Vec::new(),
                    comment: None,
                };
                let key = qualified_name(&view_schema, &view_name);
                schema.views.insert(key, view);
            }
            Statement::CreateExtension(CreateExtension {
                name,
                version,
                schema: ext_schema,
                ..
            }) => {
                let ext_name = unquote_ident(&name.to_string()).to_string();
                if ext_name == "plpgsql" {
                    continue;
                }
                let ext = Extension {
                    name: ext_name.clone(),
                    version: version
                        .as_ref()
                        .map(|v| v.to_string().trim_matches('\'').to_string()),
                    schema: ext_schema
                        .as_ref()
                        .map(|s| unquote_ident(&s.to_string()).to_string()),
                };
                schema.extensions.insert(ext_name, ext);
            }
            Statement::CreateSchema { schema_name, .. } => {
                let name = match &schema_name {
                    SchemaName::Simple(obj) => unquote_ident(&obj.to_string()).to_string(),
                    SchemaName::UnnamedAuthorization(ident) => {
                        unquote_ident(&ident.to_string()).to_string()
                    }
                    SchemaName::NamedAuthorization(obj, _) => {
                        unquote_ident(&obj.to_string()).to_string()
                    }
                };
                schema.schemas.insert(
                    name.clone(),
                    PgSchema {
                        name,
                        grants: Vec::new(),
                        comment: None,
                    },
                );
            }
            Statement::CreateDomain(CreateDomain {
                name,
                data_type,
                collation,
                default,
                constraints,
            }) => {
                let (domain_schema, domain_name) = extract_qualified_name(&name);
                let pg_type = parse_data_type(&data_type)?;

                let mut not_null = false;
                let mut check_constraints = Vec::new();

                for constraint in constraints {
                    match constraint {
                        TableConstraint::Check(chk) => {
                            check_constraints.push(DomainConstraint {
                                name: chk.name.as_ref().map(|n| n.to_string()),
                                expression: normalize_expr(&chk.expr.to_string()),
                            });
                        }
                        // Every remaining variant is passed through the
                        // NOT-NULL string fallback. Listed explicitly so
                        // additions to `TableConstraint` upstream force a
                        // review here instead of being silently swallowed.
                        TableConstraint::Unique(_)
                        | TableConstraint::PrimaryKey(_)
                        | TableConstraint::ForeignKey(_)
                        | TableConstraint::Index(_)
                        | TableConstraint::FulltextOrSpatial(_)
                        | TableConstraint::Exclusion(_)
                        | TableConstraint::PrimaryKeyUsingIndex(_)
                        | TableConstraint::UniqueUsingIndex(_) => {
                            let constraint_str = constraint.to_string().to_uppercase();
                            if constraint_str.contains("NOT NULL") {
                                not_null = true;
                            }
                        }
                    }
                }

                let domain = Domain {
                    schema: domain_schema.clone(),
                    name: domain_name.clone(),
                    data_type: pg_type,
                    default: default.as_ref().map(|e| normalize_expr(&e.to_string())),
                    not_null,
                    collation: collation.as_ref().map(|c| c.to_string()),
                    check_constraints,
                    owner: None,
                    grants: Vec::new(),
                    comment: None,
                };
                let key = qualified_name(&domain_schema, &domain_name);
                schema.domains.insert(key, domain);
            }
            Statement::CreateTrigger(CreateTrigger {
                name,
                period,
                events,
                table_name,
                trigger_object,
                referencing,
                condition,
                exec_body,
                is_constraint,
                characteristics,
                ..
            }) => {
                let (tbl_schema, tbl_name) = extract_qualified_name(&table_name);
                let trigger_name = unquote_ident(&name.to_string()).to_string();
                let exec = exec_body.as_ref().ok_or_else(|| {
                    SchemaError::ParseError(format!(
                        "Trigger '{trigger_name}' missing EXECUTE clause"
                    ))
                })?;
                let func_unqualified = exec.func_name.0.len() == 1;
                let (func_schema, func_name) = extract_qualified_name(&exec.func_name);
                let func_schema = if func_unqualified && is_pg_catalog_trigger_function(&func_name)
                {
                    "pg_catalog".to_string()
                } else {
                    func_schema
                };

                let timing = match period {
                    Some(TriggerPeriod::Before) => TriggerTiming::Before,
                    Some(TriggerPeriod::After) => TriggerTiming::After,
                    Some(TriggerPeriod::InsteadOf) => TriggerTiming::InsteadOf,
                    Some(TriggerPeriod::For) => TriggerTiming::Before,
                    None => TriggerTiming::Before,
                };

                let mut trigger_events = Vec::new();
                let mut update_columns = Vec::new();

                for event in &events {
                    match event {
                        SqlTriggerEvent::Insert => {
                            trigger_events.push(TriggerEvent::Insert);
                        }
                        SqlTriggerEvent::Update(cols) => {
                            trigger_events.push(TriggerEvent::Update);
                            update_columns.extend(
                                cols.iter()
                                    .map(|c| unquote_ident(&c.to_string()).to_string()),
                            );
                        }
                        SqlTriggerEvent::Delete => {
                            trigger_events.push(TriggerEvent::Delete);
                        }
                        SqlTriggerEvent::Truncate => {
                            trigger_events.push(TriggerEvent::Truncate);
                        }
                    }
                }

                let mut old_table_name = None;
                let mut new_table_name = None;
                for tr in &referencing {
                    match tr.refer_type {
                        TriggerReferencingType::OldTable => {
                            old_table_name = Some(tr.transition_relation_name.to_string());
                        }
                        TriggerReferencingType::NewTable => {
                            new_table_name = Some(tr.transition_relation_name.to_string());
                        }
                    }
                }

                let for_each_row = trigger_object
                    .as_ref()
                    .map(|to| to.to_string().to_uppercase().contains("ROW"))
                    .unwrap_or(false);

                let when_clause = condition.as_ref().map(|e| normalize_expr(&e.to_string()));

                if timing == TriggerTiming::InsteadOf {
                    if !for_each_row {
                        return Err(SchemaError::ParseError(format!(
                            "INSTEAD OF trigger '{trigger_name}' must be FOR EACH ROW"
                        )));
                    }
                    if when_clause.is_some() {
                        return Err(SchemaError::ParseError(format!(
                            "INSTEAD OF trigger '{trigger_name}' cannot have a WHEN clause"
                        )));
                    }
                }

                if old_table_name.is_some() || new_table_name.is_some() {
                    if timing != TriggerTiming::After {
                        return Err(SchemaError::ParseError(format!(
                            "REFERENCING clause on trigger '{trigger_name}' only allowed on AFTER triggers"
                        )));
                    }

                    let has_insert = trigger_events.contains(&TriggerEvent::Insert);
                    let has_update = trigger_events.contains(&TriggerEvent::Update);
                    let has_delete = trigger_events.contains(&TriggerEvent::Delete);

                    if old_table_name.is_some() && !has_update && !has_delete {
                        return Err(SchemaError::ParseError(format!(
                            "OLD TABLE on trigger '{trigger_name}' requires UPDATE or DELETE event"
                        )));
                    }

                    if new_table_name.is_some() && !has_update && !has_insert {
                        return Err(SchemaError::ParseError(format!(
                            "NEW TABLE on trigger '{trigger_name}' requires UPDATE or INSERT event"
                        )));
                    }
                }

                let function_args = exec
                    .args
                    .as_ref()
                    .map(|args| args.iter().map(|a| a.to_string()).collect())
                    .unwrap_or_default();

                let (deferrable, initially_deferred) = match characteristics {
                    Some(c) => {
                        let deferrable = c.deferrable.unwrap_or(false);
                        let initially_deferred = matches!(c.initially, Some(DeferrableInitial::Deferred));
                        if !is_constraint && (c.deferrable.is_some() || c.initially.is_some()) {
                            return Err(SchemaError::ParseError(format!(
                                "Trigger '{trigger_name}' has DEFERRABLE/INITIALLY clause but is not a CONSTRAINT trigger"
                            )));
                        }
                        if initially_deferred && !deferrable {
                            return Err(SchemaError::ParseError(format!(
                                "Trigger '{trigger_name}' has INITIALLY DEFERRED but is NOT DEFERRABLE"
                            )));
                        }
                        (deferrable, initially_deferred)
                    }
                    None => (false, false),
                };

                if is_constraint {
                    if timing != TriggerTiming::After {
                        return Err(SchemaError::ParseError(format!(
                            "CONSTRAINT trigger '{trigger_name}' must be AFTER"
                        )));
                    }
                    if !for_each_row {
                        return Err(SchemaError::ParseError(format!(
                            "CONSTRAINT trigger '{trigger_name}' must be FOR EACH ROW"
                        )));
                    }
                }

                let trigger = Trigger {
                    name: trigger_name.clone(),
                    target_schema: tbl_schema.clone(),
                    target_name: tbl_name.clone(),
                    timing,
                    events: {
                        let mut sorted = trigger_events;
                        sorted.sort();
                        sorted
                    },
                    update_columns,
                    for_each_row,
                    when_clause,
                    function_schema: func_schema,
                    function_name: func_name,
                    function_args,
                    enabled: TriggerEnabled::Origin,
                    old_table_name,
                    new_table_name,
                    is_constraint,
                    deferrable,
                    initially_deferred,
                    comment: None,
                };

                let key = format!("{tbl_schema}.{tbl_name}.{trigger_name}");
                schema.triggers.insert(key, trigger);
            }
            Statement::CreateSequence {
                name,
                data_type,
                sequence_options,
                owned_by,
                ..
            } => {
                let (seq_schema, seq_name) = extract_qualified_name(&name);
                let sequence = parse_create_sequence(
                    &seq_schema,
                    &seq_name,
                    data_type.as_ref(),
                    &sequence_options,
                    owned_by.as_ref(),
                )?;
                let key = qualified_name(&seq_schema, &seq_name);
                schema.sequences.insert(key, sequence);
            }
            Statement::Drop {
                object_type, names, ..
            } => {
                for name in names {
                    let (obj_schema, obj_name) = extract_qualified_name(&name);
                    let key = qualified_name(&obj_schema, &obj_name);

                    match object_type {
                        ObjectType::Table => {
                            schema.tables.remove(&key);
                            schema.partitions.remove(&key);
                        }
                        ObjectType::View | ObjectType::MaterializedView => {
                            schema.views.remove(&key);
                        }
                        ObjectType::Sequence => {
                            schema.sequences.remove(&key);
                        }
                        ObjectType::Schema => {
                            schema.schemas.remove(&obj_name);
                        }
                        ObjectType::Type => {
                            schema.enums.remove(&key);
                        }
                        ObjectType::Index => {
                            for table in schema.tables.values_mut() {
                                table.indexes.retain(|idx| idx.name != obj_name);
                            }
                            for partition in schema.partitions.values_mut() {
                                partition.indexes.retain(|idx| idx.name != obj_name);
                            }
                        }
                        // Cluster-level objects (Database, Role, User) and
                        // Snowflake-specific objects (Stage, Stream) are not
                        // part of the schema model pgmold tracks. Listed
                        // explicitly so an upstream `ObjectType` addition
                        // forces a compile-time review.
                        ObjectType::Database
                        | ObjectType::Role
                        | ObjectType::User
                        | ObjectType::Stage
                        | ObjectType::Stream
                        | ObjectType::Collation => {}
                    }
                }
            }
            Statement::DropFunction(DropFunction { func_desc, .. }) => {
                for desc in func_desc {
                    let (func_schema, func_name) = extract_qualified_name(&desc.name);
                    let args_str = desc
                        .args
                        .as_ref()
                        .map(|args| {
                            args.iter()
                                .map(|a| {
                                    let type_str = a.data_type.to_string();
                                    normalize_pg_type(&type_str).into_owned()
                                })
                                .collect::<Vec<_>>()
                                .join(", ")
                        })
                        .unwrap_or_default();
                    let signature = format!("{func_name}({args_str})");
                    let key = qualified_name(&func_schema, &signature);
                    schema.functions.remove(&key);
                }
            }
            Statement::DropDomain(DropDomain { name, .. }) => {
                let (domain_schema, domain_name) = extract_qualified_name(&name);
                let key = qualified_name(&domain_schema, &domain_name);
                schema.domains.remove(&key);
            }
            Statement::DropTrigger(DropTrigger {
                trigger_name,
                table_name: Some(ref tbl),
                ..
            }) => {
                let (tbl_schema, tbl_name) = extract_qualified_name(tbl);
                let trigger_key = format!(
                    "{}.{}.{}",
                    tbl_schema,
                    tbl_name,
                    unquote_ident(&trigger_name.to_string())
                );
                schema.triggers.remove(&trigger_key);
            }
            Statement::DropTrigger(DropTrigger { .. }) => {}
            Statement::DropPolicy(sqlparser::ast::DropPolicy {
                name, table_name, ..
            }) => {
                let (tbl_schema, tbl_name) = extract_qualified_name(&table_name);
                let tbl_key = qualified_name(&tbl_schema, &tbl_name);
                let policy_name = unquote_ident(&name.to_string()).to_string();

                if let Some(table) = schema.tables.get_mut(&tbl_key) {
                    table.policies.retain(|p| p.name != policy_name);
                }
                schema.pending_policies.retain(|p| {
                    !(p.table_schema == tbl_schema && p.table == tbl_name && p.name == policy_name)
                });
            }
            Statement::DropExtension(DropExtension { names, .. }) => {
                for name in names {
                    let ext_name = unquote_ident(&name.to_string()).to_string();
                    if ext_name == "plpgsql" {
                        continue;
                    }
                    schema.extensions.remove(&ext_name);
                }
            }
            // Non-DDL and dialect-specific statements that pgmold does not
            // model. Listed explicitly (instead of a bare `_`) so adding a
            // new `Statement` variant upstream triggers a clippy warning and
            // forces triage. See ARCHITECTURE.md § "Match arm discipline".

            // Data-manipulation and query statements.
            Statement::Query(_)
            | Statement::Insert(_)
            | Statement::Update(_)
            | Statement::Delete(_)
            | Statement::Merge(_)
            | Statement::Truncate(_)
            | Statement::Copy { .. }
            | Statement::CopyIntoSnowflake { .. }
            // Session / transaction control.
            | Statement::Set(_)
            | Statement::Commit { .. }
            | Statement::Rollback { .. }
            | Statement::StartTransaction { .. }
            | Statement::Savepoint { .. }
            | Statement::ReleaseSavepoint { .. }
            | Statement::Discard { .. }
            | Statement::Use(_)
            | Statement::AlterSession { .. }
            | Statement::Reset(_)
            // PL/pgSQL control-flow statements (parsed inside function bodies
            // via a separate code path; ignored at the top level).
            | Statement::Case(_)
            | Statement::If(_)
            | Statement::While(_)
            | Statement::Raise(_)
            | Statement::Return(_)
            | Statement::Declare { .. }
            | Statement::Fetch { .. }
            | Statement::Open(_)
            | Statement::Close { .. }
            | Statement::Call(_)
            | Statement::Assert { .. }
            | Statement::Print(_)
            // Prepared-statement plumbing.
            | Statement::Prepare { .. }
            | Statement::Execute { .. }
            | Statement::Deallocate { .. }
            // Introspection / SHOW commands.
            | Statement::ShowFunctions { .. }
            | Statement::ShowVariable { .. }
            | Statement::ShowStatus { .. }
            | Statement::ShowVariables { .. }
            | Statement::ShowCreate { .. }
            | Statement::ShowColumns { .. }
            | Statement::ShowDatabases { .. }
            | Statement::ShowSchemas { .. }
            | Statement::ShowCharset(_)
            | Statement::ShowObjects(_)
            | Statement::ShowTables { .. }
            | Statement::ShowViews { .. }
            | Statement::ShowCollation { .. }
            | Statement::Explain { .. }
            | Statement::ExplainTable { .. }
            // Authorization (handled via separate `parse_grant_statements`
            // pass that re-parses the raw SQL, so the AST-level variants are
            // intentionally ignored here).
            | Statement::Grant { .. }
            | Statement::Revoke { .. }
            | Statement::Deny(_)
            // ALTER DEFAULT PRIVILEGES — handled via `parse_alter_default_privileges`
            // on the raw SQL pass, same as Grant/Revoke. AST-level variant ignored.
            | Statement::AlterDefaultPrivileges(_)
            // Comments are processed by `parse_comment_statements` on the
            // raw SQL below; ignore the AST-level variant here.
            | Statement::Comment { .. }
            // Cluster-level and role / user management — not part of the
            // schema model pgmold tracks.
            | Statement::CreateRole(_)
            | Statement::AlterRole { .. }
            | Statement::CreateUser(_)
            | Statement::AlterUser(_)
            | Statement::CreateDatabase { .. }
            | Statement::AttachDatabase { .. }
            // Procedures, macros, operators, aggregates, text search,
            // foreign tables / FDWs, domains-as-alter — pgmold does not yet
            // model these. Parse-through keeps the rest of the schema valid;
            // deeper modelling is tracked as future work.
            | Statement::CreateProcedure { .. }
            | Statement::DropProcedure { .. }
            | Statement::CreateMacro { .. }
            | Statement::CreateOperator(_)
            | Statement::CreateOperatorClass(_)
            | Statement::CreateOperatorFamily(_)
            | Statement::AlterOperator(_)
            | Statement::DropOperator(_)
            | Statement::DropOperatorClass(_)
            | Statement::DropOperatorFamily(_)
            | Statement::CreateTextSearchConfiguration(_)
            | Statement::CreateTextSearchDictionary(_)
            | Statement::CreateTextSearchParser(_)
            | Statement::CreateTextSearchTemplate(_)
            | Statement::CreateForeignTable(_)
            | Statement::CreateForeignDataWrapper(_)
            | Statement::CreatePublication(_)
            | Statement::CreateSubscription(_)
            | Statement::AlterDomain(_)
            | Statement::AlterTrigger(_)
            | Statement::AlterExtension(_)
            | Statement::CreateCast(_)
            | Statement::CreateConversion(_)
            | Statement::CreateLanguage(_)
            | Statement::CreateRule(_)
            | Statement::CreateStatistics(_)
            | Statement::CreateAccessMethod(_)
            | Statement::CreateEventTrigger(_)
            | Statement::CreateTransform(_)
            | Statement::SecurityLabel(_)
            | Statement::CreateUserMapping(_)
            | Statement::CreateTablespace(_) => {}
            Statement::AlterIndex { name, operation } => {
                let (idx_schema, idx_name) = extract_qualified_name(&name);
                match operation {
                    AlterIndexOperation::RenameIndex { index_name } => {
                        let (_, new_name) = extract_qualified_name(&index_name);
                        let mut found = false;
                        for table in schema.tables.values_mut() {
                            for idx in &mut table.indexes {
                                if idx.name == idx_name {
                                    idx.name = new_name.clone();
                                    found = true;
                                }
                            }
                        }
                        if !found {
                            for partition in schema.partitions.values_mut() {
                                for idx in &mut partition.indexes {
                                    if idx.name == idx_name {
                                        idx.name = new_name.clone();
                                        found = true;
                                    }
                                }
                            }
                        }
                        if !found {
                            return Err(SchemaError::ParseError(format!(
                                "ALTER INDEX: index '{}.{}' not declared in schema",
                                idx_schema, idx_name
                            )));
                        }
                    }
                    // Changing tablespace is a storage move, not a schema
                    // change — pgmold does not track tablespaces.
                    AlterIndexOperation::SetTablespace { .. } => {}
                }
            }
            // pgmold consumes `AlterTable` (above) but not these sibling
            // ALTER variants yet.
            Statement::AlterView { .. }
            | Statement::AlterSchema(_)
            | Statement::AlterPolicy { .. }
            | Statement::RenameTable(_)
            // Maintenance ops (VACUUM / ANALYZE / LOCK TABLE, etc.).
            | Statement::Analyze(_)
            | Statement::Vacuum(_)
            | Statement::OptimizeTable { .. }
            | Statement::LockTables { .. }
            | Statement::UnlockTables
            | Statement::Flush { .. }
            | Statement::Cache { .. }
            | Statement::UNCache { .. }
            // LISTEN / NOTIFY — runtime messaging, not schema.
            | Statement::LISTEN { .. }
            | Statement::UNLISTEN { .. }
            | Statement::NOTIFY { .. }
            // Data loading / export.
            | Statement::Load { .. }
            | Statement::LoadData { .. }
            | Statement::Install { .. }
            | Statement::Directory { .. }
            | Statement::Unload { .. }
            | Statement::ExportData(_)
            | Statement::Msck(_)
            // Error-raising variants from dialect extensions.
            | Statement::RaisError { .. }
            | Statement::Kill { .. }
            // Dialect-specific connectors, secrets, servers, stages, virtual
            // tables, and DuckDB / Snowflake / ClickHouse / MSSQL-specific
            // plumbing.
            | Statement::CreateVirtualTable { .. }
            | Statement::CreateSecret { .. }
            | Statement::DropSecret { .. }
            | Statement::CreateConnector(_)
            | Statement::AlterConnector { .. }
            | Statement::DropConnector { .. }
            | Statement::AttachDuckDBDatabase { .. }
            | Statement::DetachDuckDBDatabase { .. }
            | Statement::CreateStage { .. }
            | Statement::List(_)
            | Statement::Remove(_)
            | Statement::Pragma { .. }
            // `CreateType` representations other than `Enum` (Composite /
            // Range / SqlDefinition, and the bare `representation: None`
            // form) are currently dropped. This is a real pgmold gap tracked
            // separately; the previously wildcarded behaviour is preserved
            // here.
            | Statement::CreateType { .. }
            | Statement::AlterCollation(_)
            | Statement::AlterOperatorFamily(_)
            | Statement::AlterOperatorClass(_)
            | Statement::CreateCollation(_)
            | Statement::ShowCatalogs { .. }
            | Statement::ShowProcessList { .. }
            | Statement::Lock(_)
            | Statement::Throw(_)
            | Statement::WaitFor(_) => {}
            Statement::CreateServer(stmt) => {
                parse_create_server(stmt, &mut schema);
            }
            Statement::CreateAggregate(stmt) => {
                parse_create_aggregate(stmt, &mut schema)?;
            }
            Statement::AlterFunction(alter) => {
                parse_alter_aggregate_owner(alter, &mut schema);
            }
        }
    }

    parse_owner_statements(sql, &mut schema);
    parse_grant_statements(sql, &mut schema)?;
    parse_revoke_statements(sql, &mut schema)?;
    parse_alter_default_privileges(sql, &mut schema)?;
    parse_comment_statements(sql, &mut schema);

    schema.pending_policies = schema.finalize_partial();

    Ok(schema)
}

/// Returns `true` when `name` refers to a built-in `pg_catalog` trigger
/// helper. PostgreSQL resolves unqualified trigger function names via
/// `search_path`, which always includes `pg_catalog`; emitting these under
/// `public` produces invalid DDL (the function does not live there), so the
/// parser records them under their actual schema.
fn is_pg_catalog_trigger_function(name: &str) -> bool {
    matches!(
        name,
        "tsvector_update_trigger"
            | "tsvector_update_trigger_column"
            | "suppress_redundant_updates_trigger"
    )
}

fn parse_create_aggregate(stmt: CreateAggregate, schema: &mut Schema) -> Result<()> {
    let (agg_schema, agg_name) = extract_qualified_name(&stmt.name);
    let args: Vec<String> = stmt
        .args
        .iter()
        .map(|dt| crate::model::normalize_pg_type(&dt.to_string()).into_owned())
        .collect();

    let mut sfunc_schema: Option<String> = None;
    let mut sfunc_name: Option<String> = None;
    let mut stype: Option<String> = None;
    let mut finalfunc_schema: Option<String> = None;
    let mut finalfunc_name: Option<String> = None;
    let mut initcond: Option<String> = None;
    let mut parallel: Option<AggregateParallel> = None;

    for option in stmt.options {
        match option {
            CreateAggregateOption::Sfunc(name) => {
                let (s, n) = extract_qualified_name(&name);
                sfunc_schema = Some(s);
                sfunc_name = Some(n);
            }
            CreateAggregateOption::Stype(data_type) => {
                stype = Some(crate::model::normalize_pg_type(&data_type.to_string()).into_owned());
            }
            CreateAggregateOption::Finalfunc(name) => {
                let (s, n) = extract_qualified_name(&name);
                finalfunc_schema = Some(s);
                finalfunc_name = Some(n);
            }
            CreateAggregateOption::Initcond(value) => {
                initcond = Some(value.to_string().trim_matches('\'').to_string());
            }
            CreateAggregateOption::Parallel(p) => {
                parallel = match p {
                    FunctionParallel::Safe => Some(AggregateParallel::Safe),
                    FunctionParallel::Restricted => Some(AggregateParallel::Restricted),
                    // PARALLEL = UNSAFE is the PostgreSQL default; drop to keep parse and introspect aligned.
                    FunctionParallel::Unsafe => None,
                };
            }
            CreateAggregateOption::Sspace(_)
            | CreateAggregateOption::FinalfuncExtra
            | CreateAggregateOption::FinalfuncModify(_)
            | CreateAggregateOption::Combinefunc(_)
            | CreateAggregateOption::Serialfunc(_)
            | CreateAggregateOption::Deserialfunc(_)
            | CreateAggregateOption::Msfunc(_)
            | CreateAggregateOption::Minvfunc(_)
            | CreateAggregateOption::Mstype(_)
            | CreateAggregateOption::Msspace(_)
            | CreateAggregateOption::Mfinalfunc(_)
            | CreateAggregateOption::MfinalfuncExtra
            | CreateAggregateOption::MfinalfuncModify(_)
            | CreateAggregateOption::Minitcond(_)
            | CreateAggregateOption::Sortop(_)
            | CreateAggregateOption::Hypothetical => {}
        }
    }

    let sfunc_schema = sfunc_schema.ok_or_else(|| {
        SchemaError::ParseError(format!(
            "CREATE AGGREGATE {agg_schema}.{agg_name} missing required SFUNC"
        ))
    })?;
    let sfunc_name = sfunc_name.unwrap();
    let stype = stype.ok_or_else(|| {
        SchemaError::ParseError(format!(
            "CREATE AGGREGATE {agg_schema}.{agg_name} missing required STYPE"
        ))
    })?;

    let aggregate = Aggregate {
        schema: agg_schema.clone(),
        name: agg_name.clone(),
        args,
        sfunc_schema,
        sfunc_name,
        stype,
        finalfunc_schema,
        finalfunc_name,
        initcond,
        parallel,
        owner: None,
        grants: Vec::new(),
        comment: None,
    };

    let key = qualified_name(&agg_schema, &aggregate.signature());
    schema.aggregates.insert(key, aggregate);
    Ok(())
}

fn parse_alter_aggregate_owner(alter: AlterFunction, schema: &mut Schema) {
    if !matches!(alter.kind, AlterFunctionKind::Aggregate) {
        return;
    }
    let AlterFunctionOperation::OwnerTo(owner) = alter.operation else {
        return;
    };
    let owner_name = match owner {
        Owner::Ident(ident) => ident.value,
        Owner::CurrentRole | Owner::CurrentUser | Owner::SessionUser => return,
    };

    let (agg_schema, agg_name) = extract_qualified_name(&alter.function.name);
    let args_sig = if alter.aggregate_star {
        String::new()
    } else {
        alter
            .function
            .args
            .unwrap_or_default()
            .iter()
            .map(|a| crate::model::normalize_pg_type(&a.data_type.to_string()).into_owned())
            .collect::<Vec<_>>()
            .join(", ")
    };
    let signature = format!("{agg_name}({args_sig})");
    let object_key = qualified_name(&agg_schema, &signature);

    schema.pending_owners.push(PendingOwner {
        object_type: PendingOwnerObjectType::Aggregate,
        object_key,
        owner: owner_name,
    });
}

fn parse_create_server(stmt: CreateServerStatement, schema: &mut Schema) {
    let name = stmt.name.to_string();
    let name = name.trim_matches('"').to_string();
    let foreign_data_wrapper = stmt.foreign_data_wrapper.to_string();
    let foreign_data_wrapper = foreign_data_wrapper.trim_matches('"').to_string();
    let server_type = stmt.server_type.map(|t| t.value);
    let server_version = stmt.version.map(|v| v.value);
    let options = stmt
        .options
        .unwrap_or_default()
        .into_iter()
        .map(|opt| (opt.key.value, opt.value.value))
        .collect();
    let server = Server {
        name: name.clone(),
        foreign_data_wrapper,
        server_type,
        server_version,
        options,
        owner: None,
        comment: None,
    };
    schema.servers.insert(name, server);
}

fn make_trigger_key(schema: &str, table: &str, trigger_name: &str) -> String {
    format!("{}.{}.{}", schema, table, trigger_name)
}
