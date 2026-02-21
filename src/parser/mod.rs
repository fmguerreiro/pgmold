mod dependencies;
mod functions;
mod grants;
mod loader;
mod ownership;
mod preprocess;
mod sequences;
mod tables;
mod util;

#[cfg(test)]
mod tests;

pub use dependencies::{
    extract_function_references, extract_table_references, topological_sort, ObjectRef,
};
pub use loader::load_schema_sources;

use crate::model::*;
use crate::pg::sqlgen::strip_ident_quotes;
use crate::util::{normalize_sql_whitespace, Result, SchemaError};
use sqlparser::ast::{
    AlterTable, AlterTableOperation, CreateDomain, CreateExtension, CreateFunction, CreateTrigger,
    CreateView, DropDomain, DropExtension, DropFunction, DropTrigger, ObjectType,
    RenameTableNameKind, SchemaName, Statement, TableConstraint, TriggerEvent as SqlTriggerEvent,
    TriggerPeriod, TriggerReferencingType, UserDefinedTypeRepresentation,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::fs;

use self::functions::parse_create_function;
use self::grants::{
    parse_alter_default_privileges, parse_grant_statements, parse_revoke_statements,
};
use self::ownership::parse_owner_statements;
use self::preprocess::preprocess_sql;
use self::sequences::parse_create_sequence;
use self::tables::{parse_column_with_serial, parse_create_table, parse_referential_action};
use self::util::{
    extract_qualified_name, normalize_expr, parse_data_type, parse_for_values, parse_policy_command,
};

pub fn parse_sql_file(path: &str) -> Result<Schema> {
    let content = fs::read_to_string(path)
        .map_err(|e| SchemaError::ParseError(format!("Failed to read file: {e}")))?;
    parse_sql_string(&content)
}

pub fn parse_sql_string(sql: &str) -> Result<Schema> {
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
                    .map(|n| n.to_string().trim_matches('"').to_string())
                    .ok_or_else(|| SchemaError::ParseError("Index must have name".into()))?;
                let (tbl_schema, tbl_name) = extract_qualified_name(&ci.table_name);
                let tbl_key = qualified_name(&tbl_schema, &tbl_name);

                if let Some(table) = schema.tables.get_mut(&tbl_key) {
                    table.indexes.push(Index {
                        name: idx_name,
                        columns: ci
                            .columns
                            .iter()
                            .map(|c| c.column.expr.to_string().trim_matches('"').to_string())
                            .collect(),
                        unique: ci.unique,
                        index_type: IndexType::BTree,
                        predicate: ci.predicate.as_ref().map(|p| p.to_string()),
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
                };
                let key = qualified_name(&enum_schema, &enum_name);
                schema.enums.insert(key, enum_type);
            }
            Statement::CreatePolicy {
                name,
                table_name,
                command,
                to,
                using,
                with_check,
                ..
            } => {
                let (tbl_schema, tbl_name) = extract_qualified_name(&table_name);
                let policy = Policy {
                    name: name.to_string().trim_matches('"').to_string(),
                    table_schema: tbl_schema,
                    table: tbl_name,
                    command: parse_policy_command(&command),
                    roles: {
                        let parsed_roles: Vec<String> = to
                            .iter()
                            .flat_map(|owners| {
                                owners.iter().map(|o| strip_ident_quotes(&o.to_string()))
                            })
                            .collect();
                        if parsed_roles.is_empty() {
                            vec!["public".to_string()]
                        } else {
                            parsed_roles
                        }
                    },
                    using_expr: using.as_ref().map(|e| normalize_expr(&e.to_string())),
                    check_expr: with_check.as_ref().map(|e| normalize_expr(&e.to_string())),
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
                        AlterTableOperation::EnableTrigger { name: trig_name } => {
                            let trigger_key =
                                format!("{}.{}.{}", tbl_schema, tbl_name, trig_name.value);
                            if let Some(trigger) = schema.triggers.get_mut(&trigger_key) {
                                trigger.enabled = TriggerEnabled::Origin;
                            }
                        }
                        AlterTableOperation::DisableTrigger { name: trig_name } => {
                            let trigger_key =
                                format!("{}.{}.{}", tbl_schema, tbl_name, trig_name.value);
                            if let Some(trigger) = schema.triggers.get_mut(&trigger_key) {
                                trigger.enabled = TriggerEnabled::Disabled;
                            }
                        }
                        AlterTableOperation::EnableReplicaTrigger { name: trig_name } => {
                            let trigger_key =
                                format!("{}.{}.{}", tbl_schema, tbl_name, trig_name.value);
                            if let Some(trigger) = schema.triggers.get_mut(&trigger_key) {
                                trigger.enabled = TriggerEnabled::Replica;
                            }
                        }
                        AlterTableOperation::EnableAlwaysTrigger { name: trig_name } => {
                            let trigger_key =
                                format!("{}.{}.{}", tbl_schema, tbl_name, trig_name.value);
                            if let Some(trigger) = schema.triggers.get_mut(&trigger_key) {
                                trigger.enabled = TriggerEnabled::Always;
                            }
                        }
                        AlterTableOperation::AddConstraint { constraint, .. } => {
                            if let Some(table) = schema.tables.get_mut(&tbl_key) {
                                if let TableConstraint::ForeignKey(fk) = constraint {
                                    let fk_name = fk
                                        .name
                                        .as_ref()
                                        .map(|n| n.to_string().trim_matches('"').to_string())
                                        .unwrap_or_else(|| {
                                            format!(
                                                "{}_{}_fkey",
                                                tbl_name,
                                                fk.columns[0].to_string().trim_matches('"')
                                            )
                                        });
                                    let (ref_schema, ref_table) =
                                        extract_qualified_name(&fk.foreign_table);
                                    table.foreign_keys.push(ForeignKey {
                                        name: fk_name,
                                        columns: fk
                                            .columns
                                            .iter()
                                            .map(|c| c.to_string().trim_matches('"').to_string())
                                            .collect(),
                                        referenced_schema: ref_schema,
                                        referenced_table: ref_table,
                                        referenced_columns: fk
                                            .referred_columns
                                            .iter()
                                            .map(|c| c.to_string().trim_matches('"').to_string())
                                            .collect(),
                                        on_delete: parse_referential_action(&fk.on_delete),
                                        on_update: parse_referential_action(&fk.on_update),
                                    });
                                } else if let TableConstraint::Check(chk) = constraint {
                                    let constraint_name = chk
                                        .name
                                        .as_ref()
                                        .map(|n| n.to_string().trim_matches('"').to_string())
                                        .unwrap_or_else(|| format!("{tbl_name}_check"));

                                    table.check_constraints.push(CheckConstraint {
                                        name: constraint_name,
                                        expression: normalize_expr(&chk.expr.to_string()),
                                    });
                                    table.check_constraints.sort();
                                } else if let TableConstraint::Unique(uniq) = constraint {
                                    let constraint_name = uniq
                                        .name
                                        .as_ref()
                                        .map(|n| n.to_string().trim_matches('"').to_string())
                                        .unwrap_or_else(|| format!("{tbl_name}_unique"));

                                    table.indexes.push(Index {
                                        name: constraint_name,
                                        columns: uniq
                                            .columns
                                            .iter()
                                            .map(|c| {
                                                c.column
                                                    .expr
                                                    .to_string()
                                                    .trim_matches('"')
                                                    .to_string()
                                            })
                                            .collect(),
                                        unique: true,
                                        index_type: IndexType::BTree,
                                        predicate: None,
                                    });
                                    table.indexes.sort();
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
                                    .map(|n| n.value.trim_matches('"').to_string())
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
                                let old_name = old_column_name.value.trim_matches('"').to_string();
                                let new_name = new_column_name.value.trim_matches('"').to_string();

                                if let Some(mut column) = table.columns.remove(&old_name) {
                                    column.name = new_name.clone();
                                    table.columns.insert(new_name, column);
                                }
                            }
                        }
                        AlterTableOperation::RenameConstraint { old_name, new_name } => {
                            let old_constraint_name = old_name.value.trim_matches('"').to_string();
                            let new_constraint_name = new_name.value.trim_matches('"').to_string();

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
                        _ => {}
                    }
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
                let ext_name = name.to_string().trim_matches('"').to_string();
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
                        .map(|s| s.to_string().trim_matches('"').to_string()),
                };
                schema.extensions.insert(ext_name, ext);
            }
            Statement::CreateSchema { schema_name, .. } => {
                let name = match &schema_name {
                    SchemaName::Simple(obj) => obj.to_string().trim_matches('"').to_string(),
                    SchemaName::UnnamedAuthorization(ident) => {
                        ident.to_string().trim_matches('"').to_string()
                    }
                    SchemaName::NamedAuthorization(obj, _) => {
                        obj.to_string().trim_matches('"').to_string()
                    }
                };
                schema.schemas.insert(
                    name.clone(),
                    PgSchema {
                        name,
                        grants: Vec::new(),
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
                        _ => {
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
                ..
            }) => {
                let (tbl_schema, tbl_name) = extract_qualified_name(&table_name);
                let trigger_name = name.to_string().trim_matches('"').to_string();
                let exec = exec_body.as_ref().ok_or_else(|| {
                    SchemaError::ParseError(format!(
                        "Trigger '{trigger_name}' missing EXECUTE clause"
                    ))
                })?;
                let (func_schema, func_name) = extract_qualified_name(&exec.func_desc.name);

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
                            update_columns.extend(cols.iter().map(|c| c.to_string()));
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
                    .func_desc
                    .args
                    .as_ref()
                    .map(|args| args.iter().map(|a| a.to_string()).collect())
                    .unwrap_or_default();

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
                        _ => {}
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
                                    normalize_pg_type(&type_str)
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
                    trigger_name.to_string().trim_matches('"')
                );
                schema.triggers.remove(&trigger_key);
            }
            Statement::DropTrigger(DropTrigger { .. }) => {}
            Statement::DropPolicy {
                name, table_name, ..
            } => {
                let (tbl_schema, tbl_name) = extract_qualified_name(&table_name);
                let tbl_key = qualified_name(&tbl_schema, &tbl_name);
                let policy_name = name.to_string().trim_matches('"').to_string();

                if let Some(table) = schema.tables.get_mut(&tbl_key) {
                    table.policies.retain(|p| p.name != policy_name);
                }
                schema.pending_policies.retain(|p| {
                    !(p.table_schema == tbl_schema && p.table == tbl_name && p.name == policy_name)
                });
            }
            Statement::DropExtension(DropExtension { names, .. }) => {
                for name in names {
                    let ext_name = name.to_string().trim_matches('"').to_string();
                    if ext_name == "plpgsql" {
                        continue;
                    }
                    schema.extensions.remove(&ext_name);
                }
            }
            _ => {}
        }
    }

    parse_owner_statements(sql, &mut schema);
    parse_grant_statements(sql, &mut schema)?;
    parse_revoke_statements(sql, &mut schema)?;
    parse_alter_default_privileges(sql, &mut schema)?;

    schema.pending_policies = schema.finalize_partial();

    Ok(schema)
}
