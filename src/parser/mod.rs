mod loader;
pub use loader::load_schema_sources;

use crate::model::*;
use crate::util::{Result, SchemaError};
use sqlparser::ast::{
    ColumnDef, ColumnOption, DataType, Expr, ForValues, PartitionBoundValue, SequenceOptions,
    Statement, TableConstraint,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::BTreeMap;
use std::fs;

use crate::util::{normalize_sql_whitespace, normalize_type_casts};

fn normalize_expr(expr: &str) -> String {
    normalize_type_casts(expr)
}

fn extract_qualified_name(name: &sqlparser::ast::ObjectName) -> (String, String) {
    let parts: Vec<String> = name
        .0
        .iter()
        .map(|part| part.to_string().trim_matches('"').to_string())
        .collect();
    match parts.as_slice() {
        [schema, table] => (schema.clone(), table.clone()),
        [table] => ("public".to_string(), table.clone()),
        _ => panic!("Unexpected object name format: {name:?}"),
    }
}

fn parse_policy_command(cmd: &Option<sqlparser::ast::CreatePolicyCommand>) -> PolicyCommand {
    match cmd {
        Some(sqlparser::ast::CreatePolicyCommand::All) => PolicyCommand::All,
        Some(sqlparser::ast::CreatePolicyCommand::Select) => PolicyCommand::Select,
        Some(sqlparser::ast::CreatePolicyCommand::Insert) => PolicyCommand::Insert,
        Some(sqlparser::ast::CreatePolicyCommand::Update) => PolicyCommand::Update,
        Some(sqlparser::ast::CreatePolicyCommand::Delete) => PolicyCommand::Delete,
        None => PolicyCommand::All,
    }
}

fn parse_for_values(for_values: &Option<ForValues>) -> Result<PartitionBound> {
    match for_values {
        Some(ForValues::In(values)) => Ok(PartitionBound::List {
            values: values
                .iter()
                .map(|e| normalize_expr(&e.to_string()))
                .collect(),
        }),
        Some(ForValues::From { from, to }) => Ok(PartitionBound::Range {
            from: from.iter().map(partition_bound_value_to_string).collect(),
            to: to.iter().map(partition_bound_value_to_string).collect(),
        }),
        Some(ForValues::With { modulus, remainder }) => Ok(PartitionBound::Hash {
            modulus: *modulus as u32,
            remainder: *remainder as u32,
        }),
        Some(ForValues::Default) => Ok(PartitionBound::Default),
        None => Err(SchemaError::ParseError(
            "PARTITION OF requires FOR VALUES clause".into(),
        )),
    }
}

fn partition_bound_value_to_string(v: &PartitionBoundValue) -> String {
    match v {
        PartitionBoundValue::Expr(e) => normalize_expr(&e.to_string()),
        PartitionBoundValue::MinValue => "MINVALUE".to_string(),
        PartitionBoundValue::MaxValue => "MAXVALUE".to_string(),
    }
}

pub fn parse_sql_file(path: &str) -> Result<Schema> {
    let content = fs::read_to_string(path)
        .map_err(|e| SchemaError::ParseError(format!("Failed to read file: {e}")))?;
    parse_sql_string(&content)
}

/// Preprocess SQL to remove/normalize syntax not supported by sqlparser 0.52
fn preprocess_sql(sql: &str) -> String {
    use regex::Regex;
    // Match SET search_path until newline or AS keyword
    let set_search_path_re =
        Regex::new(r"(?i)\bSET\s+search_path\s+TO\s+'[^']*'(?:\s*,\s*'[^']*')*").unwrap();
    // Remove ALTER FUNCTION statements (ownership, etc.)
    let alter_function_re = Regex::new(r"(?i)ALTER\s+FUNCTION\s+[^;]+;").unwrap();
    // Remove ALTER SEQUENCE statements (sqlparser doesn't support them)
    let alter_sequence_re = Regex::new(r"(?i)ALTER\s+SEQUENCE\s+[^;]+;").unwrap();

    let processed = set_search_path_re.replace_all(sql, "");
    let processed = alter_function_re.replace_all(&processed, "");
    let processed = alter_sequence_re.replace_all(&processed, "");

    processed.to_string()
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

                // Check if this is a PARTITION OF statement
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
                    });
                    table.indexes.sort();
                }
            }
            Statement::CreateType {
                name,
                representation: Some(sqlparser::ast::UserDefinedTypeRepresentation::Enum { labels }),
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
                    roles: to
                        .iter()
                        .flat_map(|owners| {
                            owners
                                .iter()
                                .map(|o| crate::pg::sqlgen::strip_ident_quotes(&o.to_string()))
                        })
                        .collect(),
                    using_expr: using.as_ref().map(|e| normalize_expr(&e.to_string())),
                    check_expr: with_check.as_ref().map(|e| normalize_expr(&e.to_string())),
                };
                schema.pending_policies.push(policy);
            }
            Statement::AlterTable(sqlparser::ast::AlterTable {
                name, operations, ..
            }) => {
                let (tbl_schema, tbl_name) = extract_qualified_name(&name);
                let tbl_key = qualified_name(&tbl_schema, &tbl_name);
                for op in operations {
                    match op {
                        sqlparser::ast::AlterTableOperation::EnableRowLevelSecurity => {
                            if let Some(table) = schema.tables.get_mut(&tbl_key) {
                                table.row_level_security = true;
                            }
                        }
                        sqlparser::ast::AlterTableOperation::DisableRowLevelSecurity => {
                            if let Some(table) = schema.tables.get_mut(&tbl_key) {
                                table.row_level_security = false;
                            }
                        }
                        sqlparser::ast::AlterTableOperation::EnableTrigger { name: trig_name } => {
                            let trigger_key =
                                format!("{}.{}.{}", tbl_schema, tbl_name, trig_name.value);
                            if let Some(trigger) = schema.triggers.get_mut(&trigger_key) {
                                trigger.enabled = TriggerEnabled::Origin;
                            }
                        }
                        sqlparser::ast::AlterTableOperation::DisableTrigger { name: trig_name } => {
                            let trigger_key =
                                format!("{}.{}.{}", tbl_schema, tbl_name, trig_name.value);
                            if let Some(trigger) = schema.triggers.get_mut(&trigger_key) {
                                trigger.enabled = TriggerEnabled::Disabled;
                            }
                        }
                        sqlparser::ast::AlterTableOperation::EnableReplicaTrigger {
                            name: trig_name,
                        } => {
                            let trigger_key =
                                format!("{}.{}.{}", tbl_schema, tbl_name, trig_name.value);
                            if let Some(trigger) = schema.triggers.get_mut(&trigger_key) {
                                trigger.enabled = TriggerEnabled::Replica;
                            }
                        }
                        sqlparser::ast::AlterTableOperation::EnableAlwaysTrigger {
                            name: trig_name,
                        } => {
                            let trigger_key =
                                format!("{}.{}.{}", tbl_schema, tbl_name, trig_name.value);
                            if let Some(trigger) = schema.triggers.get_mut(&trigger_key) {
                                trigger.enabled = TriggerEnabled::Always;
                            }
                        }
                        sqlparser::ast::AlterTableOperation::AddConstraint {
                            constraint, ..
                        } => {
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
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            Statement::CreateFunction(sqlparser::ast::CreateFunction {
                name,
                args,
                return_type,
                function_body,
                language,
                behavior,
                security,
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
                )?;
                let key = qualified_name(&func_schema, &func.signature());
                schema.functions.insert(key, func);
            }
            Statement::CreateView(sqlparser::ast::CreateView {
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
                };
                let key = qualified_name(&view_schema, &view_name);
                schema.views.insert(key, view);
            }
            Statement::CreateExtension(sqlparser::ast::CreateExtension {
                name,
                version,
                schema: ext_schema,
                ..
            }) => {
                let ext_name = name.to_string().trim_matches('"').to_string();
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
            Statement::CreateDomain(sqlparser::ast::CreateDomain {
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
                };
                let key = qualified_name(&domain_schema, &domain_name);
                schema.domains.insert(key, domain);
            }
            Statement::CreateTrigger(sqlparser::ast::CreateTrigger {
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
                    Some(sqlparser::ast::TriggerPeriod::Before) => TriggerTiming::Before,
                    Some(sqlparser::ast::TriggerPeriod::After) => TriggerTiming::After,
                    Some(sqlparser::ast::TriggerPeriod::InsteadOf) => TriggerTiming::InsteadOf,
                    Some(sqlparser::ast::TriggerPeriod::For) => TriggerTiming::Before,
                    None => TriggerTiming::Before,
                };

                let mut trigger_events = Vec::new();
                let mut update_columns = Vec::new();

                for event in &events {
                    match event {
                        sqlparser::ast::TriggerEvent::Insert => {
                            trigger_events.push(TriggerEvent::Insert);
                        }
                        sqlparser::ast::TriggerEvent::Update(cols) => {
                            trigger_events.push(TriggerEvent::Update);
                            update_columns.extend(cols.iter().map(|c| c.to_string()));
                        }
                        sqlparser::ast::TriggerEvent::Delete => {
                            trigger_events.push(TriggerEvent::Delete);
                        }
                        sqlparser::ast::TriggerEvent::Truncate => {
                            trigger_events.push(TriggerEvent::Truncate);
                        }
                    }
                }

                let mut old_table_name = None;
                let mut new_table_name = None;
                for tr in &referencing {
                    match tr.refer_type {
                        sqlparser::ast::TriggerReferencingType::OldTable => {
                            old_table_name = Some(tr.transition_relation_name.to_string());
                        }
                        sqlparser::ast::TriggerReferencingType::NewTable => {
                            new_table_name = Some(tr.transition_relation_name.to_string());
                        }
                    }
                }

                let for_each_row = trigger_object
                    .as_ref()
                    .map(|to| to.to_string().to_uppercase().contains("ROW"))
                    .unwrap_or(false);

                let when_clause = condition.as_ref().map(|e| normalize_expr(&e.to_string()));

                // Validate INSTEAD OF trigger rules per PostgreSQL requirements
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

                // Validate REFERENCING clause rules
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
            _ => {}
        }
    }

    // Associate pending policies with their tables.
    // Orphaned policies (referencing non-existent tables) remain in pending_policies
    // for potential resolution after schema merging in load_schema_sources.
    schema.pending_policies = schema.finalize_partial();

    Ok(schema)
}

struct ParsedTable {
    table: Table,
    sequences: Vec<Sequence>,
}

fn parse_create_table(
    schema: &str,
    name: &str,
    columns: &[ColumnDef],
    constraints: &[TableConstraint],
    partition_by: Option<&Expr>,
) -> Result<ParsedTable> {
    let mut table = Table {
        schema: schema.to_string(),
        name: name.to_string(),
        columns: BTreeMap::new(),
        indexes: Vec::new(),
        primary_key: None,
        foreign_keys: Vec::new(),
        check_constraints: Vec::new(),
        comment: None,
        row_level_security: false,
        policies: Vec::new(),
        partition_by: partition_by.and_then(parse_partition_by),
    };

    let mut sequences = Vec::new();

    for col_def in columns {
        let (column, maybe_sequence) = parse_column_with_serial(schema, name, col_def)?;
        table.columns.insert(column.name.clone(), column);
        if let Some(seq) = maybe_sequence {
            sequences.push(seq);
        }
    }

    // Check for inline PRIMARY KEY in column options
    for col_def in columns {
        for option in &col_def.options {
            // Check for PRIMARY KEY by examining the option's string representation
            let option_str = format!("{:?}", option.option);
            if option_str.contains("PRIMARY") || option_str.contains("Primary") {
                let pk_col = col_def.name.to_string().trim_matches('"').to_string();
                table.primary_key = Some(PrimaryKey {
                    columns: vec![pk_col.clone()],
                });
                // Mark PRIMARY KEY column as NOT NULL
                if let Some(col) = table.columns.get_mut(&pk_col) {
                    col.nullable = false;
                }
            }
        }
    }

    // Parse table-level constraints
    for constraint in constraints {
        match constraint {
            TableConstraint::PrimaryKey(pk) => {
                let pk_columns: Vec<String> = pk
                    .columns
                    .iter()
                    .map(|c| c.to_string().trim_matches('"').to_string())
                    .collect();
                table.primary_key = Some(PrimaryKey {
                    columns: pk_columns.clone(),
                });
                // Mark all PRIMARY KEY columns as NOT NULL
                for pk_col in &pk_columns {
                    if let Some(col) = table.columns.get_mut(pk_col) {
                        col.nullable = false;
                    }
                }
            }
            TableConstraint::ForeignKey(fk) => {
                let fk_name = fk
                    .name
                    .as_ref()
                    .map(|n| n.to_string().trim_matches('"').to_string())
                    .unwrap_or_else(|| {
                        format!(
                            "{}_{}_fkey",
                            table.name,
                            fk.columns[0].to_string().trim_matches('"')
                        )
                    });

                let (ref_schema, ref_table) = extract_qualified_name(&fk.foreign_table);
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
            }
            TableConstraint::Check(chk) => {
                let constraint_name = chk
                    .name
                    .as_ref()
                    .map(|n| n.to_string().trim_matches('"').to_string())
                    .unwrap_or_else(|| format!("{}_check", table.name));

                table.check_constraints.push(CheckConstraint {
                    name: constraint_name,
                    expression: normalize_expr(&chk.expr.to_string()),
                });
            }
            _ => {}
        }
    }

    table.foreign_keys.sort();
    table.check_constraints.sort();

    Ok(ParsedTable { table, sequences })
}

fn parse_column_with_serial(
    table_schema: &str,
    table_name: &str,
    col_def: &ColumnDef,
) -> Result<(Column, Option<Sequence>)> {
    let mut nullable = true;
    let mut default = None;

    for option in &col_def.options {
        match &option.option {
            ColumnOption::NotNull => nullable = false,
            ColumnOption::Null => nullable = true,
            ColumnOption::Default(expr) => {
                default = Some(normalize_expr(&expr.to_string()));
            }
            _ => {}
        }
    }

    let col_name = col_def.name.to_string().trim_matches('"').to_string();

    if let Some(seq_data_type) = detect_serial_type(&col_def.data_type) {
        let seq_name = format!("{table_name}_{col_name}_seq");
        let seq_qualified = qualified_name(table_schema, &seq_name);

        let pg_type = match seq_data_type {
            SequenceDataType::SmallInt => PgType::SmallInt,
            SequenceDataType::Integer => PgType::Integer,
            SequenceDataType::BigInt => PgType::BigInt,
        };

        let max_value = match seq_data_type {
            SequenceDataType::SmallInt => Some(32767),
            SequenceDataType::Integer => Some(2147483647),
            SequenceDataType::BigInt => Some(9223372036854775807),
        };

        let column = Column {
            name: col_name.clone(),
            data_type: pg_type,
            nullable,
            default: Some(format!("nextval('{seq_qualified}'::regclass)")),
            comment: None,
        };

        let sequence = Sequence {
            name: seq_name,
            schema: table_schema.to_string(),
            data_type: seq_data_type,
            start: Some(1),
            increment: Some(1),
            min_value: Some(1),
            max_value,
            cycle: false,
            cache: Some(1),
            owned_by: Some(SequenceOwner {
                table_schema: table_schema.to_string(),
                table_name: table_name.to_string(),
                column_name: col_name,
            }),
        };

        Ok((column, Some(sequence)))
    } else {
        let column = Column {
            name: col_name,
            data_type: parse_data_type(&col_def.data_type)?,
            nullable,
            default,
            comment: None,
        };
        Ok((column, None))
    }
}

fn detect_serial_type(dt: &DataType) -> Option<SequenceDataType> {
    if let DataType::Custom(name, _) = dt {
        let type_name = name.to_string().to_lowercase();
        match type_name.as_str() {
            "serial" => Some(SequenceDataType::Integer),
            "bigserial" => Some(SequenceDataType::BigInt),
            "smallserial" => Some(SequenceDataType::SmallInt),
            _ => None,
        }
    } else {
        None
    }
}

fn parse_data_type(dt: &DataType) -> Result<PgType> {
    match dt {
        DataType::Integer(_) | DataType::Int(_) => Ok(PgType::Integer),
        DataType::BigInt(_) => Ok(PgType::BigInt),
        DataType::SmallInt(_) => Ok(PgType::SmallInt),
        DataType::Varchar(len) => {
            let size = len.as_ref().and_then(|l| match l {
                sqlparser::ast::CharacterLength::IntegerLength { length, .. } => {
                    Some(*length as u32)
                }
                sqlparser::ast::CharacterLength::Max => None,
            });
            Ok(PgType::Varchar(size))
        }
        DataType::Text => Ok(PgType::Text),
        DataType::Boolean => Ok(PgType::Boolean),
        DataType::Timestamp(_, tz) => {
            if *tz == sqlparser::ast::TimezoneInfo::WithTimeZone {
                Ok(PgType::TimestampTz)
            } else {
                Ok(PgType::Timestamp)
            }
        }
        DataType::Date => Ok(PgType::Date),
        DataType::Uuid => Ok(PgType::Uuid),
        DataType::JSON => Ok(PgType::Json),
        DataType::JSONB => Ok(PgType::Jsonb),
        DataType::Custom(name, modifiers) => {
            let parts: Vec<String> = name
                .0
                .iter()
                .map(|part| part.to_string().trim_matches('"').to_string())
                .collect();

            let type_name = parts.last().map(|s| s.as_str()).unwrap_or("");

            if type_name == "vector" {
                let dimension = modifiers.first().and_then(|m| m.parse::<u32>().ok());
                return Ok(PgType::Vector(dimension));
            }

            let qualified = match parts.as_slice() {
                [schema, type_name] => format!("{schema}.{type_name}"),
                [type_name] => format!("public.{type_name}"),
                _ => name.to_string(),
            };
            Ok(PgType::CustomEnum(qualified))
        }
        _ => Ok(PgType::Text),
    }
}

fn parse_referential_action(
    action: &Option<sqlparser::ast::ReferentialAction>,
) -> ReferentialAction {
    match action {
        Some(sqlparser::ast::ReferentialAction::NoAction) => ReferentialAction::NoAction,
        Some(sqlparser::ast::ReferentialAction::Restrict) => ReferentialAction::Restrict,
        Some(sqlparser::ast::ReferentialAction::Cascade) => ReferentialAction::Cascade,
        Some(sqlparser::ast::ReferentialAction::SetNull) => ReferentialAction::SetNull,
        Some(sqlparser::ast::ReferentialAction::SetDefault) => ReferentialAction::SetDefault,
        None => ReferentialAction::NoAction,
    }
}

/// Parse partition_by expression from sqlparser into our PartitionKey model.
/// sqlparser parses `PARTITION BY RANGE (col1, col2)` as a function call expression
/// where the function name is RANGE/LIST/HASH and the arguments are the columns.
fn parse_partition_by(expr: &Expr) -> Option<PartitionKey> {
    match expr {
        Expr::Function(func) => {
            let strategy_name = func.name.to_string().to_uppercase();
            let strategy = match strategy_name.as_str() {
                "RANGE" => PartitionStrategy::Range,
                "LIST" => PartitionStrategy::List,
                "HASH" => PartitionStrategy::Hash,
                _ => return None,
            };

            let columns: Vec<String> = match &func.args {
                sqlparser::ast::FunctionArguments::List(args) => args
                    .args
                    .iter()
                    .filter_map(|arg| match arg {
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(Expr::Identifier(ident)),
                        ) => Some(ident.value.clone()),
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(expr),
                        ) => Some(expr.to_string()),
                        _ => None,
                    })
                    .collect(),
                _ => Vec::new(),
            };

            Some(PartitionKey {
                strategy,
                columns,
                expressions: Vec::new(),
            })
        }
        _ => None,
    }
}

/// Strips dollar-quote delimiters from a function body.
/// Handles both `$$...$$` and `$tag$...$tag$` formats.
fn strip_dollar_quotes(body: &str) -> String {
    let trimmed = body.trim();

    if !trimmed.starts_with('$') {
        return body.to_string();
    }

    if let Some(tag_end) = trimmed[1..].find('$') {
        let tag = &trimmed[..=tag_end + 1];
        if let Some(content) = trimmed.strip_prefix(tag) {
            if let Some(inner) = content.strip_suffix(tag) {
                return inner.to_string();
            }
        }
    }

    body.to_string()
}

#[allow(clippy::too_many_arguments)]
fn parse_create_function(
    schema: &str,
    name: &str,
    args: Option<&[sqlparser::ast::OperateFunctionArg]>,
    return_type: Option<&sqlparser::ast::DataType>,
    function_body: Option<&sqlparser::ast::CreateFunctionBody>,
    language: Option<&sqlparser::ast::Ident>,
    behavior: Option<&sqlparser::ast::FunctionBehavior>,
    security: Option<&sqlparser::ast::FunctionSecurity>,
) -> Result<Function> {
    let return_type_str = return_type
        .map(|rt| crate::model::normalize_pg_type(&rt.to_string()))
        .ok_or_else(|| {
            SchemaError::ParseError(format!(
                "Function {schema}.{name} is missing RETURNS clause"
            ))
        })?;

    let language_str = language
        .map(|l| l.to_string().to_lowercase())
        .unwrap_or_else(|| "sql".to_string());

    let body = function_body
        .map(|fb| match fb {
            sqlparser::ast::CreateFunctionBody::AsBeforeOptions { body, .. } => body.to_string(),
            sqlparser::ast::CreateFunctionBody::AsAfterOptions(expr) => expr.to_string(),
            sqlparser::ast::CreateFunctionBody::Return(expr) => expr.to_string(),
            sqlparser::ast::CreateFunctionBody::AsBeginEnd(stmts) => stmts.to_string(),
            sqlparser::ast::CreateFunctionBody::AsReturnExpr(expr) => expr.to_string(),
            sqlparser::ast::CreateFunctionBody::AsReturnSelect(sel) => sel.to_string(),
        })
        .map(|b| strip_dollar_quotes(&b).trim().to_string())
        .ok_or_else(|| {
            SchemaError::ParseError(format!("Function {schema}.{name} is missing body"))
        })?;

    let volatility = behavior
        .map(|b| match b {
            sqlparser::ast::FunctionBehavior::Immutable => Volatility::Immutable,
            sqlparser::ast::FunctionBehavior::Stable => Volatility::Stable,
            sqlparser::ast::FunctionBehavior::Volatile => Volatility::Volatile,
        })
        .unwrap_or_default();

    let security_type = security
        .map(|s| match s {
            sqlparser::ast::FunctionSecurity::Definer => SecurityType::Definer,
            sqlparser::ast::FunctionSecurity::Invoker => SecurityType::Invoker,
        })
        .unwrap_or_default();

    let arguments: Vec<FunctionArg> = args
        .map(|arg_list| {
            arg_list
                .iter()
                .map(|arg| {
                    let mode = match arg.mode {
                        Some(sqlparser::ast::ArgMode::In) => ArgMode::In,
                        Some(sqlparser::ast::ArgMode::Out) => ArgMode::Out,
                        Some(sqlparser::ast::ArgMode::InOut) => ArgMode::InOut,
                        None => ArgMode::In,
                    };
                    FunctionArg {
                        name: arg
                            .name
                            .as_ref()
                            .map(|n| crate::pg::sqlgen::strip_ident_quotes(&n.value)),
                        data_type: crate::model::normalize_pg_type(&arg.data_type.to_string()),
                        mode,
                        default: arg
                            .default_expr
                            .as_ref()
                            .map(|e| e.to_string().to_lowercase()),
                    }
                })
                .collect()
        })
        .unwrap_or_default();

    Ok(Function {
        schema: schema.to_string(),
        name: name.to_string(),
        arguments,
        return_type: return_type_str,
        language: language_str,
        body,
        volatility,
        security: security_type,
    })
}

fn parse_create_sequence(
    schema: &str,
    name: &str,
    data_type: Option<&DataType>,
    sequence_options: &[SequenceOptions],
    owned_by: Option<&sqlparser::ast::ObjectName>,
) -> Result<Sequence> {
    let seq_data_type = data_type
        .map(|dt| match dt {
            DataType::SmallInt(_) => SequenceDataType::SmallInt,
            DataType::BigInt(_) => SequenceDataType::BigInt,
            DataType::Integer(_) | DataType::Int(_) => SequenceDataType::Integer,
            _ => SequenceDataType::Integer,
        })
        .unwrap_or(SequenceDataType::Integer);

    let mut start: Option<i64> = None;
    let mut increment: Option<i64> = None;
    let mut min_value: Option<i64> = None;
    let mut max_value: Option<i64> = None;
    let mut cycle = false;
    let mut cache: Option<i64> = None;

    for option in sequence_options {
        match option {
            SequenceOptions::IncrementBy(expr, _) => {
                increment = extract_i64_from_expr(expr);
            }
            SequenceOptions::MinValue(Some(expr)) => {
                min_value = extract_i64_from_expr(expr);
            }
            SequenceOptions::MaxValue(Some(expr)) => {
                max_value = extract_i64_from_expr(expr);
            }
            SequenceOptions::StartWith(expr, _) => {
                start = extract_i64_from_expr(expr);
            }
            SequenceOptions::Cache(expr) => {
                cache = extract_i64_from_expr(expr);
            }
            SequenceOptions::Cycle(c) => {
                cycle = *c;
            }
            _ => {}
        }
    }

    let owned_by_parsed = owned_by.and_then(|obj_name| {
        let parts: Vec<String> = obj_name
            .0
            .iter()
            .map(|part| part.to_string().trim_matches('"').to_string())
            .collect();
        match parts.as_slice() {
            [table_schema, table_name, column_name] => Some(SequenceOwner {
                table_schema: table_schema.clone(),
                table_name: table_name.clone(),
                column_name: column_name.clone(),
            }),
            [table_name, column_name] => Some(SequenceOwner {
                table_schema: "public".to_string(),
                table_name: table_name.clone(),
                column_name: column_name.clone(),
            }),
            _ => None,
        }
    });

    let final_increment = increment.or(Some(1));
    let inc = final_increment.unwrap_or(1);
    let is_ascending = inc > 0;

    let final_cache = cache.or(Some(1));

    let final_min_value = min_value.or({
        if is_ascending {
            Some(1)
        } else {
            match seq_data_type {
                SequenceDataType::SmallInt => Some(-32768),
                SequenceDataType::Integer => Some(-2147483648),
                SequenceDataType::BigInt => Some(-9223372036854775808),
            }
        }
    });

    let final_max_value = max_value.or({
        if is_ascending {
            match seq_data_type {
                SequenceDataType::SmallInt => Some(32767),
                SequenceDataType::Integer => Some(2147483647),
                SequenceDataType::BigInt => Some(9223372036854775807),
            }
        } else {
            Some(-1)
        }
    });

    let final_start = start.or({
        if is_ascending {
            final_min_value
        } else {
            final_max_value
        }
    });

    Ok(Sequence {
        name: name.to_string(),
        schema: schema.to_string(),
        data_type: seq_data_type,
        start: final_start,
        increment: final_increment,
        min_value: final_min_value,
        max_value: final_max_value,
        cycle,
        cache: final_cache,
        owned_by: owned_by_parsed,
    })
}

fn extract_i64_from_expr(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Value(value_with_span) => {
            if let sqlparser::ast::Value::Number(n, _) = &value_with_span.value {
                n.parse::<i64>().ok()
            } else {
                None
            }
        }
        Expr::UnaryOp { op, expr } => {
            if matches!(op, sqlparser::ast::UnaryOperator::Minus) {
                extract_i64_from_expr(expr).map(|n| -n)
            } else {
                None
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_extension() {
        let sql = r#"
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pgcrypto;
"#;

        let schema = parse_sql_string(sql).expect("Should parse");

        assert_eq!(schema.extensions.len(), 2);
        assert!(schema.extensions.contains_key("uuid-ossp"));
        assert!(schema.extensions.contains_key("pgcrypto"));

        let uuid_ext = &schema.extensions["uuid-ossp"];
        assert_eq!(uuid_ext.name, "uuid-ossp");
    }

    #[test]
    fn parse_simple_view() {
        let sql = r#"
CREATE TABLE users (
    id BIGINT NOT NULL PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true
);

CREATE VIEW active_users AS
SELECT id, email FROM users WHERE active = true;
"#;

        let schema = parse_sql_string(sql).expect("Should parse");

        assert_eq!(schema.views.len(), 1);
        assert!(schema.views.contains_key("public.active_users"));

        let view = &schema.views["public.active_users"];
        assert_eq!(view.name, "active_users");
        assert!(!view.materialized);
        assert!(view.query.contains("SELECT"));
    }

    #[test]
    fn parse_materialized_view() {
        let sql = r#"
CREATE TABLE orders (
    id BIGINT NOT NULL PRIMARY KEY,
    amount BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE MATERIALIZED VIEW order_totals AS
SELECT DATE(created_at) as day, SUM(amount) as total
FROM orders
GROUP BY DATE(created_at);
"#;

        let schema = parse_sql_string(sql).expect("Should parse");

        assert_eq!(schema.views.len(), 1);
        assert!(schema.views.contains_key("public.order_totals"));

        let view = &schema.views["public.order_totals"];
        assert_eq!(view.name, "order_totals");
        assert!(view.materialized);
    }

    #[test]
    fn parse_simple_schema() {
        let sql = r#"
CREATE TYPE user_role AS ENUM ('admin', 'user', 'guest');

CREATE TABLE users (
    id BIGINT NOT NULL,
    email VARCHAR(255) NOT NULL,
    role user_role NOT NULL DEFAULT 'guest',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (id)
);

CREATE UNIQUE INDEX users_email_idx ON users (email);

CREATE TABLE posts (
    id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    title TEXT NOT NULL,
    content TEXT,
    PRIMARY KEY (id),
    CONSTRAINT posts_user_id_fkey FOREIGN KEY (user_id)
        REFERENCES users (id) ON DELETE CASCADE
);

CREATE INDEX posts_user_id_idx ON posts (user_id);
"#;

        let schema = parse_sql_string(sql).expect("Should parse");

        assert_eq!(schema.enums.len(), 1);
        assert!(schema.enums.contains_key("public.user_role"));
        assert_eq!(
            schema.enums["public.user_role"].values,
            vec!["admin", "user", "guest"]
        );

        assert_eq!(schema.tables.len(), 2);
        assert!(schema.tables.contains_key("public.users"));
        assert!(schema.tables.contains_key("public.posts"));

        let users = &schema.tables["public.users"];
        assert_eq!(users.columns.len(), 4);
        assert!(users.primary_key.is_some());
        assert_eq!(users.primary_key.as_ref().unwrap().columns, vec!["id"]);
        assert_eq!(users.indexes.len(), 1);
        assert!(users.indexes[0].unique);

        let posts = &schema.tables["public.posts"];
        assert_eq!(posts.columns.len(), 4);
        assert_eq!(posts.foreign_keys.len(), 1);
        assert_eq!(posts.foreign_keys[0].name, "posts_user_id_fkey");
        assert_eq!(posts.foreign_keys[0].on_delete, ReferentialAction::Cascade);
    }

    #[test]
    fn parse_check_constraint() {
        let sql = r#"
CREATE TABLE products (
    id BIGINT NOT NULL PRIMARY KEY,
    price BIGINT NOT NULL,
    quantity INTEGER NOT NULL,
    CONSTRAINT price_positive CHECK (price > 0),
    CONSTRAINT quantity_non_negative CHECK (quantity >= 0)
);
"#;

        let schema = parse_sql_string(sql).expect("Should parse");

        let products = &schema.tables["public.products"];
        assert_eq!(products.check_constraints.len(), 2);

        let price_check = products
            .check_constraints
            .iter()
            .find(|c| c.name == "price_positive")
            .expect("price_positive constraint should exist");
        assert_eq!(price_check.expression, "price > 0");

        let quantity_check = products
            .check_constraints
            .iter()
            .find(|c| c.name == "quantity_non_negative")
            .expect("quantity_non_negative constraint should exist");
        assert_eq!(quantity_check.expression, "quantity >= 0");
    }

    #[test]
    fn parses_qualified_table_name() {
        let sql = "CREATE TABLE auth.users (id INTEGER PRIMARY KEY);";
        let schema = parse_sql_string(sql).unwrap();
        let table = schema.tables.get("auth.users").unwrap();
        assert_eq!(table.schema, "auth");
        assert_eq!(table.name, "users");
    }

    #[test]
    fn parses_unqualified_table_defaults_to_public() {
        let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY);";
        let schema = parse_sql_string(sql).unwrap();
        let table = schema.tables.get("public.users").unwrap();
        assert_eq!(table.schema, "public");
        assert_eq!(table.name, "users");
        assert!(
            table.primary_key.is_some(),
            "PRIMARY KEY should be detected"
        );
        assert_eq!(
            table.primary_key.as_ref().unwrap().columns,
            vec!["id".to_string()]
        );
    }

    #[test]
    fn parses_cross_schema_foreign_key() {
        let sql = r#"
            CREATE TABLE public.orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                FOREIGN KEY (user_id) REFERENCES auth.users(id)
            );
        "#;
        let schema = parse_sql_string(sql).unwrap();
        let table = schema.tables.get("public.orders").unwrap();
        let fk = &table.foreign_keys[0];
        assert_eq!(fk.referenced_schema, "auth");
        assert_eq!(fk.referenced_table, "users");
    }

    #[test]
    fn parses_qualified_view_name() {
        let sql =
            "CREATE VIEW reporting.active_users AS SELECT * FROM public.users WHERE active = true;";
        let schema = parse_sql_string(sql).unwrap();
        let view = schema.views.get("reporting.active_users").unwrap();
        assert_eq!(view.schema, "reporting");
        assert_eq!(view.name, "active_users");
    }

    #[test]
    fn parses_qualified_function_name() {
        let sql = r#"
            CREATE FUNCTION utils.add_one(x INTEGER) RETURNS INTEGER
            LANGUAGE SQL AS $$ SELECT x + 1 $$;
        "#;
        let schema = parse_sql_string(sql).unwrap();
        let func = schema.functions.get("utils.add_one(integer)").unwrap();
        assert_eq!(func.schema, "utils");
        assert_eq!(func.name, "add_one");
    }

    #[test]
    fn parses_function_with_set_search_path() {
        let sql = r#"
            CREATE OR REPLACE FUNCTION auth.custom_access_token_hook(event jsonb)
            RETURNS jsonb
            LANGUAGE plpgsql
            SECURITY DEFINER
            SET search_path = auth, pg_temp, public
            AS $$
            BEGIN
                RETURN event;
            END;
            $$;
        "#;
        let schema = parse_sql_string(sql).unwrap();
        let func = schema
            .functions
            .get("auth.custom_access_token_hook(jsonb)")
            .unwrap();
        assert_eq!(func.schema, "auth");
        assert_eq!(func.name, "custom_access_token_hook");
        assert_eq!(func.language, "plpgsql");
        assert_eq!(func.security, SecurityType::Definer);
    }

    #[test]
    fn parses_function_with_security_invoker() {
        let sql = r#"
            CREATE FUNCTION public.safe_func() RETURNS INTEGER
            LANGUAGE sql SECURITY INVOKER
            AS $$ SELECT 1 $$;
        "#;
        let schema = parse_sql_string(sql).unwrap();
        let func = schema.functions.get("public.safe_func()").unwrap();
        assert_eq!(func.security, SecurityType::Invoker);
    }

    #[test]
    fn parses_function_without_security_defaults_to_invoker() {
        let sql = r#"
            CREATE FUNCTION public.default_func() RETURNS INTEGER
            LANGUAGE sql AS $$ SELECT 1 $$;
        "#;
        let schema = parse_sql_string(sql).unwrap();
        let func = schema.functions.get("public.default_func()").unwrap();
        assert_eq!(func.security, SecurityType::Invoker);
    }

    #[test]
    fn parses_qualified_enum_name() {
        let sql = "CREATE TYPE auth.role AS ENUM ('admin', 'user');";
        let schema = parse_sql_string(sql).unwrap();
        let enum_type = schema.enums.get("auth.role").unwrap();
        assert_eq!(enum_type.schema, "auth");
        assert_eq!(enum_type.name, "role");
    }

    #[test]
    fn parses_simple_trigger() {
        let sql = r#"
CREATE FUNCTION audit_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RETURN NEW;
END;
$$;

CREATE TRIGGER audit_trigger
    AFTER INSERT ON users
    FOR EACH ROW
    EXECUTE FUNCTION audit_fn();
"#;
        let schema = parse_sql_string(sql).unwrap();
        assert_eq!(schema.triggers.len(), 1);

        let trigger = schema.triggers.get("public.users.audit_trigger").unwrap();
        assert_eq!(trigger.name, "audit_trigger");
        assert_eq!(trigger.target_schema, "public");
        assert_eq!(trigger.target_name, "users");
        assert_eq!(trigger.timing, TriggerTiming::After);
        assert_eq!(trigger.events, vec![TriggerEvent::Insert]);
        assert!(trigger.for_each_row);
        assert_eq!(trigger.function_name, "audit_fn");
    }

    #[test]
    fn parses_trigger_with_update_of_columns() {
        let sql = r#"
CREATE FUNCTION notify_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;

CREATE TRIGGER notify_email_change
    BEFORE UPDATE OF email, name ON users
    FOR EACH ROW
    EXECUTE FUNCTION notify_fn();
"#;
        let schema = parse_sql_string(sql).unwrap();
        let trigger = schema
            .triggers
            .get("public.users.notify_email_change")
            .unwrap();

        assert_eq!(trigger.timing, TriggerTiming::Before);
        assert_eq!(trigger.events, vec![TriggerEvent::Update]);
        assert_eq!(trigger.update_columns, vec!["email", "name"]);
    }

    #[test]
    fn parses_trigger_with_multiple_events() {
        let sql = r#"
CREATE FUNCTION log_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;

CREATE TRIGGER log_changes
    AFTER INSERT OR UPDATE OR DELETE ON orders
    FOR EACH ROW
    EXECUTE FUNCTION log_fn();
"#;
        let schema = parse_sql_string(sql).unwrap();
        let trigger = schema.triggers.get("public.orders.log_changes").unwrap();

        assert_eq!(trigger.events.len(), 3);
        assert!(trigger.events.contains(&TriggerEvent::Insert));
        assert!(trigger.events.contains(&TriggerEvent::Update));
        assert!(trigger.events.contains(&TriggerEvent::Delete));
    }

    #[test]
    fn parses_trigger_with_when_clause() {
        let sql = r#"
CREATE FUNCTION check_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;

CREATE TRIGGER check_amount
    BEFORE INSERT ON orders
    FOR EACH ROW
    WHEN (NEW.amount > 1000)
    EXECUTE FUNCTION check_fn();
"#;
        let schema = parse_sql_string(sql).unwrap();
        let trigger = schema.triggers.get("public.orders.check_amount").unwrap();

        assert!(trigger.when_clause.is_some());
        assert!(trigger.when_clause.as_ref().unwrap().contains("amount"));
    }

    #[test]
    fn parses_trigger_for_each_statement() {
        let sql = r#"
CREATE FUNCTION batch_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END; $$;

CREATE TRIGGER batch_notify
    AFTER INSERT ON events
    FOR EACH STATEMENT
    EXECUTE FUNCTION batch_fn();
"#;
        let schema = parse_sql_string(sql).unwrap();
        let trigger = schema.triggers.get("public.events.batch_notify").unwrap();

        assert!(!trigger.for_each_row);
    }

    #[test]
    fn parses_instead_of_trigger_on_view() {
        let sql = r#"
CREATE VIEW active_users AS SELECT * FROM users WHERE active = true;

CREATE FUNCTION insert_active_user_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO users (name, active) VALUES (NEW.name, true);
    RETURN NEW;
END;
$$;

CREATE TRIGGER insert_active_user
    INSTEAD OF INSERT ON active_users
    FOR EACH ROW
    EXECUTE FUNCTION insert_active_user_fn();
"#;
        let schema = parse_sql_string(sql).unwrap();
        assert_eq!(schema.triggers.len(), 1);

        let trigger = schema
            .triggers
            .get("public.active_users.insert_active_user")
            .unwrap();
        assert_eq!(trigger.name, "insert_active_user");
        assert_eq!(trigger.target_schema, "public");
        assert_eq!(trigger.target_name, "active_users");
        assert_eq!(trigger.timing, TriggerTiming::InsteadOf);
        assert_eq!(trigger.events, vec![TriggerEvent::Insert]);
        assert!(trigger.for_each_row);
        assert!(trigger.when_clause.is_none());
        assert_eq!(trigger.function_name, "insert_active_user_fn");
    }

    #[test]
    fn instead_of_trigger_rejects_for_each_statement() {
        let sql = r#"
CREATE VIEW active_users AS SELECT * FROM users WHERE active = true;

CREATE FUNCTION insert_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;

CREATE TRIGGER bad_trigger
    INSTEAD OF INSERT ON active_users
    FOR EACH STATEMENT
    EXECUTE FUNCTION insert_fn();
"#;
        let result = parse_sql_string(sql);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("must be FOR EACH ROW"), "Error: {err}");
    }

    #[test]
    fn instead_of_trigger_rejects_when_clause() {
        let sql = r#"
CREATE VIEW active_users AS SELECT * FROM users WHERE active = true;

CREATE FUNCTION insert_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;

CREATE TRIGGER bad_trigger
    INSTEAD OF INSERT ON active_users
    FOR EACH ROW
    WHEN (NEW.id > 0)
    EXECUTE FUNCTION insert_fn();
"#;
        let result = parse_sql_string(sql);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("cannot have a WHEN clause"), "Error: {err}");
    }

    #[test]
    fn parse_create_sequence_minimal() {
        let sql = "CREATE SEQUENCE users_id_seq;";
        let schema = parse_sql_string(sql).unwrap();
        assert!(schema.sequences.contains_key("public.users_id_seq"));
        let seq = schema.sequences.get("public.users_id_seq").unwrap();
        assert_eq!(seq.name, "users_id_seq");
        assert_eq!(seq.schema, "public");
    }

    #[test]
    fn parse_create_sequence_with_schema() {
        let sql = "CREATE SEQUENCE auth.counter_seq;";
        let schema = parse_sql_string(sql).unwrap();
        assert!(schema.sequences.contains_key("auth.counter_seq"));
    }

    #[test]
    fn parse_create_sequence_with_data_type() {
        let sql = "CREATE SEQUENCE myschema.counter_seq AS bigint;";
        let schema = parse_sql_string(sql).unwrap();
        let seq = schema.sequences.get("myschema.counter_seq").unwrap();
        assert_eq!(seq.data_type, SequenceDataType::BigInt);
    }

    #[test]
    fn parse_create_sequence_with_start() {
        let sql = "CREATE SEQUENCE myschema.counter_seq START WITH 100;";
        let schema = parse_sql_string(sql).unwrap();
        let seq = schema.sequences.get("myschema.counter_seq").unwrap();
        assert_eq!(seq.start, Some(100));
    }

    #[test]
    fn parse_create_sequence_with_increment() {
        let sql = "CREATE SEQUENCE myschema.counter_seq INCREMENT BY 5;";
        let schema = parse_sql_string(sql).unwrap();
        let seq = schema.sequences.get("myschema.counter_seq").unwrap();
        assert_eq!(seq.increment, Some(5));
    }

    #[test]
    fn parse_create_sequence_owned_by() {
        let sql = "CREATE SEQUENCE public.users_id_seq OWNED BY public.users.id;";
        let schema = parse_sql_string(sql).unwrap();
        let seq = schema.sequences.get("public.users_id_seq").unwrap();
        let owner = seq.owned_by.as_ref().unwrap();
        assert_eq!(owner.table_schema, "public");
        assert_eq!(owner.table_name, "users");
        assert_eq!(owner.column_name, "id");
    }

    #[test]
    fn parse_create_sequence_with_negative_start() {
        let sql = "CREATE SEQUENCE test.desc_seq START WITH -1;";
        let schema = parse_sql_string(sql).unwrap();
        let seq = schema.sequences.get("test.desc_seq").unwrap();
        assert_eq!(seq.start, Some(-1));
    }

    #[test]
    fn parse_create_sequence_with_negative_increment() {
        let sql = "CREATE SEQUENCE test.desc_seq INCREMENT BY -1;";
        let schema = parse_sql_string(sql).unwrap();
        let seq = schema.sequences.get("test.desc_seq").unwrap();
        assert_eq!(seq.increment, Some(-1));
    }

    #[test]
    fn parse_create_sequence_with_negative_minvalue() {
        let sql = "CREATE SEQUENCE test.desc_seq MINVALUE -1000;";
        let schema = parse_sql_string(sql).unwrap();
        let seq = schema.sequences.get("test.desc_seq").unwrap();
        assert_eq!(seq.min_value, Some(-1000));
    }

    #[test]
    fn parse_create_sequence_descending_defaults() {
        let sql = "CREATE SEQUENCE public.desc_seq INCREMENT BY -1;";
        let schema = parse_sql_string(sql).unwrap();
        let seq = schema.sequences.get("public.desc_seq").unwrap();
        assert_eq!(seq.increment, Some(-1));
        assert_eq!(seq.min_value, Some(-2147483648));
        assert_eq!(seq.max_value, Some(-1));
        assert_eq!(seq.start, Some(-1));
    }

    #[test]
    fn parse_sequence_postgresql_order() {
        // PostgreSQL order: INCREMENT BY before START WITH
        let sql = "CREATE SEQUENCE seq INCREMENT BY 1 START WITH 1;";
        let result = parse_sql_string(sql);
        assert!(result.is_ok(), "PostgreSQL order should work: {result:?}");
    }

    #[test]
    fn parse_alter_sequence_not_supported() {
        // sqlparser doesn't support ALTER SEQUENCE
        let sql = r#"ALTER SEQUENCE "public"."seq" OWNED BY "public"."users"."id";"#;
        let result = parse_sql_string(sql);
        // ALTER SEQUENCE is preprocessed out, so should parse OK (empty schema)
        assert!(result.is_ok());
    }

    #[test]
    fn parse_create_sequence_full_options_with_owned_by() {
        // Full sequence with all options including OWNED BY inline
        let sql = r#"CREATE SEQUENCE "public"."user_id_seq" AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 OWNED BY "public"."users"."id";"#;
        let result = parse_sql_string(sql);
        assert!(
            result.is_ok(),
            "Full CREATE SEQUENCE should parse: {result:?}"
        );
        let schema = result.unwrap();
        let seq = schema.sequences.get("public.user_id_seq").unwrap();
        assert!(seq.owned_by.is_some());
    }

    #[test]
    fn is_serial_type_detection() {
        use sqlparser::ast::DataType;
        use sqlparser::ast::Ident;
        use sqlparser::ast::ObjectName;
        use sqlparser::ast::ObjectNamePart;

        // SERIAL
        let serial = DataType::Custom(
            ObjectName(vec![ObjectNamePart::Identifier(Ident::new("serial"))]),
            vec![],
        );
        assert_eq!(detect_serial_type(&serial), Some(SequenceDataType::Integer));

        // BIGSERIAL
        let bigserial = DataType::Custom(
            ObjectName(vec![ObjectNamePart::Identifier(Ident::new("bigserial"))]),
            vec![],
        );
        assert_eq!(
            detect_serial_type(&bigserial),
            Some(SequenceDataType::BigInt)
        );

        // SMALLSERIAL
        let smallserial = DataType::Custom(
            ObjectName(vec![ObjectNamePart::Identifier(Ident::new("smallserial"))]),
            vec![],
        );
        assert_eq!(
            detect_serial_type(&smallserial),
            Some(SequenceDataType::SmallInt)
        );

        // Not serial
        let integer = DataType::Integer(None);
        assert_eq!(detect_serial_type(&integer), None);
    }

    #[test]
    fn parse_serial_column_creates_sequence() {
        let sql = "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);";
        let schema = parse_sql_string(sql).unwrap();

        // Table should exist with integer column
        assert!(schema.tables.contains_key("public.users"));
        let table = schema.tables.get("public.users").unwrap();
        let id_col = table.columns.get("id").unwrap();
        assert_eq!(id_col.data_type, PgType::Integer);
        assert_eq!(
            id_col.default,
            Some("nextval('public.users_id_seq'::regclass)".to_string())
        );

        // Sequence should exist
        assert!(schema.sequences.contains_key("public.users_id_seq"));
        let seq = schema.sequences.get("public.users_id_seq").unwrap();
        assert_eq!(seq.data_type, SequenceDataType::Integer);
        assert!(seq.owned_by.is_some());
        let owner = seq.owned_by.as_ref().unwrap();
        assert_eq!(owner.table_schema, "public");
        assert_eq!(owner.table_name, "users");
        assert_eq!(owner.column_name, "id");
    }

    #[test]
    fn parse_serial_ignores_explicit_default() {
        let sql = "CREATE TABLE test (id SERIAL DEFAULT 999);";
        let schema = parse_sql_string(sql).unwrap();
        let table = schema.tables.get("public.test").unwrap();
        let col = table.columns.get("id").unwrap();
        assert_eq!(
            col.default,
            Some("nextval('public.test_id_seq'::regclass)".to_string())
        );
    }

    #[test]
    fn parse_bigserial_column() {
        let sql = "CREATE TABLE events (id BIGSERIAL PRIMARY KEY);";
        let schema = parse_sql_string(sql).unwrap();

        let table = schema.tables.get("public.events").unwrap();
        let id_col = table.columns.get("id").unwrap();
        assert_eq!(id_col.data_type, PgType::BigInt);

        let seq = schema.sequences.get("public.events_id_seq").unwrap();
        assert_eq!(seq.data_type, SequenceDataType::BigInt);
    }

    #[test]
    fn parse_smallserial_column() {
        let sql = "CREATE TABLE counters (id SMALLSERIAL PRIMARY KEY);";
        let schema = parse_sql_string(sql).unwrap();

        let table = schema.tables.get("public.counters").unwrap();
        let id_col = table.columns.get("id").unwrap();
        assert_eq!(id_col.data_type, PgType::SmallInt);

        let seq = schema.sequences.get("public.counters_id_seq").unwrap();
        assert_eq!(seq.data_type, SequenceDataType::SmallInt);
    }

    #[test]
    fn parse_serial_with_schema() {
        let sql = "CREATE TABLE auth.users (id SERIAL PRIMARY KEY, name TEXT);";
        let schema = parse_sql_string(sql).unwrap();

        assert!(schema.tables.contains_key("auth.users"));
        let table = schema.tables.get("auth.users").unwrap();
        let id_col = table.columns.get("id").unwrap();
        assert_eq!(
            id_col.default,
            Some("nextval('auth.users_id_seq'::regclass)".to_string())
        );

        assert!(schema.sequences.contains_key("auth.users_id_seq"));
        let seq = schema.sequences.get("auth.users_id_seq").unwrap();
        assert_eq!(seq.schema, "auth");
        let owner = seq.owned_by.as_ref().unwrap();
        assert_eq!(owner.table_schema, "auth");
    }

    #[test]
    fn trigger_enabled_by_default() {
        let sql = r#"
CREATE FUNCTION audit_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
CREATE TRIGGER audit_trigger AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION audit_fn();
"#;
        let schema = parse_sql_string(sql).unwrap();
        let trigger = schema.triggers.get("public.users.audit_trigger").unwrap();
        assert_eq!(trigger.enabled, TriggerEnabled::Origin);
    }

    #[test]
    fn parses_disable_trigger() {
        let sql = r#"
CREATE FUNCTION audit_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
CREATE TRIGGER audit_trigger AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION audit_fn();
ALTER TABLE users DISABLE TRIGGER audit_trigger;
"#;
        let schema = parse_sql_string(sql).unwrap();
        let trigger = schema.triggers.get("public.users.audit_trigger").unwrap();
        assert_eq!(trigger.enabled, TriggerEnabled::Disabled);
    }

    #[test]
    fn parses_enable_trigger() {
        let sql = r#"
CREATE FUNCTION audit_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
CREATE TRIGGER audit_trigger AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION audit_fn();
ALTER TABLE users DISABLE TRIGGER audit_trigger;
ALTER TABLE users ENABLE TRIGGER audit_trigger;
"#;
        let schema = parse_sql_string(sql).unwrap();
        let trigger = schema.triggers.get("public.users.audit_trigger").unwrap();
        assert_eq!(trigger.enabled, TriggerEnabled::Origin);
    }

    #[test]
    fn parses_enable_replica_trigger() {
        let sql = r#"
CREATE FUNCTION audit_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
CREATE TRIGGER audit_trigger AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION audit_fn();
ALTER TABLE users ENABLE REPLICA TRIGGER audit_trigger;
"#;
        let schema = parse_sql_string(sql).unwrap();
        let trigger = schema.triggers.get("public.users.audit_trigger").unwrap();
        assert_eq!(trigger.enabled, TriggerEnabled::Replica);
    }

    #[test]
    fn parses_enable_always_trigger() {
        let sql = r#"
CREATE FUNCTION audit_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
CREATE TRIGGER audit_trigger AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION audit_fn();
ALTER TABLE users ENABLE ALWAYS TRIGGER audit_trigger;
"#;
        let schema = parse_sql_string(sql).unwrap();
        let trigger = schema.triggers.get("public.users.audit_trigger").unwrap();
        assert_eq!(trigger.enabled, TriggerEnabled::Always);
    }

    #[test]
    fn parses_disable_trigger_with_schema() {
        let sql = r#"
CREATE FUNCTION audit_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
CREATE TRIGGER audit_trigger AFTER INSERT ON myschema.users FOR EACH ROW EXECUTE FUNCTION audit_fn();
ALTER TABLE myschema.users DISABLE TRIGGER audit_trigger;
"#;
        let schema = parse_sql_string(sql).unwrap();
        let trigger = schema.triggers.get("myschema.users.audit_trigger").unwrap();
        assert_eq!(trigger.enabled, TriggerEnabled::Disabled);
    }

    #[test]
    fn parses_trigger_with_old_table() {
        let sql = r#"
CREATE FUNCTION audit_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN OLD; END; $$;
CREATE TRIGGER audit_deletes
    AFTER DELETE ON users
    REFERENCING OLD TABLE AS deleted_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION audit_fn();
"#;
        let schema = parse_sql_string(sql).unwrap();
        let trigger = schema.triggers.get("public.users.audit_deletes").unwrap();
        assert_eq!(trigger.old_table_name, Some("deleted_rows".to_string()));
        assert_eq!(trigger.new_table_name, None);
    }

    #[test]
    fn parses_trigger_with_new_table() {
        let sql = r#"
CREATE FUNCTION audit_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
CREATE TRIGGER audit_inserts
    AFTER INSERT ON users
    REFERENCING NEW TABLE AS inserted_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION audit_fn();
"#;
        let schema = parse_sql_string(sql).unwrap();
        let trigger = schema.triggers.get("public.users.audit_inserts").unwrap();
        assert_eq!(trigger.old_table_name, None);
        assert_eq!(trigger.new_table_name, Some("inserted_rows".to_string()));
    }

    #[test]
    fn parses_trigger_with_both_transition_tables() {
        let sql = r#"
CREATE FUNCTION audit_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
CREATE TRIGGER audit_updates
    AFTER UPDATE ON users
    REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION audit_fn();
"#;
        let schema = parse_sql_string(sql).unwrap();
        let trigger = schema.triggers.get("public.users.audit_updates").unwrap();
        assert_eq!(trigger.old_table_name, Some("old_rows".to_string()));
        assert_eq!(trigger.new_table_name, Some("new_rows".to_string()));
    }

    #[test]
    fn rejects_referencing_on_before_trigger() {
        let sql = r#"
CREATE FUNCTION audit_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
CREATE TRIGGER bad_trigger
    BEFORE INSERT ON users
    REFERENCING NEW TABLE AS new_rows
    FOR EACH ROW
    EXECUTE FUNCTION audit_fn();
"#;
        let result = parse_sql_string(sql);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("REFERENCING") && err.contains("AFTER"));
    }

    #[test]
    fn rejects_referencing_on_instead_of_trigger() {
        let sql = r#"
CREATE VIEW user_view AS SELECT id, name FROM users;
CREATE FUNCTION insert_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
CREATE TRIGGER bad_trigger
    INSTEAD OF INSERT ON user_view
    REFERENCING NEW TABLE AS new_rows
    FOR EACH ROW
    EXECUTE FUNCTION insert_fn();
"#;
        let result = parse_sql_string(sql);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("REFERENCING") || err.contains("INSTEAD OF"));
    }

    #[test]
    fn rejects_old_table_on_insert_only_trigger() {
        let sql = r#"
CREATE FUNCTION audit_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
CREATE TRIGGER bad_trigger
    AFTER INSERT ON users
    REFERENCING OLD TABLE AS old_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION audit_fn();
"#;
        let result = parse_sql_string(sql);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("OLD TABLE")
                && (err.contains("INSERT") || err.contains("UPDATE") || err.contains("DELETE"))
        );
    }

    #[test]
    fn rejects_new_table_on_delete_only_trigger() {
        let sql = r#"
CREATE FUNCTION audit_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN OLD; END; $$;
CREATE TRIGGER bad_trigger
    AFTER DELETE ON users
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION audit_fn();
"#;
        let result = parse_sql_string(sql);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("NEW TABLE")
                && (err.contains("INSERT") || err.contains("UPDATE") || err.contains("DELETE"))
        );
    }

    #[test]
    fn parses_partition_by_range() {
        let sql = r#"
CREATE TABLE measurement (
    city_id INT NOT NULL,
    logdate DATE NOT NULL,
    peaktemp INT,
    unitsales INT
) PARTITION BY RANGE (logdate);
"#;
        let schema = parse_sql_string(sql).unwrap();
        let table = schema.tables.get("public.measurement").unwrap();

        let partition_by = table
            .partition_by
            .as_ref()
            .expect("partition_by should be set");
        assert_eq!(partition_by.strategy, PartitionStrategy::Range);
        assert_eq!(partition_by.columns, vec!["logdate".to_string()]);
    }

    #[test]
    fn parses_partition_by_list() {
        let sql = r#"
CREATE TABLE customers (
    id INT NOT NULL,
    status TEXT NOT NULL,
    name TEXT
) PARTITION BY LIST (status);
"#;
        let schema = parse_sql_string(sql).unwrap();
        let table = schema.tables.get("public.customers").unwrap();

        let partition_by = table
            .partition_by
            .as_ref()
            .expect("partition_by should be set");
        assert_eq!(partition_by.strategy, PartitionStrategy::List);
        assert_eq!(partition_by.columns, vec!["status".to_string()]);
    }

    #[test]
    fn parses_partition_by_hash() {
        let sql = r#"
CREATE TABLE orders (
    id INT NOT NULL,
    customer_id INT NOT NULL,
    created_at TIMESTAMP NOT NULL
) PARTITION BY HASH (id);
"#;
        let schema = parse_sql_string(sql).unwrap();
        let table = schema.tables.get("public.orders").unwrap();

        let partition_by = table
            .partition_by
            .as_ref()
            .expect("partition_by should be set");
        assert_eq!(partition_by.strategy, PartitionStrategy::Hash);
        assert_eq!(partition_by.columns, vec!["id".to_string()]);
    }

    #[test]
    fn parses_partition_by_multiple_columns() {
        let sql = r#"
CREATE TABLE events (
    region TEXT NOT NULL,
    event_date DATE NOT NULL,
    event_id INT NOT NULL
) PARTITION BY RANGE (region, event_date);
"#;
        let schema = parse_sql_string(sql).unwrap();
        let table = schema.tables.get("public.events").unwrap();

        let partition_by = table
            .partition_by
            .as_ref()
            .expect("partition_by should be set");
        assert_eq!(partition_by.strategy, PartitionStrategy::Range);
        assert_eq!(
            partition_by.columns,
            vec!["region".to_string(), "event_date".to_string()]
        );
    }

    #[test]
    fn parses_range_partition() {
        let sql = r#"
CREATE TABLE measurement (
    city_id INT NOT NULL,
    logdate DATE NOT NULL
) PARTITION BY RANGE (logdate);

CREATE TABLE measurement_2024 PARTITION OF measurement
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
"#;
        let schema = parse_sql_string(sql).unwrap();

        let partition = schema
            .partitions
            .get("public.measurement_2024")
            .expect("partition should exist");
        assert_eq!(partition.parent_schema, "public");
        assert_eq!(partition.parent_name, "measurement");
        match &partition.bound {
            PartitionBound::Range { from, to } => {
                assert_eq!(from, &vec!["'2024-01-01'".to_string()]);
                assert_eq!(to, &vec!["'2025-01-01'".to_string()]);
            }
            _ => panic!("Expected Range bound"),
        }
    }

    #[test]
    fn parses_list_partition() {
        let sql = r#"
CREATE TABLE customers (
    id INT NOT NULL,
    status TEXT NOT NULL
) PARTITION BY LIST (status);

CREATE TABLE customers_active PARTITION OF customers
    FOR VALUES IN ('active', 'pending');
"#;
        let schema = parse_sql_string(sql).unwrap();

        let partition = schema
            .partitions
            .get("public.customers_active")
            .expect("partition should exist");
        match &partition.bound {
            PartitionBound::List { values } => {
                assert_eq!(
                    values,
                    &vec!["'active'".to_string(), "'pending'".to_string()]
                );
            }
            _ => panic!("Expected List bound"),
        }
    }

    #[test]
    fn parses_hash_partition() {
        let sql = r#"
CREATE TABLE orders (
    id INT NOT NULL
) PARTITION BY HASH (id);

CREATE TABLE orders_part1 PARTITION OF orders
    FOR VALUES WITH (MODULUS 4, REMAINDER 0);
"#;
        let schema = parse_sql_string(sql).unwrap();

        let partition = schema
            .partitions
            .get("public.orders_part1")
            .expect("partition should exist");
        match &partition.bound {
            PartitionBound::Hash { modulus, remainder } => {
                assert_eq!(*modulus, 4);
                assert_eq!(*remainder, 0);
            }
            _ => panic!("Expected Hash bound"),
        }
    }

    #[test]
    fn parses_default_partition() {
        let sql = r#"
CREATE TABLE logs (
    id INT NOT NULL,
    level TEXT NOT NULL
) PARTITION BY LIST (level);

CREATE TABLE logs_other PARTITION OF logs DEFAULT;
"#;
        let schema = parse_sql_string(sql).unwrap();

        let partition = schema
            .partitions
            .get("public.logs_other")
            .expect("partition should exist");
        assert_eq!(partition.bound, PartitionBound::Default);
    }

    #[test]
    fn parses_partition_with_schema() {
        let sql = r#"
CREATE TABLE analytics.events (
    id INT NOT NULL,
    occurred_at DATE NOT NULL
) PARTITION BY RANGE (occurred_at);

CREATE TABLE analytics.events_2024 PARTITION OF analytics.events
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
"#;
        let schema = parse_sql_string(sql).unwrap();

        let table = schema.tables.get("analytics.events").unwrap();
        assert!(table.partition_by.is_some());

        let partition = schema
            .partitions
            .get("analytics.events_2024")
            .expect("partition should exist");
        assert_eq!(partition.schema, "analytics");
        assert_eq!(partition.parent_schema, "analytics");
        assert_eq!(partition.parent_name, "events");
    }

    #[test]
    fn parses_simple_domain() {
        let sql = "CREATE DOMAIN email_address AS TEXT;";

        let schema = parse_sql_string(sql).expect("Should parse");

        assert_eq!(schema.domains.len(), 1);
        assert!(schema.domains.contains_key("public.email_address"));

        let domain = &schema.domains["public.email_address"];
        assert_eq!(domain.name, "email_address");
        assert_eq!(domain.schema, "public");
        assert!(!domain.not_null);
        assert!(domain.default.is_none());
        assert!(domain.check_constraints.is_empty());
    }

    #[test]
    fn parses_domain_with_check_constraint() {
        let sql = "CREATE DOMAIN email_address AS TEXT CHECK (VALUE ~ '@');";

        let schema = parse_sql_string(sql).expect("Should parse");

        let domain = &schema.domains["public.email_address"];
        assert_eq!(domain.check_constraints.len(), 1);
        assert!(domain.check_constraints[0].expression.contains("@"));
    }

    #[test]
    fn parses_domain_with_named_constraint() {
        let sql =
            "CREATE DOMAIN positive_int AS INTEGER CONSTRAINT must_be_positive CHECK (VALUE > 0);";

        let schema = parse_sql_string(sql).expect("Should parse");

        let domain = &schema.domains["public.positive_int"];
        assert_eq!(domain.data_type, PgType::Integer);
        assert_eq!(domain.check_constraints.len(), 1);
        assert_eq!(
            domain.check_constraints[0].name.as_deref(),
            Some("must_be_positive")
        );
    }

    #[test]
    fn parses_domain_with_default() {
        let sql = "CREATE DOMAIN status AS TEXT DEFAULT 'pending';";

        let schema = parse_sql_string(sql).expect("Should parse");

        let domain = &schema.domains["public.status"];
        assert_eq!(domain.default.as_deref(), Some("'pending'"));
    }

    #[test]
    fn parses_domain_with_collation() {
        let sql = r#"CREATE DOMAIN case_insensitive AS TEXT COLLATE "en_US";"#;

        let schema = parse_sql_string(sql).expect("Should parse");

        let domain = &schema.domains["public.case_insensitive"];
        assert!(domain.collation.is_some());
    }

    #[test]
    fn parses_domain_full_syntax() {
        let sql = r#"
CREATE DOMAIN us_postal_code AS TEXT
    COLLATE "en_US"
    DEFAULT '00000'
    CONSTRAINT valid_format CHECK (VALUE ~ '^\d{5}(-\d{4})?$');
"#;

        let schema = parse_sql_string(sql).expect("Should parse");

        let domain = &schema.domains["public.us_postal_code"];
        assert_eq!(domain.name, "us_postal_code");
        assert_eq!(domain.data_type, PgType::Text);
        assert!(domain.collation.is_some());
        assert_eq!(domain.default.as_deref(), Some("'00000'"));
        assert_eq!(domain.check_constraints.len(), 1);
        assert_eq!(
            domain.check_constraints[0].name.as_deref(),
            Some("valid_format")
        );
    }

    #[test]
    fn parses_domain_with_schema() {
        let sql = "CREATE DOMAIN auth.email AS TEXT CHECK (VALUE ~ '@');";

        let schema = parse_sql_string(sql).expect("Should parse");

        assert!(schema.domains.contains_key("auth.email"));
        let domain = &schema.domains["auth.email"];
        assert_eq!(domain.schema, "auth");
        assert_eq!(domain.name, "email");
    }

    #[test]
    fn domain_round_trip_with_check_constraint() {
        use crate::dump::generate_dump;

        let mut schema = Schema::new();
        schema.domains.insert(
            "public.email_address".to_string(),
            Domain {
                schema: "public".to_string(),
                name: "email_address".to_string(),
                data_type: PgType::Text,
                default: None,
                not_null: false,
                collation: None,
                check_constraints: vec![DomainConstraint {
                    name: None,
                    expression: "VALUE ~ '@'".to_string(),
                }],
            },
        );

        let fingerprint_before = schema.fingerprint();
        let sql = generate_dump(&schema, None);
        let parsed = parse_sql_string(&sql).expect("Should parse generated SQL");
        let fingerprint_after = parsed.fingerprint();

        assert_eq!(
            fingerprint_before, fingerprint_after,
            "Domain should round-trip correctly"
        );
        assert_eq!(parsed.domains.len(), 1);
        let parsed_domain = &parsed.domains["public.email_address"];
        assert_eq!(parsed_domain.data_type, PgType::Text);
        assert_eq!(parsed_domain.check_constraints.len(), 1);
    }

    #[test]
    fn domain_round_trip_with_table_using_domain() {
        use crate::dump::generate_dump;

        let mut schema = Schema::new();
        schema.domains.insert(
            "public.email_address".to_string(),
            Domain {
                schema: "public".to_string(),
                name: "email_address".to_string(),
                data_type: PgType::Text,
                default: None,
                not_null: false,
                collation: None,
                check_constraints: vec![DomainConstraint {
                    name: None,
                    expression: "VALUE ~ '@'".to_string(),
                }],
            },
        );

        let mut users_columns = BTreeMap::new();
        users_columns.insert(
            "id".to_string(),
            Column {
                name: "id".to_string(),
                data_type: PgType::BigInt,
                nullable: false,
                default: None,
                comment: None,
            },
        );
        users_columns.insert(
            "email".to_string(),
            Column {
                name: "email".to_string(),
                data_type: PgType::CustomEnum("public.email_address".to_string()),
                nullable: false,
                default: None,
                comment: None,
            },
        );

        schema.tables.insert(
            "public.users".to_string(),
            Table {
                schema: "public".to_string(),
                name: "users".to_string(),
                columns: users_columns,
                primary_key: Some(PrimaryKey {
                    columns: vec!["id".to_string()],
                }),
                indexes: Vec::new(),
                foreign_keys: Vec::new(),
                check_constraints: Vec::new(),
                comment: None,
                row_level_security: false,
                policies: Vec::new(),
                partition_by: None,
            },
        );

        let fingerprint_before = schema.fingerprint();
        let sql = generate_dump(&schema, None);
        let parsed = parse_sql_string(&sql).expect("Should parse generated SQL");
        let fingerprint_after = parsed.fingerprint();

        assert_eq!(
            fingerprint_before, fingerprint_after,
            "Domain and table should round-trip correctly"
        );
    }

    #[test]
    fn parses_policy_with_quoted_role_names() {
        let sql = r#"
            CREATE TABLE users (id BIGINT PRIMARY KEY);
            ALTER TABLE users ENABLE ROW LEVEL SECURITY;
            CREATE POLICY admin_policy ON users FOR ALL TO "authenticated" USING (true);
        "#;
        let schema = parse_sql_string(sql).unwrap();
        let table = schema.tables.get("public.users").unwrap();
        let policy = &table.policies[0];

        assert_eq!(policy.roles.len(), 1);
        assert_eq!(
            policy.roles[0], "authenticated",
            "Role name should not have quotes"
        );
    }

    #[test]
    fn parses_policy_before_table_in_same_file() {
        // Bug fix: policies should work regardless of statement order
        let sql = r#"
            CREATE POLICY admin_policy ON users FOR ALL TO "authenticated" USING (true);
            CREATE TABLE users (id BIGINT PRIMARY KEY);
            ALTER TABLE users ENABLE ROW LEVEL SECURITY;
        "#;
        let schema = parse_sql_string(sql).unwrap();
        let table = schema.tables.get("public.users").unwrap();

        assert_eq!(
            table.policies.len(),
            1,
            "Policy should be associated with table"
        );
        assert_eq!(table.policies[0].name, "admin_policy");
        assert_eq!(table.policies[0].roles, vec!["authenticated"]);
    }

    #[test]
    fn parses_multiple_policies_different_order() {
        // Mix of policies before and after table definition
        let sql = r#"
            CREATE POLICY first_policy ON users FOR SELECT USING (true);
            CREATE TABLE users (id BIGINT PRIMARY KEY, role TEXT);
            CREATE POLICY second_policy ON users FOR INSERT WITH CHECK (role = 'admin');
            ALTER TABLE users ENABLE ROW LEVEL SECURITY;
        "#;
        let schema = parse_sql_string(sql).unwrap();
        let table = schema.tables.get("public.users").unwrap();

        assert_eq!(
            table.policies.len(),
            2,
            "Both policies should be associated"
        );
        // Policies are sorted by name
        let names: Vec<&str> = table.policies.iter().map(|p| p.name.as_str()).collect();
        assert!(names.contains(&"first_policy"));
        assert!(names.contains(&"second_policy"));
    }

    #[test]
    fn policy_references_nonexistent_table_errors() {
        let sql = r#"
            CREATE POLICY orphan_policy ON nonexistent_table FOR ALL USING (true);
        "#;
        let result = parse_sql_string(sql);
        // The policy references a non-existent table, which should result in pending_policies
        // being non-empty, but parse_sql_string uses finalize_partial which doesn't error
        let schema = result.unwrap();
        assert!(
            schema.pending_policies.len() == 1,
            "Orphaned policy should remain in pending"
        );
        assert_eq!(schema.pending_policies[0].name, "orphan_policy");
    }

    #[test]
    fn parses_check_constraint_from_alter_table() {
        let sql = r#"
            CREATE TABLE products (id BIGINT PRIMARY KEY, price INTEGER);
            ALTER TABLE products ADD CONSTRAINT price_positive CHECK (price > 0);
        "#;
        let schema = parse_sql_string(sql).unwrap();
        let table = schema.tables.get("public.products").unwrap();

        assert_eq!(table.check_constraints.len(), 1);
        assert_eq!(table.check_constraints[0].name, "price_positive");
        assert!(table.check_constraints[0].expression.contains("price > 0"));
    }

    #[test]
    fn parses_function_with_quoted_parameter_names() {
        let sql = r#"
            CREATE FUNCTION auth.is_org_admin("p_role_name" text, "p_enterprise_id" uuid)
            RETURNS boolean LANGUAGE sql AS $$ SELECT true $$;
        "#;
        let schema = parse_sql_string(sql).unwrap();
        let func = schema
            .functions
            .get("auth.is_org_admin(text, uuid)")
            .unwrap();

        assert_eq!(func.arguments[0].name, Some("p_role_name".to_string()));
        assert_eq!(func.arguments[1].name, Some("p_enterprise_id".to_string()));
    }

    #[test]
    fn type_casts_normalized_to_lowercase() {
        let sql = r#"
            CREATE TABLE users (
                id BIGINT,
                role TEXT DEFAULT 'admin'::TEXT
            );
            CREATE POLICY user_policy ON users
                FOR ALL
                USING (role = 'admin'::TEXT)
                WITH CHECK (role = 'user'::VARCHAR);
            ALTER TABLE users ADD CONSTRAINT role_check CHECK (role IN ('admin'::TEXT, 'user'::TEXT));
        "#;
        let schema = parse_sql_string(sql).unwrap();

        let table = schema.tables.get("public.users").unwrap();

        let role_col = table.columns.get("role").unwrap();
        assert_eq!(
            role_col.default,
            Some("'admin'::text".to_string()),
            "Column default type casts should be lowercase"
        );

        let policy = &table.policies[0];
        assert_eq!(
            policy.using_expr,
            Some("role = 'admin'::text".to_string()),
            "Policy USING expression type casts should be lowercase"
        );
        assert_eq!(
            policy.check_expr,
            Some("role = 'user'::varchar".to_string()),
            "Policy CHECK expression type casts should be lowercase"
        );

        let check = &table.check_constraints[0];
        assert!(
            check.expression.contains("'admin'::text"),
            "Check constraint expression type casts should be lowercase: {}",
            check.expression
        );
    }

    #[test]
    fn parses_trigger_on_cross_schema_table_with_qualified_function() {
        // Bug: triggers on non-public schema tables are not parsed correctly
        // when the function is also in a non-public schema
        let sql = r#"
CREATE FUNCTION auth.on_auth_user_created() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
CREATE TRIGGER "on_auth_user_created" AFTER INSERT ON "auth"."users" FOR EACH ROW EXECUTE FUNCTION "auth"."on_auth_user_created"();
"#;
        let schema = parse_sql_string(sql).unwrap();

        assert_eq!(schema.triggers.len(), 1, "Should parse exactly one trigger");
        let trigger = schema
            .triggers
            .get("auth.users.on_auth_user_created")
            .expect("Trigger should be keyed as auth.users.on_auth_user_created");
        assert_eq!(trigger.name, "on_auth_user_created");
        assert_eq!(trigger.target_schema, "auth");
        assert_eq!(trigger.target_name, "users");
        assert_eq!(trigger.function_schema, "auth");
        assert_eq!(trigger.function_name, "on_auth_user_created");
    }

    #[test]
    fn parse_vector_types() {
        let sql = r#"
CREATE TABLE embeddings (
    id BIGINT NOT NULL PRIMARY KEY,
    embedding_default vector,
    embedding_1536 vector(1536),
    embedding_qualified public.vector(768)
);
"#;

        let schema = parse_sql_string(sql).expect("Should parse");

        let embeddings = &schema.tables["public.embeddings"];
        assert_eq!(embeddings.columns.len(), 4);

        let embedding_default = &embeddings.columns["embedding_default"];
        assert_eq!(embedding_default.data_type, PgType::Vector(None));

        let embedding_1536 = &embeddings.columns["embedding_1536"];
        assert_eq!(embedding_1536.data_type, PgType::Vector(Some(1536)));

        let embedding_qualified = &embeddings.columns["embedding_qualified"];
        assert_eq!(embedding_qualified.data_type, PgType::Vector(Some(768)));
    }
}
