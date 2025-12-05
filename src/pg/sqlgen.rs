use crate::diff::{ColumnChanges, EnumValuePosition, MigrationOp, PolicyChanges, SequenceChanges};
use crate::model::{
    parse_qualified_name, CheckConstraint, Column, ForeignKey, Function, Index, IndexType, PgType,
    Policy, PolicyCommand, ReferentialAction, SecurityType, Sequence, SequenceDataType, Table,
    Trigger, TriggerEnabled, TriggerEvent, TriggerTiming, View, Volatility,
};

pub fn generate_sql(ops: &[MigrationOp]) -> Vec<String> {
    ops.iter().flat_map(generate_op_sql).collect()
}

fn generate_op_sql(op: &MigrationOp) -> Vec<String> {
    match op {
        MigrationOp::CreateExtension(ext) => {
            let mut sql = format!("CREATE EXTENSION IF NOT EXISTS {}", quote_ident(&ext.name));
            if let Some(ref schema) = ext.schema {
                sql.push_str(&format!(" SCHEMA {}", quote_ident(schema)));
            }
            if let Some(ref version) = ext.version {
                sql.push_str(&format!(" VERSION '{}'", escape_string(version)));
            }
            sql.push(';');
            vec![sql]
        }

        MigrationOp::DropExtension(name) => {
            vec![format!("DROP EXTENSION IF EXISTS {};", quote_ident(name))]
        }

        MigrationOp::CreateEnum(enum_type) => vec![format!(
            "CREATE TYPE {} AS ENUM ({});",
            quote_qualified(&enum_type.schema, &enum_type.name),
            enum_type
                .values
                .iter()
                .map(|v| format!("'{}'", escape_string(v)))
                .collect::<Vec<_>>()
                .join(", ")
        )],

        MigrationOp::DropEnum(name) => {
            let (schema, enum_name) = parse_qualified_name(name);
            vec![format!(
                "DROP TYPE {};",
                quote_qualified(&schema, &enum_name)
            )]
        }

        MigrationOp::AddEnumValue {
            enum_name,
            value,
            position,
        } => {
            let (schema, name) = parse_qualified_name(enum_name);
            let mut sql = format!(
                "ALTER TYPE {} ADD VALUE '{}'",
                quote_qualified(&schema, &name),
                escape_string(value)
            );

            if let Some(pos) = position {
                match pos {
                    EnumValuePosition::Before(ref v) => {
                        sql.push_str(&format!(" BEFORE '{}'", escape_string(v)));
                    }
                    EnumValuePosition::After(ref v) => {
                        sql.push_str(&format!(" AFTER '{}'", escape_string(v)));
                    }
                }
            }

            sql.push(';');
            vec![sql]
        }

        MigrationOp::CreateTable(table) => generate_create_table(table),

        MigrationOp::DropTable(name) => {
            let (schema, table_name) = parse_qualified_name(name);
            vec![format!(
                "DROP TABLE {};",
                quote_qualified(&schema, &table_name)
            )]
        }

        MigrationOp::AddColumn { table, column } => {
            let (schema, table_name) = parse_qualified_name(table);
            vec![format!(
                "ALTER TABLE {} ADD COLUMN {};",
                quote_qualified(&schema, &table_name),
                format_column(column)
            )]
        }

        MigrationOp::DropColumn { table, column } => {
            let (schema, table_name) = parse_qualified_name(table);
            vec![format!(
                "ALTER TABLE {} DROP COLUMN {};",
                quote_qualified(&schema, &table_name),
                quote_ident(column)
            )]
        }

        MigrationOp::AlterColumn {
            table,
            column,
            changes,
        } => generate_alter_column(table, column, changes),

        MigrationOp::AddPrimaryKey { table, primary_key } => {
            let (schema, table_name) = parse_qualified_name(table);
            vec![format!(
                "ALTER TABLE {} ADD PRIMARY KEY ({});",
                quote_qualified(&schema, &table_name),
                format_column_list(&primary_key.columns)
            )]
        }

        MigrationOp::DropPrimaryKey { table } => {
            let (schema, table_name) = parse_qualified_name(table);
            vec![format!(
                "ALTER TABLE {} DROP CONSTRAINT {}_pkey;",
                quote_qualified(&schema, &table_name),
                quote_ident(&table_name)
            )]
        }

        MigrationOp::AddIndex { table, index } => {
            let (schema, table_name) = parse_qualified_name(table);
            vec![generate_create_index(&schema, &table_name, index)]
        }

        MigrationOp::DropIndex { index_name, .. } => {
            vec![format!("DROP INDEX {};", quote_ident(index_name))]
        }

        MigrationOp::AddForeignKey { table, foreign_key } => {
            let (schema, table_name) = parse_qualified_name(table);
            vec![generate_add_foreign_key(&schema, &table_name, foreign_key)]
        }

        MigrationOp::DropForeignKey {
            table,
            foreign_key_name,
        } => {
            let (schema, table_name) = parse_qualified_name(table);
            vec![format!(
                "ALTER TABLE {} DROP CONSTRAINT {};",
                quote_qualified(&schema, &table_name),
                quote_ident(foreign_key_name)
            )]
        }

        MigrationOp::AddCheckConstraint {
            table,
            check_constraint,
        } => {
            let (schema, table_name) = parse_qualified_name(table);
            vec![generate_add_check_constraint(
                &schema,
                &table_name,
                check_constraint,
            )]
        }

        MigrationOp::DropCheckConstraint {
            table,
            constraint_name,
        } => {
            let (schema, table_name) = parse_qualified_name(table);
            vec![format!(
                "ALTER TABLE {} DROP CONSTRAINT {};",
                quote_qualified(&schema, &table_name),
                quote_ident(constraint_name)
            )]
        }

        MigrationOp::EnableRls { table } => {
            let (schema, table_name) = parse_qualified_name(table);
            vec![format!(
                "ALTER TABLE {} ENABLE ROW LEVEL SECURITY;",
                quote_qualified(&schema, &table_name)
            )]
        }

        MigrationOp::DisableRls { table } => {
            let (schema, table_name) = parse_qualified_name(table);
            vec![format!(
                "ALTER TABLE {} DISABLE ROW LEVEL SECURITY;",
                quote_qualified(&schema, &table_name)
            )]
        }

        MigrationOp::CreatePolicy(policy) => vec![generate_create_policy(policy)],

        MigrationOp::DropPolicy { table, name } => {
            let (schema, table_name) = parse_qualified_name(table);
            vec![format!(
                "DROP POLICY {} ON {};",
                quote_ident(name),
                quote_qualified(&schema, &table_name)
            )]
        }

        MigrationOp::AlterPolicy {
            table,
            name,
            changes,
        } => generate_alter_policy(table, name, changes),

        MigrationOp::CreateFunction(func) => vec![generate_create_function(func)],

        MigrationOp::DropFunction { name, args } => {
            let (schema, func_name) = parse_qualified_name(name);
            vec![format!(
                "DROP FUNCTION {}({});",
                quote_qualified(&schema, &func_name),
                args
            )]
        }

        MigrationOp::AlterFunction { new_function, .. } => {
            vec![generate_create_or_replace_function(new_function)]
        }

        MigrationOp::CreateView(view) => vec![generate_create_view(view)],

        MigrationOp::DropView { name, materialized } => {
            let (schema, view_name) = parse_qualified_name(name);
            let view_type = if *materialized {
                "MATERIALIZED VIEW"
            } else {
                "VIEW"
            };
            vec![format!(
                "DROP {} {};",
                view_type,
                quote_qualified(&schema, &view_name)
            )]
        }

        MigrationOp::AlterView { new_view, .. } => {
            vec![generate_create_or_replace_view(new_view)]
        }

        MigrationOp::CreateTrigger(trigger) => {
            let mut statements = vec![generate_create_trigger(trigger)];
            if trigger.enabled != TriggerEnabled::Origin {
                statements.push(generate_alter_trigger_enabled(
                    &trigger.target_schema,
                    &trigger.target_name,
                    &trigger.name,
                    trigger.enabled,
                ));
            }
            statements
        }

        MigrationOp::DropTrigger {
            target_schema,
            target_name,
            name,
        } => {
            vec![format!(
                "DROP TRIGGER {} ON {};",
                quote_ident(name),
                quote_qualified(target_schema, target_name)
            )]
        }

        MigrationOp::AlterTriggerEnabled {
            target_schema,
            target_name,
            name,
            enabled,
        } => {
            vec![generate_alter_trigger_enabled(
                target_schema,
                target_name,
                name,
                *enabled,
            )]
        }

        MigrationOp::CreateSequence(seq) => vec![generate_create_sequence(seq)],

        MigrationOp::DropSequence(name) => {
            let (schema, seq_name) = parse_qualified_name(name);
            vec![format!(
                "DROP SEQUENCE {};",
                quote_qualified(&schema, &seq_name)
            )]
        }

        MigrationOp::AlterSequence { name, changes } => {
            vec![generate_alter_sequence(name, changes)]
        }
    }
}

fn generate_create_table(table: &Table) -> Vec<String> {
    let mut statements = Vec::new();

    let mut column_defs: Vec<String> = table.columns.values().map(format_column).collect();

    if let Some(ref primary_key) = table.primary_key {
        column_defs.push(format!(
            "PRIMARY KEY ({})",
            format_column_list(&primary_key.columns)
        ));
    }

    let qualified_name = quote_qualified(&table.schema, &table.name);
    statements.push(format!(
        "CREATE TABLE {} (\n    {}\n);",
        qualified_name,
        column_defs.join(",\n    ")
    ));

    for index in &table.indexes {
        statements.push(generate_create_index(&table.schema, &table.name, index));
    }

    for foreign_key in &table.foreign_keys {
        statements.push(generate_add_foreign_key(
            &table.schema,
            &table.name,
            foreign_key,
        ));
    }

    for check_constraint in &table.check_constraints {
        statements.push(generate_add_check_constraint(
            &table.schema,
            &table.name,
            check_constraint,
        ));
    }

    statements
}

fn generate_create_index(schema: &str, table: &str, index: &Index) -> String {
    let unique = if index.unique { "UNIQUE " } else { "" };
    let index_type = match index.index_type {
        IndexType::BTree => "",
        IndexType::Hash => " USING hash",
        IndexType::Gin => " USING gin",
        IndexType::Gist => " USING gist",
    };

    format!(
        "CREATE {}INDEX {}{} ON {} ({});",
        unique,
        quote_ident(&index.name),
        index_type,
        quote_qualified(schema, table),
        format_column_list(&index.columns)
    )
}

fn generate_add_foreign_key(schema: &str, table: &str, foreign_key: &ForeignKey) -> String {
    format!(
        "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({}) ON DELETE {} ON UPDATE {};",
        quote_qualified(schema, table),
        quote_ident(&foreign_key.name),
        format_column_list(&foreign_key.columns),
        quote_qualified(&foreign_key.referenced_schema, &foreign_key.referenced_table),
        format_column_list(&foreign_key.referenced_columns),
        format_referential_action(&foreign_key.on_delete),
        format_referential_action(&foreign_key.on_update)
    )
}

fn generate_add_check_constraint(
    schema: &str,
    table: &str,
    check_constraint: &CheckConstraint,
) -> String {
    format!(
        "ALTER TABLE {} ADD CONSTRAINT {} CHECK ({});",
        quote_qualified(schema, table),
        quote_ident(&check_constraint.name),
        check_constraint.expression
    )
}

fn generate_alter_column(table: &str, column: &str, changes: &ColumnChanges) -> Vec<String> {
    let (schema, table_name) = parse_qualified_name(table);
    let qualified = quote_qualified(&schema, &table_name);
    let mut statements = Vec::new();

    if let Some(ref data_type) = changes.data_type {
        statements.push(format!(
            "ALTER TABLE {} ALTER COLUMN {} TYPE {};",
            qualified,
            quote_ident(column),
            format_pg_type(data_type)
        ));
    }

    if let Some(nullable) = changes.nullable {
        if nullable {
            statements.push(format!(
                "ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL;",
                qualified,
                quote_ident(column)
            ));
        } else {
            statements.push(format!(
                "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL;",
                qualified,
                quote_ident(column)
            ));
        }
    }

    if let Some(ref default) = changes.default {
        match default {
            Some(value) => {
                statements.push(format!(
                    "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {};",
                    qualified,
                    quote_ident(column),
                    value
                ));
            }
            None => {
                statements.push(format!(
                    "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT;",
                    qualified,
                    quote_ident(column)
                ));
            }
        }
    }

    statements
}

fn format_column(column: &Column) -> String {
    let mut parts = vec![quote_ident(&column.name), format_pg_type(&column.data_type)];

    if !column.nullable {
        parts.push("NOT NULL".to_string());
    }

    if let Some(ref default) = column.default {
        parts.push(format!("DEFAULT {default}"));
    }

    parts.join(" ")
}

fn format_pg_type(pg_type: &PgType) -> String {
    match pg_type {
        PgType::Integer => "INTEGER".to_string(),
        PgType::BigInt => "BIGINT".to_string(),
        PgType::SmallInt => "SMALLINT".to_string(),
        PgType::Varchar(Some(len)) => format!("VARCHAR({len})"),
        PgType::Varchar(None) => "VARCHAR".to_string(),
        PgType::Text => "TEXT".to_string(),
        PgType::Boolean => "BOOLEAN".to_string(),
        PgType::TimestampTz => "TIMESTAMP WITH TIME ZONE".to_string(),
        PgType::Timestamp => "TIMESTAMP".to_string(),
        PgType::Date => "DATE".to_string(),
        PgType::Uuid => "UUID".to_string(),
        PgType::Json => "JSON".to_string(),
        PgType::Jsonb => "JSONB".to_string(),
        PgType::CustomEnum(name) => {
            let (schema, enum_name) = parse_qualified_name(name);
            quote_qualified(&schema, &enum_name)
        }
    }
}

fn format_referential_action(action: &ReferentialAction) -> &'static str {
    match action {
        ReferentialAction::NoAction => "NO ACTION",
        ReferentialAction::Restrict => "RESTRICT",
        ReferentialAction::Cascade => "CASCADE",
        ReferentialAction::SetNull => "SET NULL",
        ReferentialAction::SetDefault => "SET DEFAULT",
    }
}

fn format_column_list(columns: &[String]) -> String {
    columns
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn quote_qualified(schema: &str, name: &str) -> String {
    format!("{}.{}", quote_ident(schema), quote_ident(name))
}

fn escape_string(value: &str) -> String {
    value.replace('\'', "''")
}

fn generate_create_policy(policy: &Policy) -> String {
    let mut sql = format!(
        "CREATE POLICY {} ON {}",
        quote_ident(&policy.name),
        quote_qualified(&policy.table_schema, &policy.table)
    );

    sql.push_str(&format!(" FOR {}", format_policy_command(&policy.command)));

    if !policy.roles.is_empty() {
        sql.push_str(&format!(
            " TO {}",
            policy
                .roles
                .iter()
                .map(|r| quote_ident(r))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    if let Some(ref using_expr) = policy.using_expr {
        sql.push_str(&format!(" USING ({using_expr})"));
    }

    if let Some(ref check_expr) = policy.check_expr {
        sql.push_str(&format!(" WITH CHECK ({check_expr})"));
    }

    sql.push(';');
    sql
}

fn generate_alter_policy(table: &str, name: &str, changes: &PolicyChanges) -> Vec<String> {
    let (schema, table_name) = parse_qualified_name(table);
    let qualified = quote_qualified(&schema, &table_name);
    let mut statements = Vec::new();

    if let Some(ref roles) = changes.roles {
        statements.push(format!(
            "ALTER POLICY {} ON {} TO {};",
            quote_ident(name),
            qualified,
            roles
                .iter()
                .map(|r| quote_ident(r))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    if let Some(Some(expr)) = &changes.using_expr {
        statements.push(format!(
            "ALTER POLICY {} ON {} USING ({});",
            quote_ident(name),
            qualified,
            expr
        ))
    }

    if let Some(Some(expr)) = &changes.check_expr {
        statements.push(format!(
            "ALTER POLICY {} ON {} WITH CHECK ({});",
            quote_ident(name),
            qualified,
            expr
        ))
    }

    statements
}

fn format_policy_command(command: &PolicyCommand) -> &'static str {
    match command {
        PolicyCommand::All => "ALL",
        PolicyCommand::Select => "SELECT",
        PolicyCommand::Insert => "INSERT",
        PolicyCommand::Update => "UPDATE",
        PolicyCommand::Delete => "DELETE",
    }
}

fn generate_create_function(func: &Function) -> String {
    generate_function_ddl(func, false)
}

fn generate_create_or_replace_function(func: &Function) -> String {
    generate_function_ddl(func, true)
}

fn generate_function_ddl(func: &Function, replace: bool) -> String {
    let create_stmt = if replace {
        "CREATE OR REPLACE FUNCTION"
    } else {
        "CREATE FUNCTION"
    };

    let args = func
        .arguments
        .iter()
        .map(|arg| {
            let mut parts = Vec::new();
            if let Some(ref name) = arg.name {
                parts.push(quote_ident(name));
            }
            parts.push(arg.data_type.clone());
            if let Some(ref default) = arg.default {
                parts.push(format!("DEFAULT {default}"));
            }
            parts.join(" ")
        })
        .collect::<Vec<_>>()
        .join(", ");

    let volatility = match func.volatility {
        Volatility::Immutable => "IMMUTABLE",
        Volatility::Stable => "STABLE",
        Volatility::Volatile => "VOLATILE",
    };

    let security = match func.security {
        SecurityType::Definer => "SECURITY DEFINER",
        SecurityType::Invoker => "SECURITY INVOKER",
    };

    format!(
        "{} {}({}) RETURNS {} LANGUAGE {} {} {} AS $${}$$;",
        create_stmt,
        quote_qualified(&func.schema, &func.name),
        args,
        func.return_type,
        func.language,
        volatility,
        security,
        func.body
    )
}

fn generate_create_view(view: &View) -> String {
    generate_view_ddl(view, false)
}

fn generate_create_or_replace_view(view: &View) -> String {
    generate_view_ddl(view, true)
}

fn generate_view_ddl(view: &View, replace: bool) -> String {
    let qualified_name = quote_qualified(&view.schema, &view.name);
    if view.materialized {
        if replace {
            format!(
                "DROP MATERIALIZED VIEW IF EXISTS {}; CREATE MATERIALIZED VIEW {} AS {};",
                qualified_name, qualified_name, view.query
            )
        } else {
            format!(
                "CREATE MATERIALIZED VIEW {} AS {};",
                qualified_name, view.query
            )
        }
    } else {
        let create_stmt = if replace {
            "CREATE OR REPLACE VIEW"
        } else {
            "CREATE VIEW"
        };
        format!("{} {} AS {};", create_stmt, qualified_name, view.query)
    }
}

fn generate_create_sequence(seq: &Sequence) -> String {
    let mut parts = vec![
        "CREATE SEQUENCE".to_string(),
        quote_qualified(&seq.schema, &seq.name),
    ];

    let data_type_str = match seq.data_type {
        SequenceDataType::SmallInt => "smallint",
        SequenceDataType::Integer => "integer",
        SequenceDataType::BigInt => "bigint",
    };
    parts.push(format!("AS {data_type_str}"));

    if let Some(start) = seq.start {
        parts.push(format!("START WITH {start}"));
    }

    if let Some(increment) = seq.increment {
        parts.push(format!("INCREMENT BY {increment}"));
    }

    if let Some(min_value) = seq.min_value {
        parts.push(format!("MINVALUE {min_value}"));
    }

    if let Some(max_value) = seq.max_value {
        parts.push(format!("MAXVALUE {max_value}"));
    }

    if seq.cycle {
        parts.push("CYCLE".to_string());
    }

    if let Some(cache) = seq.cache {
        parts.push(format!("CACHE {cache}"));
    }

    if let Some(ref owner) = seq.owned_by {
        parts.push(format!(
            "OWNED BY {}.{}.{}",
            quote_ident(&owner.table_schema),
            quote_ident(&owner.table_name),
            quote_ident(&owner.column_name)
        ));
    }

    format!("{};", parts.join(" "))
}

fn generate_alter_sequence(name: &str, changes: &SequenceChanges) -> String {
    let (schema, seq_name) = parse_qualified_name(name);
    let mut parts = vec![
        "ALTER SEQUENCE".to_string(),
        quote_qualified(&schema, &seq_name),
    ];

    if let Some(ref data_type) = changes.data_type {
        let data_type_str = match data_type {
            SequenceDataType::SmallInt => "smallint",
            SequenceDataType::Integer => "integer",
            SequenceDataType::BigInt => "bigint",
        };
        parts.push(format!("AS {data_type_str}"));
    }

    if let Some(increment) = changes.increment {
        parts.push(format!("INCREMENT BY {increment}"));
    }

    if let Some(ref min_value) = changes.min_value {
        match min_value {
            Some(val) => parts.push(format!("MINVALUE {val}")),
            None => parts.push("NO MINVALUE".to_string()),
        }
    }

    if let Some(ref max_value) = changes.max_value {
        match max_value {
            Some(val) => parts.push(format!("MAXVALUE {val}")),
            None => parts.push("NO MAXVALUE".to_string()),
        }
    }

    if let Some(restart) = changes.restart {
        parts.push(format!("RESTART WITH {restart}"));
    }

    if let Some(cache) = changes.cache {
        parts.push(format!("CACHE {cache}"));
    }

    if let Some(cycle) = changes.cycle {
        if cycle {
            parts.push("CYCLE".to_string());
        } else {
            parts.push("NO CYCLE".to_string());
        }
    }

    if let Some(ref owned_by) = changes.owned_by {
        match owned_by {
            Some(owner) => {
                parts.push(format!(
                    "OWNED BY {}.{}.{}",
                    quote_ident(&owner.table_schema),
                    quote_ident(&owner.table_name),
                    quote_ident(&owner.column_name)
                ));
            }
            None => parts.push("OWNED BY NONE".to_string()),
        }
    }

    format!("{};", parts.join(" "))
}

fn generate_create_trigger(trigger: &Trigger) -> String {
    let mut sql = format!("CREATE TRIGGER {}", quote_ident(&trigger.name));

    let timing = match trigger.timing {
        TriggerTiming::Before => "BEFORE",
        TriggerTiming::After => "AFTER",
        TriggerTiming::InsteadOf => "INSTEAD OF",
    };

    let events: Vec<String> = trigger
        .events
        .iter()
        .map(|e| match e {
            TriggerEvent::Insert => "INSERT".to_string(),
            TriggerEvent::Update => {
                if trigger.update_columns.is_empty() {
                    "UPDATE".to_string()
                } else {
                    format!(
                        "UPDATE OF {}",
                        trigger
                            .update_columns
                            .iter()
                            .map(|c| quote_ident(c))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                }
            }
            TriggerEvent::Delete => "DELETE".to_string(),
            TriggerEvent::Truncate => "TRUNCATE".to_string(),
        })
        .collect();

    sql.push_str(&format!(" {} {}", timing, events.join(" OR ")));
    sql.push_str(&format!(
        " ON {}",
        quote_qualified(&trigger.target_schema, &trigger.target_name)
    ));

    if trigger.for_each_row {
        sql.push_str(" FOR EACH ROW");
    } else {
        sql.push_str(" FOR EACH STATEMENT");
    }

    // Generate REFERENCING clause for transition tables
    let mut referencing_parts = Vec::new();
    if let Some(ref name) = trigger.old_table_name {
        referencing_parts.push(format!("OLD TABLE AS {}", quote_ident(name)));
    }
    if let Some(ref name) = trigger.new_table_name {
        referencing_parts.push(format!("NEW TABLE AS {}", quote_ident(name)));
    }
    if !referencing_parts.is_empty() {
        sql.push_str(&format!(" REFERENCING {}", referencing_parts.join(" ")));
    }

    if let Some(ref when_clause) = trigger.when_clause {
        sql.push_str(&format!(" WHEN ({when_clause})"));
    }

    sql.push_str(&format!(
        " EXECUTE FUNCTION {}",
        quote_qualified(&trigger.function_schema, &trigger.function_name)
    ));

    if trigger.function_args.is_empty() {
        sql.push_str("();");
    } else {
        sql.push_str(&format!("({});", trigger.function_args.join(", ")));
    }

    sql
}

fn generate_alter_trigger_enabled(
    target_schema: &str,
    target_name: &str,
    trigger_name: &str,
    enabled: TriggerEnabled,
) -> String {
    let action = match enabled {
        TriggerEnabled::Origin => "ENABLE TRIGGER",
        TriggerEnabled::Disabled => "DISABLE TRIGGER",
        TriggerEnabled::Replica => "ENABLE REPLICA TRIGGER",
        TriggerEnabled::Always => "ENABLE ALWAYS TRIGGER",
    };
    format!(
        "ALTER TABLE {} {} {};",
        quote_qualified(target_schema, target_name),
        action,
        quote_ident(trigger_name)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{EnumType, PrimaryKey};
    use std::collections::BTreeMap;

    #[test]
    fn create_enum_generates_valid_sql() {
        let ops = vec![MigrationOp::CreateEnum(EnumType {
            name: "user_role".to_string(),
            schema: "public".to_string(),
            values: vec!["admin".to_string(), "user".to_string(), "guest".to_string()],
        })];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "CREATE TYPE \"public\".\"user_role\" AS ENUM ('admin', 'user', 'guest');"
        );
    }

    #[test]
    fn drop_enum_generates_valid_sql() {
        let ops = vec![MigrationOp::DropEnum("public.user_role".to_string())];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(sql[0], "DROP TYPE \"public\".\"user_role\";");
    }

    #[test]
    fn add_column_generates_valid_sql() {
        let ops = vec![MigrationOp::AddColumn {
            table: "public.users".to_string(),
            column: Column {
                name: "email".to_string(),
                data_type: PgType::Varchar(Some(255)),
                nullable: false,
                default: None,
                comment: None,
            },
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "ALTER TABLE \"public\".\"users\" ADD COLUMN \"email\" VARCHAR(255) NOT NULL;"
        );
    }

    #[test]
    fn drop_column_generates_valid_sql() {
        let ops = vec![MigrationOp::DropColumn {
            table: "public.users".to_string(),
            column: "email".to_string(),
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "ALTER TABLE \"public\".\"users\" DROP COLUMN \"email\";"
        );
    }

    #[test]
    fn create_table_generates_valid_sql() {
        let mut columns = BTreeMap::new();
        columns.insert(
            "id".to_string(),
            Column {
                name: "id".to_string(),
                data_type: PgType::BigInt,
                nullable: false,
                default: None,
                comment: None,
            },
        );
        columns.insert(
            "name".to_string(),
            Column {
                name: "name".to_string(),
                data_type: PgType::Text,
                nullable: true,
                default: None,
                comment: None,
            },
        );

        let ops = vec![MigrationOp::CreateTable(Table {
            name: "users".to_string(),
            schema: "public".to_string(),
            columns,
            indexes: vec![],
            primary_key: Some(PrimaryKey {
                columns: vec!["id".to_string()],
            }),
            foreign_keys: vec![],
            check_constraints: vec![],
            comment: None,
            row_level_security: false,
            policies: vec![],
        })];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert!(sql[0].contains("CREATE TABLE \"public\".\"users\""));
        assert!(sql[0].contains("\"id\" BIGINT NOT NULL"));
        assert!(sql[0].contains("\"name\" TEXT"));
        assert!(sql[0].contains("PRIMARY KEY (\"id\")"));
    }

    #[test]
    fn quote_ident_escapes_quotes() {
        assert_eq!(quote_ident("simple"), "\"simple\"");
        assert_eq!(quote_ident("has\"quote"), "\"has\"\"quote\"");
    }

    #[test]
    fn add_index_generates_valid_sql() {
        let ops = vec![MigrationOp::AddIndex {
            table: "public.users".to_string(),
            index: Index {
                name: "users_email_idx".to_string(),
                columns: vec!["email".to_string()],
                unique: true,
                index_type: IndexType::BTree,
            },
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "CREATE UNIQUE INDEX \"users_email_idx\" ON \"public\".\"users\" (\"email\");"
        );
    }

    #[test]
    fn alter_column_type_generates_valid_sql() {
        let ops = vec![MigrationOp::AlterColumn {
            table: "public.users".to_string(),
            column: "name".to_string(),
            changes: ColumnChanges {
                data_type: Some(PgType::Varchar(Some(100))),
                nullable: None,
                default: None,
            },
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "ALTER TABLE \"public\".\"users\" ALTER COLUMN \"name\" TYPE VARCHAR(100);"
        );
    }

    #[test]
    fn create_view_generates_valid_sql() {
        let ops = vec![MigrationOp::CreateView(View {
            name: "active_users".to_string(),
            schema: "public".to_string(),
            query: "SELECT * FROM users WHERE active = true".to_string(),
            materialized: false,
        })];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "CREATE VIEW \"public\".\"active_users\" AS SELECT * FROM users WHERE active = true;"
        );
    }

    #[test]
    fn create_materialized_view_generates_valid_sql() {
        let ops = vec![MigrationOp::CreateView(View {
            name: "user_stats".to_string(),
            schema: "public".to_string(),
            query: "SELECT COUNT(*) FROM users".to_string(),
            materialized: true,
        })];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "CREATE MATERIALIZED VIEW \"public\".\"user_stats\" AS SELECT COUNT(*) FROM users;"
        );
    }

    #[test]
    fn drop_view_generates_valid_sql() {
        let ops = vec![MigrationOp::DropView {
            name: "public.active_users".to_string(),
            materialized: false,
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(sql[0], "DROP VIEW \"public\".\"active_users\";");
    }

    #[test]
    fn drop_materialized_view_generates_valid_sql() {
        let ops = vec![MigrationOp::DropView {
            name: "public.user_stats".to_string(),
            materialized: true,
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(sql[0], "DROP MATERIALIZED VIEW \"public\".\"user_stats\";");
    }

    #[test]
    fn add_foreign_key_generates_valid_sql() {
        let ops = vec![MigrationOp::AddForeignKey {
            table: "public.posts".to_string(),
            foreign_key: ForeignKey {
                name: "posts_user_id_fkey".to_string(),
                columns: vec!["user_id".to_string()],
                referenced_table: "users".to_string(),
                referenced_schema: "public".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: ReferentialAction::Cascade,
                on_update: ReferentialAction::NoAction,
            },
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert!(sql[0]
            .contains("ALTER TABLE \"public\".\"posts\" ADD CONSTRAINT \"posts_user_id_fkey\""));
        assert!(sql[0].contains("FOREIGN KEY (\"user_id\")"));
        assert!(sql[0].contains("REFERENCES \"public\".\"users\" (\"id\")"));
        assert!(sql[0].contains("ON DELETE CASCADE"));
        assert!(sql[0].contains("ON UPDATE NO ACTION"));
    }

    #[test]
    fn add_check_constraint_generates_valid_sql() {
        let ops = vec![MigrationOp::AddCheckConstraint {
            table: "public.products".to_string(),
            check_constraint: CheckConstraint {
                name: "price_positive".to_string(),
                expression: "price > 0".to_string(),
            },
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "ALTER TABLE \"public\".\"products\" ADD CONSTRAINT \"price_positive\" CHECK (price > 0);"
        );
    }

    #[test]
    fn drop_check_constraint_generates_valid_sql() {
        let ops = vec![MigrationOp::DropCheckConstraint {
            table: "public.products".to_string(),
            constraint_name: "price_positive".to_string(),
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "ALTER TABLE \"public\".\"products\" DROP CONSTRAINT \"price_positive\";"
        );
    }

    #[test]
    fn add_enum_value_generates_valid_sql() {
        let ops = vec![MigrationOp::AddEnumValue {
            enum_name: "public.status".to_string(),
            value: "pending".to_string(),
            position: None,
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "ALTER TYPE \"public\".\"status\" ADD VALUE 'pending';"
        );
    }

    #[test]
    fn add_enum_value_with_after_position() {
        let ops = vec![MigrationOp::AddEnumValue {
            enum_name: "public.status".to_string(),
            value: "pending".to_string(),
            position: Some(EnumValuePosition::After("active".to_string())),
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "ALTER TYPE \"public\".\"status\" ADD VALUE 'pending' AFTER 'active';"
        );
    }

    #[test]
    fn add_enum_value_with_before_position() {
        let ops = vec![MigrationOp::AddEnumValue {
            enum_name: "public.status".to_string(),
            value: "pending".to_string(),
            position: Some(EnumValuePosition::Before("active".to_string())),
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "ALTER TYPE \"public\".\"status\" ADD VALUE 'pending' BEFORE 'active';"
        );
    }

    #[test]
    fn add_enum_value_escapes_quotes() {
        let ops = vec![MigrationOp::AddEnumValue {
            enum_name: "public.status".to_string(),
            value: "it's pending".to_string(),
            position: None,
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "ALTER TYPE \"public\".\"status\" ADD VALUE 'it''s pending';"
        );
    }

    #[test]
    fn create_extension_generates_valid_sql() {
        let ops = vec![MigrationOp::CreateExtension(crate::model::Extension {
            name: "uuid-ossp".to_string(),
            version: None,
            schema: None,
        })];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(sql[0], "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";");
    }

    #[test]
    fn create_extension_with_version_and_schema() {
        let ops = vec![MigrationOp::CreateExtension(crate::model::Extension {
            name: "pgcrypto".to_string(),
            version: Some("1.3".to_string()),
            schema: Some("crypto".to_string()),
        })];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "CREATE EXTENSION IF NOT EXISTS \"pgcrypto\" SCHEMA \"crypto\" VERSION '1.3';"
        );
    }

    #[test]
    fn drop_extension_generates_valid_sql() {
        let ops = vec![MigrationOp::DropExtension("uuid-ossp".to_string())];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(sql[0], "DROP EXTENSION IF EXISTS \"uuid-ossp\";");
    }

    #[test]
    fn generates_qualified_create_table() {
        let mut columns = BTreeMap::new();
        columns.insert(
            "id".to_string(),
            Column {
                name: "id".to_string(),
                data_type: PgType::BigInt,
                nullable: false,
                default: None,
                comment: None,
            },
        );

        let table = Table {
            schema: "auth".to_string(),
            name: "users".to_string(),
            columns,
            indexes: vec![],
            primary_key: Some(PrimaryKey {
                columns: vec!["id".to_string()],
            }),
            foreign_keys: vec![],
            check_constraints: vec![],
            comment: None,
            row_level_security: false,
            policies: vec![],
        };

        let op = MigrationOp::CreateTable(table);
        let sql = generate_sql(&vec![op]);
        assert_eq!(sql.len(), 1);
        assert!(sql[0].contains(r#"CREATE TABLE "auth"."users""#));
    }

    #[test]
    fn create_simple_trigger() {
        use crate::model::{Trigger, TriggerEnabled, TriggerEvent, TriggerTiming};

        let trigger = Trigger {
            name: "audit_trigger".to_string(),
            target_schema: "public".to_string(),
            target_name: "users".to_string(),
            timing: TriggerTiming::After,
            events: vec![TriggerEvent::Insert],
            update_columns: vec![],
            for_each_row: true,
            when_clause: None,
            function_schema: "public".to_string(),
            function_name: "audit_fn".to_string(),
            function_args: vec![],
            enabled: TriggerEnabled::Origin,
            old_table_name: None,
            new_table_name: None,
        };

        let ops = vec![MigrationOp::CreateTrigger(trigger)];
        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert!(sql[0].contains("CREATE TRIGGER"));
        assert!(sql[0].contains("\"audit_trigger\""));
        assert!(sql[0].contains("AFTER INSERT"));
        assert!(sql[0].contains("ON \"public\".\"users\""));
        assert!(sql[0].contains("FOR EACH ROW"));
        assert!(sql[0].contains("EXECUTE FUNCTION"));
        assert!(sql[0].contains("\"audit_fn\""));
    }

    #[test]
    fn create_trigger_with_update_of_columns() {
        use crate::model::{Trigger, TriggerEnabled, TriggerEvent, TriggerTiming};

        let trigger = Trigger {
            name: "notify_change".to_string(),
            target_schema: "public".to_string(),
            target_name: "users".to_string(),
            timing: TriggerTiming::Before,
            events: vec![TriggerEvent::Update],
            update_columns: vec!["email".to_string(), "name".to_string()],
            for_each_row: true,
            when_clause: None,
            function_schema: "public".to_string(),
            function_name: "notify_fn".to_string(),
            function_args: vec![],
            enabled: TriggerEnabled::Origin,
            old_table_name: None,
            new_table_name: None,
        };

        let ops = vec![MigrationOp::CreateTrigger(trigger)];
        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert!(sql[0].contains("BEFORE UPDATE OF"));
        assert!(sql[0].contains("\"email\""));
        assert!(sql[0].contains("\"name\""));
    }

    #[test]
    fn create_trigger_with_multiple_events() {
        use crate::model::{Trigger, TriggerEnabled, TriggerEvent, TriggerTiming};

        let trigger = Trigger {
            name: "log_changes".to_string(),
            target_schema: "public".to_string(),
            target_name: "orders".to_string(),
            timing: TriggerTiming::After,
            events: vec![
                TriggerEvent::Insert,
                TriggerEvent::Update,
                TriggerEvent::Delete,
            ],
            update_columns: vec![],
            for_each_row: true,
            when_clause: None,
            function_schema: "public".to_string(),
            function_name: "log_fn".to_string(),
            function_args: vec![],
            enabled: TriggerEnabled::Origin,
            old_table_name: None,
            new_table_name: None,
        };

        let ops = vec![MigrationOp::CreateTrigger(trigger)];
        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert!(sql[0].contains("INSERT OR UPDATE OR DELETE"));
    }

    #[test]
    fn create_trigger_with_when_clause() {
        use crate::model::{Trigger, TriggerEnabled, TriggerEvent, TriggerTiming};

        let trigger = Trigger {
            name: "check_amount".to_string(),
            target_schema: "public".to_string(),
            target_name: "orders".to_string(),
            timing: TriggerTiming::Before,
            events: vec![TriggerEvent::Insert],
            update_columns: vec![],
            for_each_row: true,
            when_clause: Some("NEW.amount > 1000".to_string()),
            function_schema: "public".to_string(),
            function_name: "check_fn".to_string(),
            function_args: vec![],
            enabled: TriggerEnabled::Origin,
            old_table_name: None,
            new_table_name: None,
        };

        let ops = vec![MigrationOp::CreateTrigger(trigger)];
        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert!(sql[0].contains("WHEN (NEW.amount > 1000)"));
    }

    #[test]
    fn drop_trigger() {
        let ops = vec![MigrationOp::DropTrigger {
            target_schema: "public".to_string(),
            target_name: "users".to_string(),
            name: "audit_trigger".to_string(),
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            r#"DROP TRIGGER "audit_trigger" ON "public"."users";"#
        );
    }

    #[test]
    fn alter_trigger_disable() {
        use crate::model::TriggerEnabled;

        let ops = vec![MigrationOp::AlterTriggerEnabled {
            target_schema: "public".to_string(),
            target_name: "users".to_string(),
            name: "audit_trigger".to_string(),
            enabled: TriggerEnabled::Disabled,
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            r#"ALTER TABLE "public"."users" DISABLE TRIGGER "audit_trigger";"#
        );
    }

    #[test]
    fn alter_trigger_enable_origin() {
        use crate::model::TriggerEnabled;

        let ops = vec![MigrationOp::AlterTriggerEnabled {
            target_schema: "public".to_string(),
            target_name: "users".to_string(),
            name: "audit_trigger".to_string(),
            enabled: TriggerEnabled::Origin,
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            r#"ALTER TABLE "public"."users" ENABLE TRIGGER "audit_trigger";"#
        );
    }

    #[test]
    fn alter_trigger_enable_replica() {
        use crate::model::TriggerEnabled;

        let ops = vec![MigrationOp::AlterTriggerEnabled {
            target_schema: "public".to_string(),
            target_name: "users".to_string(),
            name: "audit_trigger".to_string(),
            enabled: TriggerEnabled::Replica,
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            r#"ALTER TABLE "public"."users" ENABLE REPLICA TRIGGER "audit_trigger";"#
        );
    }

    #[test]
    fn alter_trigger_enable_always() {
        use crate::model::TriggerEnabled;

        let ops = vec![MigrationOp::AlterTriggerEnabled {
            target_schema: "public".to_string(),
            target_name: "users".to_string(),
            name: "audit_trigger".to_string(),
            enabled: TriggerEnabled::Always,
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            r#"ALTER TABLE "public"."users" ENABLE ALWAYS TRIGGER "audit_trigger";"#
        );
    }

    #[test]
    fn create_disabled_trigger_emits_alter() {
        use crate::model::{Trigger, TriggerEnabled, TriggerEvent, TriggerTiming};

        let trigger = Trigger {
            name: "audit_trigger".to_string(),
            target_schema: "public".to_string(),
            target_name: "users".to_string(),
            timing: TriggerTiming::After,
            events: vec![TriggerEvent::Insert],
            update_columns: vec![],
            for_each_row: true,
            when_clause: None,
            function_schema: "public".to_string(),
            function_name: "audit_fn".to_string(),
            function_args: vec![],
            enabled: TriggerEnabled::Disabled,
            old_table_name: None,
            new_table_name: None,
        };

        let ops = vec![MigrationOp::CreateTrigger(trigger)];
        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 2);
        assert!(sql[0].contains("CREATE TRIGGER"));
        assert_eq!(
            sql[1],
            r#"ALTER TABLE "public"."users" DISABLE TRIGGER "audit_trigger";"#
        );
    }

    #[test]
    fn sqlgen_create_sequence_minimal() {
        use crate::model::{Sequence, SequenceDataType};

        let seq = Sequence {
            name: "users_id_seq".to_string(),
            schema: "public".to_string(),
            data_type: SequenceDataType::BigInt,
            start: None,
            increment: None,
            min_value: None,
            max_value: None,
            cycle: false,
            cache: None,
            owned_by: None,
        };
        let op = MigrationOp::CreateSequence(seq);
        let sql = generate_sql(&vec![op]);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "CREATE SEQUENCE \"public\".\"users_id_seq\" AS bigint;"
        );
    }

    #[test]
    fn sqlgen_create_sequence_full() {
        use crate::model::{Sequence, SequenceDataType, SequenceOwner};

        let seq = Sequence {
            name: "counter_seq".to_string(),
            schema: "auth".to_string(),
            data_type: SequenceDataType::Integer,
            start: Some(100),
            increment: Some(5),
            min_value: Some(1),
            max_value: Some(1000),
            cycle: true,
            cache: Some(10),
            owned_by: Some(SequenceOwner {
                table_schema: "auth".to_string(),
                table_name: "users".to_string(),
                column_name: "id".to_string(),
            }),
        };
        let op = MigrationOp::CreateSequence(seq);
        let sql = generate_sql(&vec![op]);
        assert_eq!(sql.len(), 1);
        assert!(sql[0].contains("CREATE SEQUENCE \"auth\".\"counter_seq\""));
        assert!(sql[0].contains("AS integer"));
        assert!(sql[0].contains("START WITH 100"));
        assert!(sql[0].contains("INCREMENT BY 5"));
        assert!(sql[0].contains("MINVALUE 1"));
        assert!(sql[0].contains("MAXVALUE 1000"));
        assert!(sql[0].contains("CYCLE"));
        assert!(sql[0].contains("CACHE 10"));
        assert!(sql[0].contains("OWNED BY \"auth\".\"users\".\"id\""));
    }

    #[test]
    fn sqlgen_drop_sequence() {
        let op = MigrationOp::DropSequence("public.users_id_seq".to_string());
        let sql = generate_sql(&vec![op]);
        assert_eq!(sql.len(), 1);
        assert_eq!(sql[0], "DROP SEQUENCE \"public\".\"users_id_seq\";");
    }

    #[test]
    fn sqlgen_alter_sequence_increment() {
        use crate::diff::SequenceChanges;

        let mut changes = SequenceChanges::default();
        changes.increment = Some(10);
        let op = MigrationOp::AlterSequence {
            name: "public.counter_seq".to_string(),
            changes,
        };
        let sql = generate_sql(&vec![op]);
        assert_eq!(sql.len(), 1);
        assert!(sql[0].contains("ALTER SEQUENCE \"public\".\"counter_seq\""));
        assert!(sql[0].contains("INCREMENT BY 10"));
    }

    #[test]
    fn sqlgen_alter_sequence_multiple_changes() {
        use crate::diff::SequenceChanges;
        use crate::model::SequenceDataType;

        let changes = SequenceChanges {
            data_type: Some(SequenceDataType::BigInt),
            increment: Some(2),
            min_value: Some(Some(10)),
            max_value: Some(None),
            restart: Some(50),
            cache: Some(20),
            cycle: Some(true),
            owned_by: None,
        };
        let op = MigrationOp::AlterSequence {
            name: "public.my_seq".to_string(),
            changes,
        };
        let sql = generate_sql(&vec![op]);
        assert_eq!(sql.len(), 1);
        assert!(sql[0].contains("ALTER SEQUENCE \"public\".\"my_seq\""));
        assert!(sql[0].contains("AS bigint"));
        assert!(sql[0].contains("INCREMENT BY 2"));
        assert!(sql[0].contains("MINVALUE 10"));
        assert!(sql[0].contains("NO MAXVALUE"));
        assert!(sql[0].contains("RESTART WITH 50"));
        assert!(sql[0].contains("CACHE 20"));
        assert!(sql[0].contains("CYCLE"));
    }

    #[test]
    fn sqlgen_alter_sequence_no_minvalue() {
        use crate::diff::SequenceChanges;

        let mut changes = SequenceChanges::default();
        changes.min_value = Some(None);
        let op = MigrationOp::AlterSequence {
            name: "public.seq".to_string(),
            changes,
        };
        let sql = generate_sql(&vec![op]);
        assert_eq!(sql.len(), 1);
        assert!(sql[0].contains("NO MINVALUE"));
    }

    #[test]
    fn sqlgen_alter_sequence_owned_by_none() {
        use crate::diff::SequenceChanges;

        let mut changes = SequenceChanges::default();
        changes.owned_by = Some(None);
        let op = MigrationOp::AlterSequence {
            name: "public.seq".to_string(),
            changes,
        };
        let sql = generate_sql(&vec![op]);
        assert_eq!(sql.len(), 1);
        assert!(sql[0].contains("OWNED BY NONE"));
    }

    #[test]
    fn sqlgen_trigger_with_old_table() {
        use crate::model::{Trigger, TriggerEnabled, TriggerEvent, TriggerTiming};

        let trigger = Trigger {
            name: "audit_deletes".to_string(),
            target_schema: "public".to_string(),
            target_name: "users".to_string(),
            timing: TriggerTiming::After,
            events: vec![TriggerEvent::Delete],
            update_columns: vec![],
            for_each_row: false,
            when_clause: None,
            function_schema: "public".to_string(),
            function_name: "audit_fn".to_string(),
            function_args: vec![],
            enabled: TriggerEnabled::Origin,
            old_table_name: Some("deleted_rows".to_string()),
            new_table_name: None,
        };

        let ops = vec![MigrationOp::CreateTrigger(trigger)];
        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert!(sql[0].contains("REFERENCING OLD TABLE AS \"deleted_rows\""));
        assert!(!sql[0].contains("NEW TABLE"));
    }

    #[test]
    fn sqlgen_trigger_with_new_table() {
        use crate::model::{Trigger, TriggerEnabled, TriggerEvent, TriggerTiming};

        let trigger = Trigger {
            name: "audit_inserts".to_string(),
            target_schema: "public".to_string(),
            target_name: "users".to_string(),
            timing: TriggerTiming::After,
            events: vec![TriggerEvent::Insert],
            update_columns: vec![],
            for_each_row: false,
            when_clause: None,
            function_schema: "public".to_string(),
            function_name: "audit_fn".to_string(),
            function_args: vec![],
            enabled: TriggerEnabled::Origin,
            old_table_name: None,
            new_table_name: Some("inserted_rows".to_string()),
        };

        let ops = vec![MigrationOp::CreateTrigger(trigger)];
        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert!(sql[0].contains("REFERENCING NEW TABLE AS \"inserted_rows\""));
        assert!(!sql[0].contains("OLD TABLE"));
    }

    #[test]
    fn sqlgen_trigger_with_both_transition_tables() {
        use crate::model::{Trigger, TriggerEnabled, TriggerEvent, TriggerTiming};

        let trigger = Trigger {
            name: "audit_updates".to_string(),
            target_schema: "public".to_string(),
            target_name: "users".to_string(),
            timing: TriggerTiming::After,
            events: vec![TriggerEvent::Update],
            update_columns: vec![],
            for_each_row: false,
            when_clause: None,
            function_schema: "public".to_string(),
            function_name: "audit_fn".to_string(),
            function_args: vec![],
            enabled: TriggerEnabled::Origin,
            old_table_name: Some("old_rows".to_string()),
            new_table_name: Some("new_rows".to_string()),
        };

        let ops = vec![MigrationOp::CreateTrigger(trigger)];
        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert!(sql[0].contains("REFERENCING OLD TABLE AS \"old_rows\" NEW TABLE AS \"new_rows\""));
    }
}
