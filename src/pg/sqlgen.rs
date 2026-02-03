use crate::diff::{
    ColumnChanges, DomainChanges, EnumValuePosition, GrantObjectKind, MigrationOp, OwnerObjectKind,
    PolicyChanges, SequenceChanges,
};
use crate::model::{
    parse_qualified_name, versioned_schema_name, CheckConstraint, Column, Domain, ForeignKey,
    Function, Index, IndexType, Partition, PartitionBound, PartitionStrategy, PgType, Policy,
    PolicyCommand, Privilege, ReferentialAction, SecurityType, Sequence, SequenceDataType, Table,
    Trigger, TriggerEnabled, TriggerEvent, TriggerTiming, VersionView, View, Volatility,
};

pub fn generate_sql(ops: &[MigrationOp]) -> Vec<String> {
    ops.iter().flat_map(generate_op_sql).collect()
}

fn generate_op_sql(op: &MigrationOp) -> Vec<String> {
    match op {
        MigrationOp::CreateSchema(pg_schema) => {
            vec![format!(
                "CREATE SCHEMA IF NOT EXISTS {};",
                quote_ident(&pg_schema.name)
            )]
        }
        MigrationOp::DropSchema(name) => {
            vec![format!(
                "DROP SCHEMA IF EXISTS {} CASCADE;",
                quote_ident(name)
            )]
        }

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

        MigrationOp::CreatePartition(partition) => {
            vec![generate_create_partition(partition)]
        }

        MigrationOp::DropPartition(name) => {
            let (schema, partition_name) = parse_qualified_name(name);
            vec![format!(
                "DROP TABLE {};",
                quote_qualified(&schema, &partition_name)
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

        MigrationOp::DropIndex { table, index_name } => {
            let (schema, _) = parse_qualified_name(table);
            vec![format!(
                "DROP INDEX {};",
                quote_qualified(&schema, index_name)
            )]
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

        // Note: We don't generate ALTER FUNCTION ... OWNER TO for new functions.
        // PostgreSQL automatically sets the owner to the creating user.
        // Changing ownership requires schema ownership which the user may not have.
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

        MigrationOp::AlterOwner {
            object_kind,
            schema,
            name,
            args,
            new_owner,
        } => vec![generate_alter_owner(
            object_kind,
            schema,
            name,
            args,
            new_owner,
        )],

        MigrationOp::CreateDomain(domain) => {
            vec![generate_create_domain(domain)]
        }

        MigrationOp::DropDomain(name) => {
            let (schema, domain_name) = parse_qualified_name(name);
            vec![format!(
                "DROP DOMAIN {};",
                quote_qualified(&schema, &domain_name)
            )]
        }

        MigrationOp::AlterDomain { name, changes } => generate_alter_domain(name, changes),

        MigrationOp::BackfillHint { hint, .. } => {
            vec![format!("-- Backfill required: {}", hint)]
        }

        MigrationOp::SetColumnNotNull { table, column } => {
            let (schema, table_name) = parse_qualified_name(table);
            vec![format!(
                "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL;",
                quote_qualified(&schema, &table_name),
                quote_ident(column)
            )]
        }

        MigrationOp::GrantPrivileges {
            object_kind,
            schema,
            name,
            args,
            grantee,
            privileges,
            with_grant_option,
        } => {
            vec![format!(
                "GRANT {} ON {} {}{} TO {}{};",
                privileges
                    .iter()
                    .map(privilege_to_sql)
                    .collect::<Vec<_>>()
                    .join(", "),
                grant_object_kind_to_sql(object_kind),
                quote_qualified(schema, name),
                args.as_ref().map(|a| format!("({a})")).unwrap_or_default(),
                if grantee == "PUBLIC" {
                    "PUBLIC".to_string()
                } else {
                    quote_ident(grantee)
                },
                if *with_grant_option {
                    " WITH GRANT OPTION"
                } else {
                    ""
                }
            )]
        }

        MigrationOp::RevokePrivileges {
            object_kind,
            schema,
            name,
            args,
            grantee,
            privileges,
            revoke_grant_option,
        } => {
            vec![format!(
                "REVOKE {}{} ON {} {}{} FROM {};",
                if *revoke_grant_option {
                    "GRANT OPTION FOR "
                } else {
                    ""
                },
                privileges
                    .iter()
                    .map(privilege_to_sql)
                    .collect::<Vec<_>>()
                    .join(", "),
                grant_object_kind_to_sql(object_kind),
                quote_qualified(schema, name),
                args.as_ref().map(|a| format!("({a})")).unwrap_or_default(),
                if grantee == "PUBLIC" {
                    "PUBLIC".to_string()
                } else {
                    quote_ident(grantee)
                }
            )]
        }

        MigrationOp::AlterDefaultPrivileges {
            target_role,
            schema,
            object_type,
            grantee,
            privileges,
            with_grant_option,
            revoke,
        } => {
            let object_type_sql = match object_type {
                crate::model::DefaultPrivilegeObjectType::Tables => "TABLES",
                crate::model::DefaultPrivilegeObjectType::Sequences => "SEQUENCES",
                crate::model::DefaultPrivilegeObjectType::Functions => "FUNCTIONS",
                crate::model::DefaultPrivilegeObjectType::Routines => "ROUTINES",
                crate::model::DefaultPrivilegeObjectType::Types => "TYPES",
                crate::model::DefaultPrivilegeObjectType::Schemas => "SCHEMAS",
            };

            let privs_sql = privileges
                .iter()
                .map(privilege_to_sql)
                .collect::<Vec<_>>()
                .join(", ");

            let schema_clause = schema
                .as_ref()
                .map(|s| format!(" IN SCHEMA {}", quote_ident(s)))
                .unwrap_or_default();

            let grantee_sql = if grantee == "PUBLIC" {
                "PUBLIC".to_string()
            } else {
                quote_ident(grantee)
            };

            if *revoke {
                vec![format!(
                    "ALTER DEFAULT PRIVILEGES FOR ROLE {}{} REVOKE {} ON {} FROM {};",
                    quote_ident(target_role),
                    schema_clause,
                    privs_sql,
                    object_type_sql,
                    grantee_sql
                )]
            } else {
                let grant_option = if *with_grant_option {
                    " WITH GRANT OPTION"
                } else {
                    ""
                };
                vec![format!(
                    "ALTER DEFAULT PRIVILEGES FOR ROLE {}{} GRANT {} ON {} TO {}{};",
                    quote_ident(target_role),
                    schema_clause,
                    privs_sql,
                    object_type_sql,
                    grantee_sql,
                    grant_option
                )]
            }
        }

        MigrationOp::CreateVersionSchema {
            base_schema,
            version,
        } => {
            let schema_name = versioned_schema_name(base_schema, version);
            vec![format!(
                "CREATE SCHEMA IF NOT EXISTS {};",
                quote_ident(&schema_name)
            )]
        }

        MigrationOp::DropVersionSchema {
            base_schema,
            version,
        } => {
            let schema_name = versioned_schema_name(base_schema, version);
            vec![format!(
                "DROP SCHEMA IF EXISTS {} CASCADE;",
                quote_ident(&schema_name)
            )]
        }

        MigrationOp::CreateVersionView { view } => {
            let mut stmts = vec![generate_version_view_ddl(view)];
            if let Some(owner) = &view.owner {
                stmts.push(format!(
                    "ALTER VIEW {} OWNER TO {};",
                    quote_qualified(&view.version_schema, &view.name),
                    quote_ident(owner)
                ));
            }
            stmts
        }

        MigrationOp::DropVersionView {
            version_schema,
            name,
        } => {
            vec![format!(
                "DROP VIEW IF EXISTS {};",
                quote_qualified(version_schema, name)
            )]
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

    // Add PARTITION BY clause if present
    let partition_clause = table.partition_by.as_ref().map_or(String::new(), |pk| {
        let strategy = match pk.strategy {
            PartitionStrategy::Range => "RANGE",
            PartitionStrategy::List => "LIST",
            PartitionStrategy::Hash => "HASH",
        };
        format!(" PARTITION BY {} ({})", strategy, pk.columns.join(", "))
    });

    statements.push(format!(
        "CREATE TABLE {} (\n    {}\n){};",
        qualified_name,
        column_defs.join(",\n    "),
        partition_clause
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

fn generate_create_partition(partition: &Partition) -> String {
    let partition_name = quote_qualified(&partition.schema, &partition.name);
    let parent_name = quote_qualified(&partition.parent_schema, &partition.parent_name);

    let bound_clause = match &partition.bound {
        PartitionBound::Range { from, to } => {
            format!(
                "FOR VALUES FROM ({}) TO ({})",
                from.join(", "),
                to.join(", ")
            )
        }
        PartitionBound::List { values } => {
            format!("FOR VALUES IN ({})", values.join(", "))
        }
        PartitionBound::Hash { modulus, remainder } => {
            format!("FOR VALUES WITH (MODULUS {modulus}, REMAINDER {remainder})")
        }
        PartitionBound::Default => "DEFAULT".to_string(),
    };

    format!("CREATE TABLE {partition_name} PARTITION OF {parent_name} {bound_clause};")
}

fn generate_create_index(schema: &str, table: &str, index: &Index) -> String {
    let unique = if index.unique { "UNIQUE " } else { "" };
    let index_type = match index.index_type {
        IndexType::BTree => "",
        IndexType::Hash => " USING hash",
        IndexType::Gin => " USING gin",
        IndexType::Gist => " USING gist",
    };

    let where_clause = index
        .predicate
        .as_ref()
        .map(|p| format!(" WHERE ({p})"))
        .unwrap_or_default();

    format!(
        "CREATE {}INDEX {}{} ON {} ({}){};",
        unique,
        quote_ident(&index.name),
        index_type,
        quote_qualified(schema, table),
        format_column_list(&index.columns),
        where_clause
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
        let type_str = format_pg_type(data_type);
        statements.push(format!(
            "ALTER TABLE {} ALTER COLUMN {} TYPE {} USING {}::{};",
            qualified,
            quote_ident(column),
            type_str,
            quote_ident(column),
            type_str
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
        PgType::Real => "REAL".to_string(),
        PgType::DoublePrecision => "DOUBLE PRECISION".to_string(),
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
        PgType::Vector(Some(dim)) => format!("vector({dim})"),
        PgType::Vector(None) => "vector".to_string(),
        PgType::CustomEnum(name) => {
            let (schema, enum_name) = parse_qualified_name(name);
            quote_qualified(&schema, &enum_name)
        }
        PgType::Named(name) => {
            if name.contains('.') {
                let (schema, type_name) = parse_qualified_name(name);
                quote_qualified(&schema, &type_name)
            } else {
                name.to_uppercase()
            }
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

/// Strips surrounding double quotes from an identifier and unescapes internal quotes.
/// Handles both quoted ("name") and unquoted (name) identifiers.
pub fn strip_ident_quotes(identifier: &str) -> String {
    let trimmed = identifier.trim();
    if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() >= 2 {
        trimmed[1..trimmed.len() - 1].replace("\"\"", "\"")
    } else {
        trimmed.to_string()
    }
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
        // Only generate TO clause if roles is non-empty (empty roles would generate invalid SQL)
        if !roles.is_empty() {
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

    let config_clause = if func.config_params.is_empty() {
        String::new()
    } else {
        format!(
            " {}",
            func.config_params
                .iter()
                .map(|(k, v)| format!("SET {k} = {v}"))
                .collect::<Vec<_>>()
                .join(" ")
        )
    };

    format!(
        "{} {}({}) RETURNS {} LANGUAGE {} {} {}{} AS $${}$$;",
        create_stmt,
        quote_qualified(&func.schema, &func.name),
        args,
        func.return_type,
        func.language,
        volatility,
        security,
        config_clause,
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

/// Generate DDL for a version view with column mappings.
/// Version views are used in expand/contract migrations to expose multiple schema versions.
fn generate_version_view_ddl(view: &VersionView) -> String {
    let qualified_name = quote_qualified(&view.version_schema, &view.name);
    let base_table = quote_qualified(&view.base_schema, &view.base_table);

    // Build column select list: "physical_col" AS "virtual_col"
    let columns: Vec<String> = view
        .column_mappings
        .iter()
        .map(|m| {
            format!(
                "{} AS {}",
                quote_ident(&m.physical_name),
                quote_ident(&m.virtual_name)
            )
        })
        .collect();

    let column_list = columns.join(", ");

    // Security invoker option for PG 15+ (required for RLS to work through views)
    let with_options = if view.security_invoker {
        " WITH (security_invoker = true)"
    } else {
        ""
    };

    format!("CREATE OR REPLACE VIEW {qualified_name}{with_options} AS SELECT {column_list} FROM {base_table};")
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

    // PostgreSQL sequence options order:
    // INCREMENT BY, MINVALUE, MAXVALUE, START WITH, CACHE, CYCLE, OWNED BY
    if let Some(increment) = seq.increment {
        parts.push(format!("INCREMENT BY {increment}"));
    }

    if let Some(min_value) = seq.min_value {
        parts.push(format!("MINVALUE {min_value}"));
    }

    if let Some(max_value) = seq.max_value {
        parts.push(format!("MAXVALUE {max_value}"));
    }

    if let Some(start) = seq.start {
        parts.push(format!("START WITH {start}"));
    }

    if let Some(cache) = seq.cache {
        parts.push(format!("CACHE {cache}"));
    }

    if seq.cycle {
        parts.push("CYCLE".to_string());
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

fn generate_alter_owner(
    object_kind: &OwnerObjectKind,
    schema: &str,
    name: &str,
    args: &Option<String>,
    new_owner: &str,
) -> String {
    let object_type = match object_kind {
        OwnerObjectKind::Table => "TABLE",
        OwnerObjectKind::View => "VIEW",
        OwnerObjectKind::Sequence => "SEQUENCE",
        OwnerObjectKind::Function => "FUNCTION",
        OwnerObjectKind::Type => "TYPE",
        OwnerObjectKind::Domain => "DOMAIN",
    };

    let qualified_name = quote_qualified(schema, name);

    let full_name = if let Some(function_args) = args {
        format!("{qualified_name}({function_args})")
    } else {
        qualified_name
    };

    format!(
        "ALTER {} {} OWNER TO {};",
        object_type,
        full_name,
        quote_ident(new_owner)
    )
}

fn generate_create_domain(domain: &Domain) -> String {
    let mut parts = vec![format!(
        "CREATE DOMAIN {} AS {}",
        quote_qualified(&domain.schema, &domain.name),
        format_pg_type(&domain.data_type)
    )];

    if let Some(ref collation) = domain.collation {
        parts.push(format!("COLLATE {collation}"));
    }

    if let Some(ref default) = domain.default {
        parts.push(format!("DEFAULT {default}"));
    }

    if domain.not_null {
        parts.push("NOT NULL".to_string());
    }

    for constraint in &domain.check_constraints {
        let constraint_sql = match &constraint.name {
            Some(name) => format!(
                "CONSTRAINT {} CHECK ({})",
                quote_ident(name),
                constraint.expression
            ),
            None => format!("CHECK ({})", constraint.expression),
        };
        parts.push(constraint_sql);
    }

    format!("{};", parts.join(" "))
}

fn generate_alter_domain(name: &str, changes: &DomainChanges) -> Vec<String> {
    let (schema, domain_name) = parse_qualified_name(name);
    let qualified = quote_qualified(&schema, &domain_name);
    let mut statements = Vec::new();

    if let Some(ref default_change) = changes.default {
        match default_change {
            Some(new_default) => {
                statements.push(format!(
                    "ALTER DOMAIN {qualified} SET DEFAULT {new_default};"
                ));
            }
            None => {
                statements.push(format!("ALTER DOMAIN {qualified} DROP DEFAULT;"));
            }
        }
    }

    if let Some(not_null) = changes.not_null {
        if not_null {
            statements.push(format!("ALTER DOMAIN {qualified} SET NOT NULL;"));
        } else {
            statements.push(format!("ALTER DOMAIN {qualified} DROP NOT NULL;"));
        }
    }

    statements
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

fn privilege_to_sql(privilege: &Privilege) -> &'static str {
    match privilege {
        Privilege::Select => "SELECT",
        Privilege::Insert => "INSERT",
        Privilege::Update => "UPDATE",
        Privilege::Delete => "DELETE",
        Privilege::Truncate => "TRUNCATE",
        Privilege::References => "REFERENCES",
        Privilege::Trigger => "TRIGGER",
        Privilege::Usage => "USAGE",
        Privilege::Execute => "EXECUTE",
        Privilege::Create => "CREATE",
    }
}

fn grant_object_kind_to_sql(kind: &GrantObjectKind) -> &'static str {
    match kind {
        GrantObjectKind::Table => "TABLE",
        GrantObjectKind::View => "VIEW",
        GrantObjectKind::Sequence => "SEQUENCE",
        GrantObjectKind::Function => "FUNCTION",
        GrantObjectKind::Schema => "SCHEMA",
        GrantObjectKind::Type => "TYPE",
        GrantObjectKind::Domain => "DOMAIN",
    }
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

            owner: None,
            grants: Vec::new(),
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
            partition_by: None,

            owner: None,
            grants: Vec::new(),
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
                predicate: None,
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
    fn drop_index_generates_schema_qualified_sql() {
        let ops = vec![MigrationOp::DropIndex {
            table: "auth.mfa_factors".to_string(),
            index_name: "mfa_factors_user_friendly_name_unique".to_string(),
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "DROP INDEX \"auth\".\"mfa_factors_user_friendly_name_unique\";"
        );
    }

    #[test]
    fn alter_column_type_generates_valid_sql_with_using_clause() {
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
            "ALTER TABLE \"public\".\"users\" ALTER COLUMN \"name\" TYPE VARCHAR(100) USING \"name\"::VARCHAR(100);"
        );
    }

    #[test]
    fn alter_column_text_to_uuid_generates_using_clause() {
        let ops = vec![MigrationOp::AlterColumn {
            table: "public.users".to_string(),
            column: "id".to_string(),
            changes: ColumnChanges {
                data_type: Some(PgType::Uuid),
                nullable: None,
                default: None,
            },
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "ALTER TABLE \"public\".\"users\" ALTER COLUMN \"id\" TYPE UUID USING \"id\"::UUID;"
        );
    }

    #[test]
    fn create_view_generates_valid_sql() {
        let ops = vec![MigrationOp::CreateView(View {
            name: "active_users".to_string(),
            schema: "public".to_string(),
            query: "SELECT * FROM users WHERE active = true".to_string(),
            materialized: false,

            owner: None,
            grants: Vec::new(),
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

            owner: None,
            grants: Vec::new(),
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
    fn create_schema_generates_valid_sql() {
        let ops = vec![MigrationOp::CreateSchema(crate::model::PgSchema {
            name: "auth".to_string(),
            grants: Vec::new(),
        })];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(sql[0], "CREATE SCHEMA IF NOT EXISTS \"auth\";");
    }

    #[test]
    fn drop_schema_generates_valid_sql() {
        let ops = vec![MigrationOp::DropSchema("old_schema".to_string())];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(sql[0], "DROP SCHEMA IF EXISTS \"old_schema\" CASCADE;");
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
            partition_by: None,

            owner: None,
            grants: Vec::new(),
        };

        let op = MigrationOp::CreateTable(table);
        let sql = generate_sql(&[op]);
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

            owner: None,
            grants: Vec::new(),
        };
        let op = MigrationOp::CreateSequence(seq);
        let sql = generate_sql(&[op]);
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
            owner: None,
            grants: Vec::new(),
            cache: Some(10),
            owned_by: Some(SequenceOwner {
                table_schema: "auth".to_string(),
                table_name: "users".to_string(),
                column_name: "id".to_string(),
            }),
        };
        let op = MigrationOp::CreateSequence(seq);
        let sql = generate_sql(&[op]);
        assert_eq!(sql.len(), 1);
        assert!(sql[0].contains("CREATE SEQUENCE \"auth\".\"counter_seq\""));
        assert!(sql[0].contains("AS integer"));
        // PostgreSQL order: INCREMENT BY before START WITH
        assert!(sql[0].contains("INCREMENT BY 5"));
        assert!(sql[0].contains("MINVALUE 1"));
        assert!(sql[0].contains("MAXVALUE 1000"));
        assert!(sql[0].contains("START WITH 100"));
        assert!(sql[0].contains("CACHE 10"));
        assert!(sql[0].contains("CYCLE"));
        assert!(sql[0].contains("OWNED BY \"auth\".\"users\".\"id\""));
    }

    #[test]
    fn sqlgen_drop_sequence() {
        let op = MigrationOp::DropSequence("public.users_id_seq".to_string());
        let sql = generate_sql(&[op]);
        assert_eq!(sql.len(), 1);
        assert_eq!(sql[0], "DROP SEQUENCE \"public\".\"users_id_seq\";");
    }

    #[test]
    fn sqlgen_alter_sequence_increment() {
        use crate::diff::SequenceChanges;

        let changes = SequenceChanges {
            increment: Some(10),
            ..Default::default()
        };
        let op = MigrationOp::AlterSequence {
            name: "public.counter_seq".to_string(),
            changes,
        };
        let sql = generate_sql(&[op]);
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
        let sql = generate_sql(&[op]);
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

        let changes = SequenceChanges {
            min_value: Some(None),
            ..Default::default()
        };
        let op = MigrationOp::AlterSequence {
            name: "public.seq".to_string(),
            changes,
        };
        let sql = generate_sql(&[op]);
        assert_eq!(sql.len(), 1);
        assert!(sql[0].contains("NO MINVALUE"));
    }

    #[test]
    fn sqlgen_alter_sequence_owned_by_none() {
        use crate::diff::SequenceChanges;

        let changes = SequenceChanges {
            owned_by: Some(None),
            ..Default::default()
        };
        let op = MigrationOp::AlterSequence {
            name: "public.seq".to_string(),
            changes,
        };
        let sql = generate_sql(&[op]);
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

    #[test]
    fn sqlgen_create_domain_simple() {
        use crate::model::Domain;

        let domain = Domain {
            schema: "public".to_string(),
            name: "email".to_string(),
            data_type: PgType::Varchar(Some(255)),
            default: None,
            not_null: false,
            collation: None,
            check_constraints: vec![],

            owner: None,
            grants: Vec::new(),
        };

        let ops = vec![MigrationOp::CreateDomain(domain)];
        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "CREATE DOMAIN \"public\".\"email\" AS VARCHAR(255);"
        );
    }

    #[test]
    fn sqlgen_create_domain_with_default_and_not_null() {
        use crate::model::Domain;

        let domain = Domain {
            schema: "public".to_string(),
            name: "positive_int".to_string(),
            data_type: PgType::Integer,
            default: Some("0".to_string()),
            not_null: true,
            collation: None,
            check_constraints: vec![],

            owner: None,
            grants: Vec::new(),
        };

        let ops = vec![MigrationOp::CreateDomain(domain)];
        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "CREATE DOMAIN \"public\".\"positive_int\" AS INTEGER DEFAULT 0 NOT NULL;"
        );
    }

    #[test]
    fn sqlgen_create_domain_with_check_constraint() {
        use crate::model::{Domain, DomainConstraint};

        let domain = Domain {
            schema: "public".to_string(),
            name: "positive_int".to_string(),
            data_type: PgType::Integer,
            default: None,
            not_null: false,
            collation: None,
            check_constraints: vec![DomainConstraint {
                name: Some("positive_check".to_string()),
                expression: "VALUE > 0".to_string(),
            }],
            owner: None,
            grants: Vec::new(),
        };

        let ops = vec![MigrationOp::CreateDomain(domain)];
        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "CREATE DOMAIN \"public\".\"positive_int\" AS INTEGER CONSTRAINT \"positive_check\" CHECK (VALUE > 0);"
        );
    }

    #[test]
    fn sqlgen_drop_domain() {
        let ops = vec![MigrationOp::DropDomain("public.email".to_string())];
        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(sql[0], "DROP DOMAIN \"public\".\"email\";");
    }

    #[test]
    fn sqlgen_alter_domain_set_default() {
        let changes = DomainChanges {
            default: Some(Some("'unknown'".to_string())),
            not_null: None,
        };
        let ops = vec![MigrationOp::AlterDomain {
            name: "public.email".to_string(),
            changes,
        }];
        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "ALTER DOMAIN \"public\".\"email\" SET DEFAULT 'unknown';"
        );
    }

    #[test]
    fn sqlgen_alter_domain_drop_default() {
        let changes = DomainChanges {
            default: Some(None),
            not_null: None,
        };
        let ops = vec![MigrationOp::AlterDomain {
            name: "public.email".to_string(),
            changes,
        }];
        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(sql[0], "ALTER DOMAIN \"public\".\"email\" DROP DEFAULT;");
    }

    #[test]
    fn sqlgen_alter_domain_set_not_null() {
        let changes = DomainChanges {
            default: None,
            not_null: Some(true),
        };
        let ops = vec![MigrationOp::AlterDomain {
            name: "public.email".to_string(),
            changes,
        }];
        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(sql[0], "ALTER DOMAIN \"public\".\"email\" SET NOT NULL;");
    }

    #[test]
    fn sqlgen_alter_domain_drop_not_null() {
        let changes = DomainChanges {
            default: None,
            not_null: Some(false),
        };
        let ops = vec![MigrationOp::AlterDomain {
            name: "public.email".to_string(),
            changes,
        }];
        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(sql[0], "ALTER DOMAIN \"public\".\"email\" DROP NOT NULL;");
    }

    #[test]
    fn strip_ident_quotes_removes_surrounding_quotes() {
        assert_eq!(strip_ident_quotes("\"p_role_name\""), "p_role_name");
        assert_eq!(strip_ident_quotes("p_role_name"), "p_role_name");
        assert_eq!(strip_ident_quotes("\"\"\"triple\"\"\""), "\"triple\"");
        assert_eq!(strip_ident_quotes("\"has\"\"escaped\""), "has\"escaped");
    }

    #[test]
    fn generate_function_ddl_quotes_parameter_names_correctly() {
        use crate::model::{ArgMode, Function, FunctionArg, SecurityType, Volatility};

        let func = Function {
            name: "is_org_admin".to_string(),
            schema: "auth".to_string(),
            arguments: vec![
                FunctionArg {
                    name: Some("p_role_name".to_string()),
                    data_type: "text".to_string(),
                    mode: ArgMode::In,
                    default: None,
                },
                FunctionArg {
                    name: Some("p_enterprise_id".to_string()),
                    data_type: "uuid".to_string(),
                    mode: ArgMode::In,
                    default: Some("null::uuid".to_string()),
                },
            ],
            return_type: "boolean".to_string(),
            language: "sql".to_string(),
            body: "SELECT true".to_string(),
            volatility: Volatility::Volatile,
            security: SecurityType::Definer,
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
        };

        let ddl = generate_create_function(&func);

        assert!(
            ddl.contains("\"p_role_name\" text"),
            "Expected single-quoted param name, got: {ddl}"
        );
        assert!(
            ddl.contains("\"p_enterprise_id\" uuid DEFAULT null::uuid"),
            "Expected single-quoted param with default, got: {ddl}"
        );
        assert!(
            !ddl.contains("\"\"\""),
            "Should not have triple quotes in: {ddl}"
        );
    }

    #[test]
    fn sqlgen_backfill_hint() {
        let op = MigrationOp::BackfillHint {
            table: "users".to_string(),
            column: "email".to_string(),
            hint: "UPDATE users SET email = <value> WHERE email IS NULL;".to_string(),
        };
        let sql = generate_sql(&[op]);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "-- Backfill required: UPDATE users SET email = <value> WHERE email IS NULL;"
        );
    }

    #[test]
    fn sqlgen_set_column_not_null() {
        let op = MigrationOp::SetColumnNotNull {
            table: "users".to_string(),
            column: "email".to_string(),
        };
        let sql = generate_sql(&[op]);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "ALTER TABLE \"public\".\"users\" ALTER COLUMN \"email\" SET NOT NULL;"
        );
    }

    #[test]
    fn sqlgen_set_column_not_null_with_schema() {
        let op = MigrationOp::SetColumnNotNull {
            table: "auth.users".to_string(),
            column: "email".to_string(),
        };
        let sql = generate_sql(&[op]);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "ALTER TABLE \"auth\".\"users\" ALTER COLUMN \"email\" SET NOT NULL;"
        );
    }

    #[test]
    fn generate_function_ddl_with_config_params() {
        use crate::model::{Function, SecurityType, Volatility};

        let func = Function {
            name: "test_func".to_string(),
            schema: "public".to_string(),
            arguments: vec![],
            return_type: "void".to_string(),
            language: "sql".to_string(),
            body: "SELECT 1".to_string(),
            volatility: Volatility::Volatile,
            security: SecurityType::Definer,
            config_params: vec![("search_path".to_string(), "public".to_string())],
            owner: None,
            grants: Vec::new(),
        };

        let ddl = generate_create_function(&func);

        assert!(
            ddl.contains("SET search_path = public"),
            "Expected SET clause in: {ddl}"
        );
    }

    #[test]
    fn generate_function_ddl_with_multiple_config_params() {
        use crate::model::{Function, SecurityType, Volatility};

        let func = Function {
            name: "test_func".to_string(),
            schema: "public".to_string(),
            arguments: vec![],
            return_type: "void".to_string(),
            language: "sql".to_string(),
            body: "SELECT 1".to_string(),
            volatility: Volatility::Volatile,
            security: SecurityType::Definer,
            config_params: vec![
                ("search_path".to_string(), "public".to_string()),
                ("work_mem".to_string(), "'64MB'".to_string()),
            ],
            owner: None,
            grants: Vec::new(),
        };

        let ddl = generate_create_function(&func);

        assert!(
            ddl.contains("SET search_path = public SET work_mem = '64MB'"),
            "Expected multiple SET clauses in: {ddl}"
        );
    }

    #[test]
    fn create_function_does_not_generate_owner_to() {
        use crate::model::{Function, SecurityType, Volatility};

        // Function with owner set (e.g., from introspection or schema file)
        let func = Function {
            name: "my_func".to_string(),
            schema: "auth".to_string(),
            arguments: vec![],
            return_type: "void".to_string(),
            language: "sql".to_string(),
            body: "SELECT 1".to_string(),
            volatility: Volatility::Volatile,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: Some("supabase_auth_admin".to_string()),
            grants: Vec::new(),
        };

        let ops = vec![MigrationOp::CreateFunction(func)];
        let sql = generate_sql(&ops);

        // Should only generate CREATE FUNCTION, not ALTER FUNCTION ... OWNER TO
        assert_eq!(sql.len(), 1, "Expected 1 SQL statement, got {}", sql.len());
        assert!(
            sql[0].starts_with("CREATE FUNCTION"),
            "Expected CREATE FUNCTION, got: {}",
            sql[0]
        );
        assert!(
            !sql[0].contains("OWNER TO"),
            "Should not contain OWNER TO: {}",
            sql[0]
        );
    }

    #[test]
    fn alter_owner_table_generates_valid_sql() {
        let ops = vec![MigrationOp::AlterOwner {
            object_kind: OwnerObjectKind::Table,
            schema: "public".to_string(),
            name: "users".to_string(),
            args: None,
            new_owner: "new_owner".to_string(),
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "ALTER TABLE \"public\".\"users\" OWNER TO \"new_owner\";"
        );
    }

    #[test]
    fn alter_owner_view_generates_valid_sql() {
        let ops = vec![MigrationOp::AlterOwner {
            object_kind: OwnerObjectKind::View,
            schema: "public".to_string(),
            name: "active_users".to_string(),
            args: None,
            new_owner: "app_user".to_string(),
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "ALTER VIEW \"public\".\"active_users\" OWNER TO \"app_user\";"
        );
    }

    #[test]
    fn alter_owner_sequence_generates_valid_sql() {
        let ops = vec![MigrationOp::AlterOwner {
            object_kind: OwnerObjectKind::Sequence,
            schema: "public".to_string(),
            name: "users_id_seq".to_string(),
            args: None,
            new_owner: "db_admin".to_string(),
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "ALTER SEQUENCE \"public\".\"users_id_seq\" OWNER TO \"db_admin\";"
        );
    }

    #[test]
    fn alter_owner_function_generates_valid_sql() {
        let ops = vec![MigrationOp::AlterOwner {
            object_kind: OwnerObjectKind::Function,
            schema: "auth".to_string(),
            name: "check_user".to_string(),
            args: Some("text, uuid".to_string()),
            new_owner: "supabase_auth_admin".to_string(),
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "ALTER FUNCTION \"auth\".\"check_user\"(text, uuid) OWNER TO \"supabase_auth_admin\";"
        );
    }

    #[test]
    fn alter_owner_function_no_args_generates_valid_sql() {
        let ops = vec![MigrationOp::AlterOwner {
            object_kind: OwnerObjectKind::Function,
            schema: "public".to_string(),
            name: "get_timestamp".to_string(),
            args: Some("".to_string()),
            new_owner: "app_owner".to_string(),
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "ALTER FUNCTION \"public\".\"get_timestamp\"() OWNER TO \"app_owner\";"
        );
    }

    #[test]
    fn alter_owner_type_enum_generates_valid_sql() {
        let ops = vec![MigrationOp::AlterOwner {
            object_kind: OwnerObjectKind::Type,
            schema: "public".to_string(),
            name: "user_role".to_string(),
            args: None,
            new_owner: "role_admin".to_string(),
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "ALTER TYPE \"public\".\"user_role\" OWNER TO \"role_admin\";"
        );
    }

    #[test]
    fn alter_owner_domain_generates_valid_sql() {
        let ops = vec![MigrationOp::AlterOwner {
            object_kind: OwnerObjectKind::Domain,
            schema: "public".to_string(),
            name: "email".to_string(),
            args: None,
            new_owner: "domain_owner".to_string(),
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "ALTER DOMAIN \"public\".\"email\" OWNER TO \"domain_owner\";"
        );
    }

    #[test]
    fn grant_privileges_table_generates_valid_sql() {
        use crate::model::Privilege;

        let ops = vec![MigrationOp::GrantPrivileges {
            object_kind: GrantObjectKind::Table,
            schema: "public".to_string(),
            name: "users".to_string(),
            args: None,
            grantee: "app_user".to_string(),
            privileges: vec![Privilege::Select, Privilege::Insert],
            with_grant_option: false,
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "GRANT SELECT, INSERT ON TABLE \"public\".\"users\" TO \"app_user\";"
        );
    }

    #[test]
    fn grant_privileges_with_grant_option() {
        use crate::model::Privilege;

        let ops = vec![MigrationOp::GrantPrivileges {
            object_kind: GrantObjectKind::Table,
            schema: "public".to_string(),
            name: "users".to_string(),
            args: None,
            grantee: "admin_user".to_string(),
            privileges: vec![Privilege::Select],
            with_grant_option: true,
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "GRANT SELECT ON TABLE \"public\".\"users\" TO \"admin_user\" WITH GRANT OPTION;"
        );
    }

    #[test]
    fn grant_privileges_to_public() {
        use crate::model::Privilege;

        let ops = vec![MigrationOp::GrantPrivileges {
            object_kind: GrantObjectKind::Sequence,
            schema: "public".to_string(),
            name: "user_id_seq".to_string(),
            args: None,
            grantee: "PUBLIC".to_string(),
            privileges: vec![Privilege::Usage],
            with_grant_option: false,
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "GRANT USAGE ON SEQUENCE \"public\".\"user_id_seq\" TO PUBLIC;"
        );
    }

    #[test]
    fn grant_privileges_function_generates_valid_sql() {
        use crate::model::Privilege;

        let ops = vec![MigrationOp::GrantPrivileges {
            object_kind: GrantObjectKind::Function,
            schema: "public".to_string(),
            name: "calculate".to_string(),
            args: Some("integer, text".to_string()),
            grantee: "app_user".to_string(),
            privileges: vec![Privilege::Execute],
            with_grant_option: false,
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "GRANT EXECUTE ON FUNCTION \"public\".\"calculate\"(integer, text) TO \"app_user\";"
        );
    }

    #[test]
    fn revoke_privileges_generates_valid_sql() {
        use crate::model::Privilege;

        let ops = vec![MigrationOp::RevokePrivileges {
            object_kind: GrantObjectKind::Table,
            schema: "public".to_string(),
            name: "users".to_string(),
            args: None,
            grantee: "old_user".to_string(),
            privileges: vec![Privilege::Delete],
            revoke_grant_option: false,
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "REVOKE DELETE ON TABLE \"public\".\"users\" FROM \"old_user\";"
        );
    }

    #[test]
    fn revoke_grant_option_generates_valid_sql() {
        use crate::model::Privilege;

        let ops = vec![MigrationOp::RevokePrivileges {
            object_kind: GrantObjectKind::View,
            schema: "public".to_string(),
            name: "user_view".to_string(),
            args: None,
            grantee: "viewer".to_string(),
            privileges: vec![Privilege::Select],
            revoke_grant_option: true,
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "REVOKE GRANT OPTION FOR SELECT ON VIEW \"public\".\"user_view\" FROM \"viewer\";"
        );
    }

    #[test]
    fn grant_all_privilege_types() {
        use crate::model::Privilege;

        let ops = vec![MigrationOp::GrantPrivileges {
            object_kind: GrantObjectKind::Table,
            schema: "public".to_string(),
            name: "users".to_string(),
            args: None,
            grantee: "power_user".to_string(),
            privileges: vec![
                Privilege::Select,
                Privilege::Insert,
                Privilege::Update,
                Privilege::Delete,
                Privilege::Truncate,
                Privilege::References,
                Privilege::Trigger,
            ],
            with_grant_option: false,
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert!(
            sql[0].contains("GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER")
        );
    }

    #[test]
    fn create_version_schema_generates_valid_sql() {
        let ops = vec![MigrationOp::CreateVersionSchema {
            base_schema: "public".to_string(),
            version: "v0001".to_string(),
        }];
        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(sql[0], "CREATE SCHEMA IF NOT EXISTS \"public_v0001\";");
    }

    #[test]
    fn drop_version_schema_generates_valid_sql() {
        let ops = vec![MigrationOp::DropVersionSchema {
            base_schema: "public".to_string(),
            version: "v0001".to_string(),
        }];
        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(sql[0], "DROP SCHEMA IF EXISTS \"public_v0001\" CASCADE;");
    }

    #[test]
    fn create_version_view_basic_generates_valid_sql() {
        use crate::model::{ColumnMapping, VersionView};
        let view = VersionView {
            name: "users".to_string(),
            base_schema: "public".to_string(),
            version_schema: "public_v0001".to_string(),
            base_table: "users".to_string(),
            column_mappings: vec![
                ColumnMapping {
                    virtual_name: "id".to_string(),
                    physical_name: "id".to_string(),
                },
                ColumnMapping {
                    virtual_name: "name".to_string(),
                    physical_name: "name".to_string(),
                },
            ],
            security_invoker: false,
            owner: None,
        };
        let ops = vec![MigrationOp::CreateVersionView { view }];
        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "CREATE OR REPLACE VIEW \"public_v0001\".\"users\" AS SELECT \"id\" AS \"id\", \"name\" AS \"name\" FROM \"public\".\"users\";"
        );
    }

    #[test]
    fn create_version_view_with_security_invoker_generates_valid_sql() {
        use crate::model::{ColumnMapping, VersionView};
        let view = VersionView {
            name: "users".to_string(),
            base_schema: "public".to_string(),
            version_schema: "public_v0002".to_string(),
            base_table: "users".to_string(),
            column_mappings: vec![ColumnMapping {
                virtual_name: "id".to_string(),
                physical_name: "id".to_string(),
            }],
            security_invoker: true,
            owner: None,
        };
        let ops = vec![MigrationOp::CreateVersionView { view }];
        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "CREATE OR REPLACE VIEW \"public_v0002\".\"users\" WITH (security_invoker = true) AS SELECT \"id\" AS \"id\" FROM \"public\".\"users\";"
        );
    }

    #[test]
    fn create_version_view_with_renamed_column_generates_valid_sql() {
        use crate::model::{ColumnMapping, VersionView};
        let view = VersionView {
            name: "users".to_string(),
            base_schema: "public".to_string(),
            version_schema: "public_v0002".to_string(),
            base_table: "users".to_string(),
            column_mappings: vec![
                ColumnMapping {
                    virtual_name: "id".to_string(),
                    physical_name: "id".to_string(),
                },
                ColumnMapping {
                    virtual_name: "description".to_string(),
                    physical_name: "_pgroll_new_description".to_string(),
                },
            ],
            security_invoker: true,
            owner: None,
        };
        let ops = vec![MigrationOp::CreateVersionView { view }];
        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "CREATE OR REPLACE VIEW \"public_v0002\".\"users\" WITH (security_invoker = true) AS SELECT \"id\" AS \"id\", \"_pgroll_new_description\" AS \"description\" FROM \"public\".\"users\";"
        );
    }

    #[test]
    fn drop_version_view_generates_valid_sql() {
        let ops = vec![MigrationOp::DropVersionView {
            version_schema: "public_v0001".to_string(),
            name: "users".to_string(),
        }];
        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(sql[0], "DROP VIEW IF EXISTS \"public_v0001\".\"users\";");
    }

    #[test]
    fn alter_default_privileges_grant_generates_valid_sql() {
        use crate::model::{DefaultPrivilegeObjectType, Privilege};

        let ops = vec![MigrationOp::AlterDefaultPrivileges {
            target_role: "admin".to_string(),
            schema: Some("public".to_string()),
            object_type: DefaultPrivilegeObjectType::Tables,
            grantee: "app_user".to_string(),
            privileges: vec![Privilege::Select, Privilege::Insert],
            with_grant_option: false,
            revoke: false,
        }];

        let sql = generate_sql(&ops);

        assert!(
            sql.contains(&"ALTER DEFAULT PRIVILEGES FOR ROLE \"admin\" IN SCHEMA \"public\" GRANT SELECT, INSERT ON TABLES TO \"app_user\";".to_string()),
            "Should generate correct ALTER DEFAULT PRIVILEGES SQL. SQL: {sql:?}"
        );
    }

    #[test]
    fn alter_default_privileges_revoke_generates_valid_sql() {
        use crate::model::{DefaultPrivilegeObjectType, Privilege};

        let ops = vec![MigrationOp::AlterDefaultPrivileges {
            target_role: "admin".to_string(),
            schema: None,
            object_type: DefaultPrivilegeObjectType::Functions,
            grantee: "app_user".to_string(),
            privileges: vec![Privilege::Execute],
            with_grant_option: false,
            revoke: true,
        }];

        let sql = generate_sql(&ops);

        assert!(
            sql.contains(&"ALTER DEFAULT PRIVILEGES FOR ROLE \"admin\" REVOKE EXECUTE ON FUNCTIONS FROM \"app_user\";".to_string()),
            "Should generate correct REVOKE SQL without IN SCHEMA. SQL: {sql:?}"
        );
    }

    #[test]
    fn alter_default_privileges_with_grant_option() {
        use crate::model::{DefaultPrivilegeObjectType, Privilege};

        let ops = vec![MigrationOp::AlterDefaultPrivileges {
            target_role: "admin".to_string(),
            schema: Some("api".to_string()),
            object_type: DefaultPrivilegeObjectType::Sequences,
            grantee: "service_role".to_string(),
            privileges: vec![Privilege::Usage],
            with_grant_option: true,
            revoke: false,
        }];

        let sql = generate_sql(&ops);

        assert!(
            sql.contains(&"ALTER DEFAULT PRIVILEGES FOR ROLE \"admin\" IN SCHEMA \"api\" GRANT USAGE ON SEQUENCES TO \"service_role\" WITH GRANT OPTION;".to_string()),
            "Should generate SQL WITH GRANT OPTION. SQL: {sql:?}"
        );
    }

    #[test]
    fn alter_default_privileges_public_grantee() {
        use crate::model::{DefaultPrivilegeObjectType, Privilege};

        let ops = vec![MigrationOp::AlterDefaultPrivileges {
            target_role: "admin".to_string(),
            schema: Some("public".to_string()),
            object_type: DefaultPrivilegeObjectType::Types,
            grantee: "PUBLIC".to_string(),
            privileges: vec![Privilege::Usage],
            with_grant_option: false,
            revoke: false,
        }];

        let sql = generate_sql(&ops);

        assert!(
            sql.contains(&"ALTER DEFAULT PRIVILEGES FOR ROLE \"admin\" IN SCHEMA \"public\" GRANT USAGE ON TYPES TO PUBLIC;".to_string()),
            "Should not quote PUBLIC grantee. SQL: {sql:?}"
        );
    }
}
