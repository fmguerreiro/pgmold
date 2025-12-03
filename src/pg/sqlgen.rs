use crate::diff::{ColumnChanges, EnumValuePosition, MigrationOp, PolicyChanges};
use crate::model::{
    parse_qualified_name, CheckConstraint, Column, ForeignKey, Function, Index, IndexType, PgType,
    Policy, PolicyCommand, ReferentialAction, SecurityType, Table, View, Volatility,
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
            vec![format!("DROP TYPE {};", quote_qualified(&schema, &enum_name))]
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
            vec![format!("DROP TABLE {};", quote_qualified(&schema, &table_name))]
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
            vec![generate_add_check_constraint(&schema, &table_name, check_constraint)]
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
            vec![format!("DROP FUNCTION {}({});", quote_qualified(&schema, &func_name), args)]
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
            vec![format!("DROP {} {};", view_type, quote_qualified(&schema, &view_name))]
        }

        MigrationOp::AlterView { new_view, .. } => {
            vec![generate_create_or_replace_view(new_view)]
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
        statements.push(generate_add_foreign_key(&table.schema, &table.name, foreign_key));
    }

    for check_constraint in &table.check_constraints {
        statements.push(generate_add_check_constraint(&table.schema, &table.name, check_constraint));
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

fn generate_add_check_constraint(schema: &str, table: &str, check_constraint: &CheckConstraint) -> String {
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
                qualified_name,
                qualified_name,
                view.query
            )
        } else {
            format!(
                "CREATE MATERIALIZED VIEW {} AS {};",
                qualified_name,
                view.query
            )
        }
    } else {
        let create_stmt = if replace {
            "CREATE OR REPLACE VIEW"
        } else {
            "CREATE VIEW"
        };
        format!(
            "{} {} AS {};",
            create_stmt,
            qualified_name,
            view.query
        )
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
        assert_eq!(sql[0], "ALTER TABLE \"public\".\"users\" DROP COLUMN \"email\";");
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
        assert!(sql[0].contains("ALTER TABLE \"public\".\"posts\" ADD CONSTRAINT \"posts_user_id_fkey\""));
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
        assert_eq!(sql[0], "ALTER TYPE \"public\".\"status\" ADD VALUE 'pending';");
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
        assert_eq!(sql[0], "ALTER TYPE \"public\".\"status\" ADD VALUE 'it''s pending';");
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
}
