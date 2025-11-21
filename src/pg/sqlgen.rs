use crate::diff::{ColumnChanges, MigrationOp, PolicyChanges};
use crate::model::{
    Column, ForeignKey, Function, Index, IndexType, PgType, Policy, PolicyCommand,
    ReferentialAction, SecurityType, Table, Volatility,
};

pub fn generate_sql(ops: &[MigrationOp]) -> Vec<String> {
    ops.iter().flat_map(generate_op_sql).collect()
}

fn generate_op_sql(op: &MigrationOp) -> Vec<String> {
    match op {
        MigrationOp::CreateEnum(enum_type) => vec![format!(
            "CREATE TYPE {} AS ENUM ({});",
            quote_ident(&enum_type.name),
            enum_type
                .values
                .iter()
                .map(|v| format!("'{}'", escape_string(v)))
                .collect::<Vec<_>>()
                .join(", ")
        )],

        MigrationOp::DropEnum(name) => vec![format!("DROP TYPE {};", quote_ident(name))],

        MigrationOp::CreateTable(table) => generate_create_table(table),

        MigrationOp::DropTable(name) => vec![format!("DROP TABLE {};", quote_ident(name))],

        MigrationOp::AddColumn { table, column } => vec![format!(
            "ALTER TABLE {} ADD COLUMN {};",
            quote_ident(table),
            format_column(column)
        )],

        MigrationOp::DropColumn { table, column } => vec![format!(
            "ALTER TABLE {} DROP COLUMN {};",
            quote_ident(table),
            quote_ident(column)
        )],

        MigrationOp::AlterColumn {
            table,
            column,
            changes,
        } => generate_alter_column(table, column, changes),

        MigrationOp::AddPrimaryKey { table, primary_key } => vec![format!(
            "ALTER TABLE {} ADD PRIMARY KEY ({});",
            quote_ident(table),
            format_column_list(&primary_key.columns)
        )],

        MigrationOp::DropPrimaryKey { table } => vec![format!(
            "ALTER TABLE {} DROP CONSTRAINT {}_pkey;",
            quote_ident(table),
            quote_ident(table)
        )],

        MigrationOp::AddIndex { table, index } => vec![generate_create_index(table, index)],

        MigrationOp::DropIndex { index_name, .. } => {
            vec![format!("DROP INDEX {};", quote_ident(index_name))]
        }

        MigrationOp::AddForeignKey { table, foreign_key } => {
            vec![generate_add_foreign_key(table, foreign_key)]
        }

        MigrationOp::DropForeignKey {
            table,
            foreign_key_name,
        } => vec![format!(
            "ALTER TABLE {} DROP CONSTRAINT {};",
            quote_ident(table),
            quote_ident(foreign_key_name)
        )],

        MigrationOp::EnableRls { table } => vec![format!(
            "ALTER TABLE {} ENABLE ROW LEVEL SECURITY;",
            quote_ident(table)
        )],

        MigrationOp::DisableRls { table } => vec![format!(
            "ALTER TABLE {} DISABLE ROW LEVEL SECURITY;",
            quote_ident(table)
        )],

        MigrationOp::CreatePolicy(policy) => vec![generate_create_policy(policy)],

        MigrationOp::DropPolicy { table, name } => vec![format!(
            "DROP POLICY {} ON {};",
            quote_ident(name),
            quote_ident(table)
        )],

        MigrationOp::AlterPolicy {
            table,
            name,
            changes,
        } => generate_alter_policy(table, name, changes),

        MigrationOp::CreateFunction(func) => vec![generate_create_function(func)],

        MigrationOp::DropFunction { name, args } => {
            vec![format!("DROP FUNCTION {}({});", quote_ident(name), args)]
        }

        MigrationOp::AlterFunction { new_function, .. } => {
            vec![generate_create_or_replace_function(new_function)]
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

    statements.push(format!(
        "CREATE TABLE {} (\n    {}\n);",
        quote_ident(&table.name),
        column_defs.join(",\n    ")
    ));

    for index in &table.indexes {
        statements.push(generate_create_index(&table.name, index));
    }

    for foreign_key in &table.foreign_keys {
        statements.push(generate_add_foreign_key(&table.name, foreign_key));
    }

    statements
}

fn generate_create_index(table: &str, index: &Index) -> String {
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
        quote_ident(table),
        format_column_list(&index.columns)
    )
}

fn generate_add_foreign_key(table: &str, foreign_key: &ForeignKey) -> String {
    format!(
        "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({}) ON DELETE {} ON UPDATE {};",
        quote_ident(table),
        quote_ident(&foreign_key.name),
        format_column_list(&foreign_key.columns),
        quote_ident(&foreign_key.referenced_table),
        format_column_list(&foreign_key.referenced_columns),
        format_referential_action(&foreign_key.on_delete),
        format_referential_action(&foreign_key.on_update)
    )
}

fn generate_alter_column(table: &str, column: &str, changes: &ColumnChanges) -> Vec<String> {
    let mut statements = Vec::new();

    if let Some(ref data_type) = changes.data_type {
        statements.push(format!(
            "ALTER TABLE {} ALTER COLUMN {} TYPE {};",
            quote_ident(table),
            quote_ident(column),
            format_pg_type(data_type)
        ));
    }

    if let Some(nullable) = changes.nullable {
        if nullable {
            statements.push(format!(
                "ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL;",
                quote_ident(table),
                quote_ident(column)
            ));
        } else {
            statements.push(format!(
                "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL;",
                quote_ident(table),
                quote_ident(column)
            ));
        }
    }

    if let Some(ref default) = changes.default {
        match default {
            Some(value) => {
                statements.push(format!(
                    "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {};",
                    quote_ident(table),
                    quote_ident(column),
                    value
                ));
            }
            None => {
                statements.push(format!(
                    "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT;",
                    quote_ident(table),
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
        PgType::CustomEnum(name) => quote_ident(name),
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

fn escape_string(value: &str) -> String {
    value.replace('\'', "''")
}

fn generate_create_policy(policy: &Policy) -> String {
    let mut sql = format!(
        "CREATE POLICY {} ON {}",
        quote_ident(&policy.name),
        quote_ident(&policy.table)
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
    let mut statements = Vec::new();

    if let Some(ref roles) = changes.roles {
        statements.push(format!(
            "ALTER POLICY {} ON {} TO {};",
            quote_ident(name),
            quote_ident(table),
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
            quote_ident(table),
            expr
        ))
    }

    if let Some(Some(expr)) = &changes.check_expr {
        statements.push(format!(
            "ALTER POLICY {} ON {} WITH CHECK ({});",
            quote_ident(name),
            quote_ident(table),
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
        quote_ident(&func.name),
        args,
        func.return_type,
        func.language,
        volatility,
        security,
        func.body
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
            values: vec!["admin".to_string(), "user".to_string(), "guest".to_string()],
        })];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(
            sql[0],
            "CREATE TYPE \"user_role\" AS ENUM ('admin', 'user', 'guest');"
        );
    }

    #[test]
    fn drop_enum_generates_valid_sql() {
        let ops = vec![MigrationOp::DropEnum("user_role".to_string())];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(sql[0], "DROP TYPE \"user_role\";");
    }

    #[test]
    fn add_column_generates_valid_sql() {
        let ops = vec![MigrationOp::AddColumn {
            table: "users".to_string(),
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
            "ALTER TABLE \"users\" ADD COLUMN \"email\" VARCHAR(255) NOT NULL;"
        );
    }

    #[test]
    fn drop_column_generates_valid_sql() {
        let ops = vec![MigrationOp::DropColumn {
            table: "users".to_string(),
            column: "email".to_string(),
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert_eq!(sql[0], "ALTER TABLE \"users\" DROP COLUMN \"email\";");
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
            columns,
            indexes: vec![],
            primary_key: Some(PrimaryKey {
                columns: vec!["id".to_string()],
            }),
            foreign_keys: vec![],
            comment: None,
            row_level_security: false,
            policies: vec![],
        })];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert!(sql[0].contains("CREATE TABLE \"users\""));
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
            table: "users".to_string(),
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
            "CREATE UNIQUE INDEX \"users_email_idx\" ON \"users\" (\"email\");"
        );
    }

    #[test]
    fn alter_column_type_generates_valid_sql() {
        let ops = vec![MigrationOp::AlterColumn {
            table: "users".to_string(),
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
            "ALTER TABLE \"users\" ALTER COLUMN \"name\" TYPE VARCHAR(100);"
        );
    }

    #[test]
    fn add_foreign_key_generates_valid_sql() {
        let ops = vec![MigrationOp::AddForeignKey {
            table: "posts".to_string(),
            foreign_key: ForeignKey {
                name: "posts_user_id_fkey".to_string(),
                columns: vec!["user_id".to_string()],
                referenced_table: "users".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: ReferentialAction::Cascade,
                on_update: ReferentialAction::NoAction,
            },
        }];

        let sql = generate_sql(&ops);
        assert_eq!(sql.len(), 1);
        assert!(sql[0].contains("ALTER TABLE \"posts\" ADD CONSTRAINT \"posts_user_id_fkey\""));
        assert!(sql[0].contains("FOREIGN KEY (\"user_id\")"));
        assert!(sql[0].contains("REFERENCES \"users\" (\"id\")"));
        assert!(sql[0].contains("ON DELETE CASCADE"));
        assert!(sql[0].contains("ON UPDATE NO ACTION"));
    }
}
