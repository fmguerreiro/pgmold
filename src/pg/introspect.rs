use crate::model::*;
use crate::pg::connection::PgConnection;
use crate::util::{Result, SchemaError};
use sqlx::Row;
use std::collections::BTreeMap;

pub async fn introspect_schema(connection: &PgConnection) -> Result<Schema> {
    let mut schema = Schema::new();

    schema.enums = introspect_enums(connection).await?;
    schema.tables = introspect_tables(connection).await?;
    schema.functions = introspect_functions(connection).await?;

    let table_names: Vec<String> = schema.tables.keys().cloned().collect();
    for table_name in table_names {
        let columns = introspect_columns(connection, &table_name).await?;
        let primary_key = introspect_primary_key(connection, &table_name).await?;
        let mut indexes = introspect_indexes(connection, &table_name).await?;
        let mut foreign_keys = introspect_foreign_keys(connection, &table_name).await?;

        indexes.sort();
        foreign_keys.sort();

        let row_level_security = introspect_rls_enabled(connection, &table_name).await?;
        let mut policies = introspect_policies(connection, &table_name).await?;
        policies.sort();

        if let Some(table) = schema.tables.get_mut(&table_name) {
            table.columns = columns;
            table.primary_key = primary_key;
            table.indexes = indexes;
            table.foreign_keys = foreign_keys;
            table.row_level_security = row_level_security;
            table.policies = policies;
        }
    }

    Ok(schema)
}

async fn introspect_enums(connection: &PgConnection) -> Result<BTreeMap<String, EnumType>> {
    let rows = sqlx::query(
        r#"
        SELECT t.typname, array_agg(e.enumlabel ORDER BY e.enumsortorder) as labels
        FROM pg_type t
        JOIN pg_enum e ON t.oid = e.enumtypid
        JOIN pg_namespace n ON t.typnamespace = n.oid
        WHERE n.nspname = 'public'
        GROUP BY t.typname
        "#,
    )
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch enums: {}", e)))?;

    let mut enums = BTreeMap::new();
    for row in rows {
        let name: String = row.get("typname");
        let labels: Vec<String> = row.get("labels");
        enums.insert(
            name.clone(),
            EnumType {
                name,
                values: labels,
            },
        );
    }

    Ok(enums)
}

async fn introspect_tables(connection: &PgConnection) -> Result<BTreeMap<String, Table>> {
    let rows = sqlx::query(
        r#"
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        "#,
    )
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch tables: {}", e)))?;

    let mut tables = BTreeMap::new();
    for row in rows {
        let name: String = row.get("table_name");
        tables.insert(
            name.clone(),
            Table {
                name,
                columns: BTreeMap::new(),
                indexes: Vec::new(),
                primary_key: None,
                foreign_keys: Vec::new(),
                comment: None,
                row_level_security: false,
                policies: Vec::new(),
            },
        );
    }

    Ok(tables)
}

async fn introspect_columns(
    connection: &PgConnection,
    table_name: &str,
) -> Result<BTreeMap<String, Column>> {
    let rows = sqlx::query(
        r#"
        SELECT column_name, data_type, character_maximum_length,
               is_nullable, column_default, udt_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = $1
        ORDER BY ordinal_position
        "#,
    )
    .bind(table_name)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch columns: {}", e)))?;

    let mut columns = BTreeMap::new();
    for row in rows {
        let name: String = row.get("column_name");
        let data_type: String = row.get("data_type");
        let char_max_length: Option<i32> = row.get("character_maximum_length");
        let is_nullable: String = row.get("is_nullable");
        let column_default: Option<String> = row.get("column_default");
        let udt_name: String = row.get("udt_name");

        let pg_type = map_pg_type(&data_type, char_max_length, &udt_name);

        columns.insert(
            name.clone(),
            Column {
                name,
                data_type: pg_type,
                nullable: is_nullable == "YES",
                default: column_default,
                comment: None,
            },
        );
    }

    Ok(columns)
}

fn map_pg_type(data_type: &str, char_max_length: Option<i32>, udt_name: &str) -> PgType {
    match data_type {
        "integer" => PgType::Integer,
        "bigint" => PgType::BigInt,
        "smallint" => PgType::SmallInt,
        "character varying" => PgType::Varchar(char_max_length.map(|l| l as u32)),
        "text" => PgType::Text,
        "boolean" => PgType::Boolean,
        "timestamp with time zone" => PgType::TimestampTz,
        "timestamp without time zone" => PgType::Timestamp,
        "date" => PgType::Date,
        "uuid" => PgType::Uuid,
        "json" => PgType::Json,
        "jsonb" => PgType::Jsonb,
        "USER-DEFINED" => PgType::CustomEnum(udt_name.to_string()),
        _ => PgType::Text,
    }
}

async fn introspect_primary_key(
    connection: &PgConnection,
    table_name: &str,
) -> Result<Option<PrimaryKey>> {
    let row = sqlx::query(
        r#"
        SELECT array_agg(a.attname ORDER BY array_position(i.indkey, a.attnum)) as columns
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indrelid
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = $1 AND n.nspname = 'public' AND i.indisprimary
        GROUP BY i.indexrelid
        "#,
    )
    .bind(table_name)
    .fetch_optional(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch primary key: {}", e)))?;

    Ok(row.map(|r| {
        let columns: Vec<String> = r.get("columns");
        PrimaryKey { columns }
    }))
}

async fn introspect_indexes(connection: &PgConnection, table_name: &str) -> Result<Vec<Index>> {
    let rows = sqlx::query(
        r#"
        SELECT i.relname as index_name, ix.indisunique, am.amname,
               array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) as columns
        FROM pg_index ix
        JOIN pg_class t ON t.oid = ix.indrelid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_am am ON am.oid = i.relam
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE t.relname = $1 AND n.nspname = 'public' AND NOT ix.indisprimary
        GROUP BY i.relname, ix.indisunique, am.amname
        "#,
    )
    .bind(table_name)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch indexes: {}", e)))?;

    let mut indexes = Vec::new();
    for row in rows {
        let name: String = row.get("index_name");
        let unique: bool = row.get("indisunique");
        let am_name: String = row.get("amname");
        let columns: Vec<String> = row.get("columns");

        let index_type = match am_name.as_str() {
            "btree" => IndexType::BTree,
            "hash" => IndexType::Hash,
            "gin" => IndexType::Gin,
            "gist" => IndexType::Gist,
            _ => IndexType::BTree,
        };

        indexes.push(Index {
            name,
            columns,
            unique,
            index_type,
        });
    }

    Ok(indexes)
}

async fn introspect_foreign_keys(
    connection: &PgConnection,
    table_name: &str,
) -> Result<Vec<ForeignKey>> {
    let rows = sqlx::query(
        r#"
        SELECT
            con.conname as name,
            ref_class.relname as referenced_table,
            array_agg(att.attname ORDER BY u.attposition) as columns,
            array_agg(ref_att.attname ORDER BY u.attposition) as referenced_columns,
            con.confdeltype,
            con.confupdtype
        FROM pg_constraint con
        JOIN pg_class class ON con.conrelid = class.oid
        JOIN pg_class ref_class ON con.confrelid = ref_class.oid
        JOIN pg_namespace n ON n.oid = class.relnamespace
        CROSS JOIN LATERAL unnest(con.conkey, con.confkey) WITH ORDINALITY AS u(attnum, ref_attnum, attposition)
        JOIN pg_attribute att ON att.attrelid = class.oid AND att.attnum = u.attnum
        JOIN pg_attribute ref_att ON ref_att.attrelid = ref_class.oid AND ref_att.attnum = u.ref_attnum
        WHERE class.relname = $1 AND n.nspname = 'public' AND con.contype = 'f'
        GROUP BY con.conname, ref_class.relname, con.confdeltype, con.confupdtype
        "#,
    )
    .bind(table_name)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch foreign keys: {}", e)))?;

    let mut foreign_keys = Vec::new();
    for row in rows {
        let name: String = row.get("name");
        let referenced_table: String = row.get("referenced_table");
        let columns: Vec<String> = row.get("columns");
        let referenced_columns: Vec<String> = row.get("referenced_columns");
        let confdeltype: i8 = row.get::<i8, _>("confdeltype");
        let confupdtype: i8 = row.get::<i8, _>("confupdtype");

        foreign_keys.push(ForeignKey {
            name,
            columns,
            referenced_table,
            referenced_columns,
            on_delete: map_referential_action(confdeltype as u8 as char),
            on_update: map_referential_action(confupdtype as u8 as char),
        });
    }

    Ok(foreign_keys)
}

fn map_referential_action(action: char) -> ReferentialAction {
    match action {
        'a' => ReferentialAction::NoAction,
        'r' => ReferentialAction::Restrict,
        'c' => ReferentialAction::Cascade,
        'n' => ReferentialAction::SetNull,
        'd' => ReferentialAction::SetDefault,
        _ => ReferentialAction::NoAction,
    }
}

async fn introspect_rls_enabled(connection: &PgConnection, table_name: &str) -> Result<bool> {
    let row = sqlx::query(
        r#"
        SELECT c.relrowsecurity
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = $1 AND n.nspname = 'public'
        "#,
    )
    .bind(table_name)
    .fetch_optional(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch RLS status: {}", e)))?;

    Ok(row.map(|r| r.get::<bool, _>("relrowsecurity")).unwrap_or(false))
}

async fn introspect_policies(connection: &PgConnection, table_name: &str) -> Result<Vec<Policy>> {
    let rows = sqlx::query(
        r#"
        SELECT
            pol.polname as name,
            pol.polcmd as command,
            COALESCE(
                ARRAY(SELECT rolname FROM pg_roles WHERE oid = ANY(pol.polroles)),
                ARRAY[]::text[]
            ) as roles,
            pg_get_expr(pol.polqual, pol.polrelid) as using_expr,
            pg_get_expr(pol.polwithcheck, pol.polrelid) as check_expr
        FROM pg_policy pol
        JOIN pg_class c ON pol.polrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.relname = $1 AND n.nspname = 'public'
        "#,
    )
    .bind(table_name)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch policies: {}", e)))?;

    let mut policies = Vec::new();
    for row in rows {
        let name: String = row.get("name");
        let command: i8 = row.get::<i8, _>("command");
        let roles: Vec<String> = row.get("roles");
        let using_expr: Option<String> = row.get("using_expr");
        let check_expr: Option<String> = row.get("check_expr");

        policies.push(Policy {
            name,
            table: table_name.to_string(),
            command: map_policy_command(command as u8 as char),
            roles,
            using_expr,
            check_expr,
        });
    }

    Ok(policies)
}

fn map_policy_command(cmd: char) -> PolicyCommand {
    match cmd {
        '*' => PolicyCommand::All,
        'r' => PolicyCommand::Select,
        'a' => PolicyCommand::Insert,
        'w' => PolicyCommand::Update,
        'd' => PolicyCommand::Delete,
        _ => PolicyCommand::All,
    }
}

async fn introspect_functions(connection: &PgConnection) -> Result<BTreeMap<String, Function>> {
    let rows = sqlx::query(
        r#"
        SELECT
            p.proname as name,
            n.nspname as schema,
            pg_get_function_arguments(p.oid) as arguments,
            pg_get_function_result(p.oid) as return_type,
            l.lanname as language,
            p.prosrc as body,
            p.provolatile as volatility,
            p.prosecdef as security_definer
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        JOIN pg_language l ON p.prolang = l.oid
        WHERE n.nspname = 'public'
          AND p.prokind = 'f'
        "#,
    )
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch functions: {}", e)))?;

    let mut functions = BTreeMap::new();
    for row in rows {
        let name: String = row.get("name");
        let schema: String = row.get("schema");
        let arguments_str: String = row.get("arguments");
        let return_type: String = row.get("return_type");
        let language: String = row.get("language");
        let body: String = row.get("body");
        let volatility_char: i8 = row.get::<i8, _>("volatility");
        let security_definer: bool = row.get("security_definer");

        let volatility = match volatility_char as u8 as char {
            'i' => Volatility::Immutable,
            's' => Volatility::Stable,
            _ => Volatility::Volatile,
        };

        let security = if security_definer {
            SecurityType::Definer
        } else {
            SecurityType::Invoker
        };

        let arguments = parse_function_arguments(&arguments_str);

        let func = Function {
            name: name.clone(),
            schema,
            arguments,
            return_type,
            language,
            body,
            volatility,
            security,
        };

        functions.insert(func.signature(), func);
    }

    Ok(functions)
}

fn parse_function_arguments(args_str: &str) -> Vec<FunctionArg> {
    if args_str.is_empty() {
        return Vec::new();
    }

    args_str
        .split(',')
        .map(|arg| {
            let arg = arg.trim();
            let parts: Vec<&str> = arg.splitn(2, ' ').collect();
            if parts.len() == 2 {
                FunctionArg {
                    name: Some(parts[0].to_string()),
                    data_type: parts[1].to_string(),
                    mode: ArgMode::In,
                    default: None,
                }
            } else {
                FunctionArg {
                    name: None,
                    data_type: arg.to_string(),
                    mode: ArgMode::In,
                    default: None,
                }
            }
        })
        .collect()
}
