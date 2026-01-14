use crate::model::*;
use crate::pg::connection::PgConnection;
use crate::pg::sqlgen::strip_ident_quotes;
use crate::util::{normalize_sql_whitespace, Result, SchemaError};
use sqlx::Row;
use std::collections::BTreeMap;

pub async fn introspect_schema(
    connection: &PgConnection,
    target_schemas: &[String],
    include_extension_objects: bool,
) -> Result<Schema> {
    let mut schema = Schema::new();

    schema.schemas = introspect_schemas(connection, target_schemas).await?;
    schema.extensions = introspect_extensions(connection).await?;
    schema.enums = introspect_enums(connection, target_schemas, include_extension_objects).await?;
    schema.domains =
        introspect_domains(connection, target_schemas, include_extension_objects).await?;
    schema.tables =
        introspect_tables(connection, target_schemas, include_extension_objects).await?;
    schema.functions =
        introspect_functions(connection, target_schemas, include_extension_objects).await?;
    schema.views = introspect_views(connection, target_schemas, include_extension_objects).await?;
    schema.triggers =
        introspect_triggers(connection, target_schemas, include_extension_objects).await?;
    schema.sequences =
        introspect_sequences(connection, target_schemas, include_extension_objects).await?;

    // Introspect partition keys and merge into tables
    let partition_keys = introspect_partition_keys(connection, target_schemas).await?;
    for (qualified_name, partition_key) in partition_keys {
        if let Some(table) = schema.tables.get_mut(&qualified_name) {
            table.partition_by = Some(partition_key);
        }
    }

    // Introspect partitions (child tables)
    schema.partitions = introspect_partitions(connection, target_schemas).await?;

    let table_keys: Vec<(String, String)> = schema
        .tables
        .values()
        .map(|t| (t.schema.clone(), t.name.clone()))
        .collect();
    for (table_schema, table_name) in table_keys {
        let columns =
            introspect_columns(connection, target_schemas, &table_schema, &table_name).await?;
        let primary_key = introspect_primary_key(connection, &table_schema, &table_name).await?;
        let mut indexes = introspect_indexes(connection, &table_schema, &table_name).await?;
        let mut foreign_keys =
            introspect_foreign_keys(connection, &table_schema, &table_name).await?;
        let mut check_constraints =
            introspect_check_constraints(connection, &table_schema, &table_name).await?;

        indexes.sort();
        foreign_keys.sort();
        check_constraints.sort();

        let row_level_security =
            introspect_rls_enabled(connection, &table_schema, &table_name).await?;
        let mut policies = introspect_policies(connection, &table_schema, &table_name).await?;
        policies.sort();

        let qualified_name = format!("{table_schema}.{table_name}");
        if let Some(table) = schema.tables.get_mut(&qualified_name) {
            table.columns = columns;
            table.primary_key = primary_key;
            table.indexes = indexes;
            table.foreign_keys = foreign_keys;
            table.check_constraints = check_constraints;
            table.row_level_security = row_level_security;
            table.policies = policies;
        }
    }

    Ok(schema)
}

async fn introspect_schemas(
    connection: &PgConnection,
    target_schemas: &[String],
) -> Result<BTreeMap<String, PgSchema>> {
    let rows = sqlx::query(
        r#"
        SELECT nspname as name
        FROM pg_namespace
        WHERE nspname NOT LIKE 'pg_%'
          AND nspname != 'information_schema'
        "#,
    )
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch schemas: {e}")))?;

    let mut schemas = BTreeMap::new();
    for row in rows {
        let name: String = row.get("name");
        // Always skip 'public' schema - it's a default schema that always exists in PostgreSQL.
        // Users who want to manage 'public' must include CREATE SCHEMA "public" in their SQL.
        if name == "public" {
            continue;
        }
        // Only include schemas that match target_schemas filter (or all if empty)
        if target_schemas.is_empty() || target_schemas.contains(&name) {
            schemas.insert(name.clone(), PgSchema { name });
        }
    }

    Ok(schemas)
}

async fn introspect_extensions(connection: &PgConnection) -> Result<BTreeMap<String, Extension>> {
    let rows = sqlx::query(
        r#"
        SELECT
            e.extname as name,
            e.extversion as version,
            n.nspname as schema
        FROM pg_extension e
        JOIN pg_namespace n ON e.extnamespace = n.oid
        WHERE e.extname != 'plpgsql'
        "#,
    )
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch extensions: {e}")))?;

    let mut extensions = BTreeMap::new();
    for row in rows {
        let name: String = row.get("name");
        let version: Option<String> = row.get("version");
        let schema: Option<String> = row.get::<Option<String>, _>("schema");

        extensions.insert(
            name.clone(),
            Extension {
                name,
                version,
                schema,
            },
        );
    }

    Ok(extensions)
}

async fn introspect_enums(
    connection: &PgConnection,
    target_schemas: &[String],
    include_extension_objects: bool,
) -> Result<BTreeMap<String, EnumType>> {
    let rows = sqlx::query(
        r#"
        SELECT n.nspname, t.typname, array_agg(e.enumlabel ORDER BY e.enumsortorder) as labels
        FROM pg_type t
        JOIN pg_enum e ON t.oid = e.enumtypid
        JOIN pg_namespace n ON t.typnamespace = n.oid
        WHERE n.nspname = ANY($1::text[])
          AND ($2::boolean OR NOT EXISTS (
              SELECT 1 FROM pg_depend d
              WHERE d.objid = t.oid
              AND d.deptype = 'e'
          ))
        GROUP BY n.nspname, t.typname
        "#,
    )
    .bind(target_schemas)
    .bind(include_extension_objects)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch enums: {e}")))?;

    let mut enums = BTreeMap::new();
    for row in rows {
        let schema: String = row.get("nspname");
        let name: String = row.get("typname");
        let labels: Vec<String> = row.get("labels");
        let enum_type = EnumType {
            name: name.clone(),
            schema: schema.clone(),
            values: labels,
        };
        let qualified_name = format!("{schema}.{name}");
        enums.insert(qualified_name, enum_type);
    }

    Ok(enums)
}

async fn introspect_domains(
    connection: &PgConnection,
    target_schemas: &[String],
    include_extension_objects: bool,
) -> Result<BTreeMap<String, Domain>> {
    let rows = sqlx::query(
        r#"
        SELECT
            n.nspname AS schema_name,
            t.typname AS domain_name,
            bt.typname AS base_type,
            t.typnotnull AS not_null,
            pg_get_expr(t.typdefaultbin, 0) AS default_expr
        FROM pg_type t
        JOIN pg_namespace n ON t.typnamespace = n.oid
        JOIN pg_type bt ON t.typbasetype = bt.oid
        WHERE t.typtype = 'd'
            AND n.nspname = ANY($1::text[])
            AND ($2::boolean OR NOT EXISTS (
                SELECT 1 FROM pg_depend d
                WHERE d.objid = t.oid
                AND d.deptype = 'e'
            ))
        "#,
    )
    .bind(target_schemas)
    .bind(include_extension_objects)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch domains: {e}")))?;

    let mut domains = BTreeMap::new();
    for row in rows {
        let schema: String = row.get("schema_name");
        let name: String = row.get("domain_name");
        let base_type: String = row.get("base_type");
        let not_null: bool = row.get("not_null");
        let default_expr: Option<String> = row
            .get::<Option<String>, &str>("default_expr")
            .filter(|s| !s.is_empty());

        let check_constraints = introspect_domain_constraints(connection, &schema, &name).await?;

        let data_type = match base_type.as_str() {
            "integer" | "int4" => PgType::Integer,
            "bigint" | "int8" => PgType::BigInt,
            "smallint" | "int2" => PgType::SmallInt,
            "real" | "float4" => PgType::Real,
            "double precision" | "float8" => PgType::DoublePrecision,
            "text" => PgType::Text,
            "boolean" | "bool" => PgType::Boolean,
            "timestamp" => PgType::Timestamp,
            "timestamp with time zone" | "timestamptz" => PgType::TimestampTz,
            "date" => PgType::Date,
            "uuid" => PgType::Uuid,
            "json" => PgType::Json,
            "jsonb" => PgType::Jsonb,
            "character varying" | "varchar" => PgType::Varchar(None),
            _ => {
                let qualified = format!("public.{base_type}");
                if base_type.contains('.') {
                    PgType::Named(base_type)
                } else {
                    PgType::CustomEnum(qualified)
                }
            }
        };

        let domain = Domain {
            schema: schema.clone(),
            name: name.clone(),
            data_type,
            default: default_expr,
            not_null,
            collation: None,
            check_constraints,
        };
        let qualified_name = format!("{schema}.{name}");
        domains.insert(qualified_name, domain);
    }

    Ok(domains)
}

async fn introspect_domain_constraints(
    connection: &PgConnection,
    schema: &str,
    domain_name: &str,
) -> Result<Vec<DomainConstraint>> {
    let rows = sqlx::query(
        r#"
        SELECT
            con.conname AS constraint_name,
            pg_get_constraintdef(con.oid) AS constraint_def
        FROM pg_constraint con
        JOIN pg_type t ON con.contypid = t.oid
        JOIN pg_namespace n ON t.typnamespace = n.oid
        WHERE con.contype = 'c'
            AND n.nspname = $1
            AND t.typname = $2
        "#,
    )
    .bind(schema)
    .bind(domain_name)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch domain constraints: {e}")))?;

    let mut constraints = Vec::new();
    for row in rows {
        let name: String = row.get("constraint_name");
        let def: String = row.get("constraint_def");
        let mut expression = def
            .strip_prefix("CHECK ")
            .unwrap_or(&def)
            .trim_start_matches('(')
            .trim_end_matches(')')
            .trim()
            .to_string();

        if let Some(cast_pos) = expression.find("::") {
            expression = expression[..cast_pos].trim_end().to_string();
        }

        let constraint_name = if name == format!("{domain_name}_check") {
            None
        } else {
            Some(name)
        };

        constraints.push(DomainConstraint {
            name: constraint_name,
            expression,
        });
    }

    Ok(constraints)
}

async fn introspect_tables(
    connection: &PgConnection,
    target_schemas: &[String],
    include_extension_objects: bool,
) -> Result<BTreeMap<String, Table>> {
    let rows = sqlx::query(
        r#"
        SELECT n.nspname AS table_schema, c.relname AS table_name
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = ANY($1::text[])
          AND c.relkind IN ('r', 'p')
          AND c.relispartition = false
          AND ($2::boolean OR NOT EXISTS (
              SELECT 1 FROM pg_depend d
              WHERE d.objid = c.oid
              AND d.deptype = 'e'
          ))
        "#,
    )
    .bind(target_schemas)
    .bind(include_extension_objects)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch tables: {e}")))?;

    let mut tables = BTreeMap::new();
    for row in rows {
        let schema: String = row.get("table_schema");
        let name: String = row.get("table_name");
        let table = Table {
            name: name.clone(),
            schema: schema.clone(),
            columns: BTreeMap::new(),
            indexes: Vec::new(),
            primary_key: None,
            foreign_keys: Vec::new(),
            check_constraints: Vec::new(),
            comment: None,
            row_level_security: false,
            policies: Vec::new(),
            partition_by: None,
        };
        let qualified_name = format!("{schema}.{name}");
        tables.insert(qualified_name, table);
    }

    Ok(tables)
}

/// Introspect partition keys for partitioned tables.
/// Returns a map of qualified_name -> PartitionKey.
async fn introspect_partition_keys(
    connection: &PgConnection,
    target_schemas: &[String],
) -> Result<BTreeMap<String, PartitionKey>> {
    let rows = sqlx::query(
        r#"
        SELECT
            n.nspname as schema,
            c.relname as name,
            pt.partstrat::text as strategy,
            pg_get_partkeydef(c.oid) as partition_key_def
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_partitioned_table pt ON c.oid = pt.partrelid
        WHERE n.nspname = ANY($1::text[])
        "#,
    )
    .bind(target_schemas)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch partition keys: {e}")))?;

    let mut partition_keys = BTreeMap::new();
    for row in rows {
        let schema: String = row.get("schema");
        let name: String = row.get("name");
        let strategy_char: String = row.get("strategy");
        let key_def: String = row.get("partition_key_def");

        let strategy = match strategy_char.as_str() {
            "r" => PartitionStrategy::Range,
            "l" => PartitionStrategy::List,
            "h" => PartitionStrategy::Hash,
            _ => continue,
        };

        // key_def is like "RANGE (logdate)" or "LIST (status)"
        // Extract the columns by parsing the parentheses
        let columns = parse_partition_key_columns(&key_def);

        let partition_key = PartitionKey {
            strategy,
            columns,
            expressions: Vec::new(),
        };

        let qualified_name = format!("{schema}.{name}");
        partition_keys.insert(qualified_name, partition_key);
    }

    Ok(partition_keys)
}

/// Introspect partitions (child tables) for partitioned tables.
/// Returns a map of qualified_name -> Partition.
async fn introspect_partitions(
    connection: &PgConnection,
    target_schemas: &[String],
) -> Result<BTreeMap<String, Partition>> {
    let rows = sqlx::query(
        r#"
        SELECT
            n.nspname as schema,
            c.relname as name,
            pn.nspname as parent_schema,
            pc.relname as parent_name,
            pg_get_expr(c.relpartbound, c.oid) as partition_bound
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_inherits i ON c.oid = i.inhrelid
        JOIN pg_class pc ON pc.oid = i.inhparent
        JOIN pg_namespace pn ON pc.relnamespace = pn.oid
        WHERE c.relispartition = true
          AND n.nspname = ANY($1::text[])
        "#,
    )
    .bind(target_schemas)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch partitions: {e}")))?;

    let mut partitions = BTreeMap::new();
    for row in rows {
        let schema: String = row.get("schema");
        let name: String = row.get("name");
        let parent_schema: String = row.get("parent_schema");
        let parent_name: String = row.get("parent_name");
        let bound_expr: String = row.get("partition_bound");

        let bound = parse_partition_bound(&bound_expr);

        let partition = Partition {
            schema: schema.clone(),
            name: name.clone(),
            parent_schema,
            parent_name,
            bound,
            indexes: Vec::new(),
            check_constraints: Vec::new(),
        };

        let qualified_name = format!("{schema}.{name}");
        partitions.insert(qualified_name, partition);
    }

    Ok(partitions)
}

/// Parse a partition bound expression like "FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')"
fn parse_partition_bound(expr: &str) -> PartitionBound {
    let expr_upper = expr.to_uppercase();

    if expr_upper.contains("DEFAULT") {
        return PartitionBound::Default;
    }

    if expr_upper.contains("FROM") && expr_upper.contains("TO") {
        // RANGE: FOR VALUES FROM (...) TO (...)
        if let (Some(from_start), Some(to_start)) = (expr.find("FROM"), expr.find("TO")) {
            let from_part = &expr[from_start + 4..to_start].trim();
            let to_part = &expr[to_start + 2..].trim();

            let from_values = extract_paren_values(from_part);
            let to_values = extract_paren_values(to_part);

            return PartitionBound::Range {
                from: from_values,
                to: to_values,
            };
        }
    }

    if expr_upper.contains("IN") {
        // LIST: FOR VALUES IN (...)
        if let Some(in_pos) = expr.find("IN") {
            let values_part = &expr[in_pos + 2..].trim();
            let values = extract_paren_values(values_part);
            return PartitionBound::List { values };
        }
    }

    if expr_upper.contains("MODULUS") && expr_upper.contains("REMAINDER") {
        // HASH: FOR VALUES WITH (MODULUS n, REMAINDER r)
        if let Some(with_pos) = expr.find("WITH") {
            let params_part = &expr[with_pos + 4..].trim();
            let params = extract_paren_values(params_part);
            let mut modulus = 0u32;
            let mut remainder = 0u32;

            for param in params {
                let param_upper = param.to_uppercase();
                if param_upper.contains("MODULUS") {
                    if let Some(val) = param.split_whitespace().last() {
                        modulus = val.parse().unwrap_or(0);
                    }
                } else if param_upper.contains("REMAINDER") {
                    if let Some(val) = param.split_whitespace().last() {
                        remainder = val.parse().unwrap_or(0);
                    }
                }
            }

            return PartitionBound::Hash { modulus, remainder };
        }
    }

    // Fallback
    PartitionBound::Default
}

/// Extract values from a parenthesized list like "(val1, val2)"
fn extract_paren_values(s: &str) -> Vec<String> {
    if let Some(start) = s.find('(') {
        if let Some(end) = s.rfind(')') {
            let inner = &s[start + 1..end];
            return inner.split(',').map(|v| v.trim().to_string()).collect();
        }
    }
    Vec::new()
}

/// Parse column names from a partition key definition like "RANGE (col1, col2)"
fn parse_partition_key_columns(key_def: &str) -> Vec<String> {
    // Find content between parentheses
    if let Some(start) = key_def.find('(') {
        if let Some(end) = key_def.rfind(')') {
            let columns_str = &key_def[start + 1..end];
            return columns_str
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
        }
    }
    Vec::new()
}

async fn introspect_columns(
    connection: &PgConnection,
    _target_schemas: &[String],
    table_schema: &str,
    table_name: &str,
) -> Result<BTreeMap<String, Column>> {
    let rows = sqlx::query(
        r#"
        SELECT
            c.column_name,
            c.data_type,
            c.character_maximum_length,
            c.is_nullable,
            c.column_default,
            c.udt_name,
            c.udt_schema,
            a.atttypmod
        FROM information_schema.columns c
        JOIN pg_catalog.pg_class t ON t.relname = c.table_name
        JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace AND n.nspname = c.table_schema
        JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid AND a.attname = c.column_name
        WHERE c.table_schema = $1 AND c.table_name = $2
        ORDER BY c.ordinal_position
        "#,
    )
    .bind(table_schema)
    .bind(table_name)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch columns: {e}")))?;

    let mut columns = BTreeMap::new();
    for row in rows {
        let name: String = row.get("column_name");
        let data_type: String = row.get("data_type");
        let char_max_length: Option<i32> = row.get("character_maximum_length");
        let is_nullable: String = row.get("is_nullable");
        let column_default: Option<String> = row.get("column_default");
        let udt_name: String = row.get("udt_name");
        let udt_schema: String = row.get("udt_schema");
        let atttypmod: i32 = row.get("atttypmod");

        let pg_type = map_pg_type(
            &data_type,
            char_max_length,
            &udt_schema,
            &udt_name,
            atttypmod,
        );

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

fn map_pg_type(
    data_type: &str,
    char_max_length: Option<i32>,
    udt_schema: &str,
    udt_name: &str,
    atttypmod: i32,
) -> PgType {
    match data_type {
        "integer" => PgType::Integer,
        "bigint" => PgType::BigInt,
        "smallint" => PgType::SmallInt,
        "real" => PgType::Real,
        "double precision" => PgType::DoublePrecision,
        "character varying" => PgType::Varchar(char_max_length.map(|l| l as u32)),
        "text" => PgType::Text,
        "boolean" => PgType::Boolean,
        "timestamp with time zone" => PgType::TimestampTz,
        "timestamp without time zone" => PgType::Timestamp,
        "date" => PgType::Date,
        "uuid" => PgType::Uuid,
        "json" => PgType::Json,
        "jsonb" => PgType::Jsonb,
        "USER-DEFINED" => {
            if udt_name == "vector" {
                // pgvector stores dimension directly in atttypmod
                // -1 means no dimension constraint (e.g., vector vs vector(1536))
                let dimension = if atttypmod != -1 {
                    Some(atttypmod as u32)
                } else {
                    None
                };
                PgType::Vector(dimension)
            } else {
                PgType::CustomEnum(format!("{udt_schema}.{udt_name}"))
            }
        }
        _ => PgType::Text,
    }
}

async fn introspect_primary_key(
    connection: &PgConnection,
    table_schema: &str,
    table_name: &str,
) -> Result<Option<PrimaryKey>> {
    let row = sqlx::query(
        r#"
        SELECT array_agg(a.attname ORDER BY array_position(i.indkey, a.attnum)) as columns
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indrelid
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = $1 AND n.nspname = $2 AND i.indisprimary
        GROUP BY i.indexrelid
        "#,
    )
    .bind(table_name)
    .bind(table_schema)
    .fetch_optional(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch primary key: {e}")))?;

    Ok(row.map(|r| {
        let columns: Vec<String> = r.get("columns");
        PrimaryKey { columns }
    }))
}

async fn introspect_indexes(
    connection: &PgConnection,
    table_schema: &str,
    table_name: &str,
) -> Result<Vec<Index>> {
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
        WHERE t.relname = $1 AND n.nspname = $2 AND NOT ix.indisprimary
        GROUP BY i.relname, ix.indisunique, am.amname
        "#,
    )
    .bind(table_name)
    .bind(table_schema)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch indexes: {e}")))?;

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
    table_schema: &str,
    table_name: &str,
) -> Result<Vec<ForeignKey>> {
    let rows = sqlx::query(
        r#"
        SELECT
            con.conname as name,
            ref_class.relname as referenced_table,
            ref_n.nspname as referenced_schema,
            array_agg(att.attname ORDER BY u.attposition) as columns,
            array_agg(ref_att.attname ORDER BY u.attposition) as referenced_columns,
            con.confdeltype,
            con.confupdtype
        FROM pg_constraint con
        JOIN pg_class class ON con.conrelid = class.oid
        JOIN pg_class ref_class ON con.confrelid = ref_class.oid
        JOIN pg_namespace n ON n.oid = class.relnamespace
        JOIN pg_namespace ref_n ON ref_n.oid = ref_class.relnamespace
        CROSS JOIN LATERAL unnest(con.conkey, con.confkey) WITH ORDINALITY AS u(attnum, ref_attnum, attposition)
        JOIN pg_attribute att ON att.attrelid = class.oid AND att.attnum = u.attnum
        JOIN pg_attribute ref_att ON ref_att.attrelid = ref_class.oid AND ref_att.attnum = u.ref_attnum
        WHERE class.relname = $1 AND n.nspname = $2 AND con.contype = 'f'
        GROUP BY con.conname, ref_class.relname, ref_n.nspname, con.confdeltype, con.confupdtype
        "#,
    )
    .bind(table_name)
    .bind(table_schema)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch foreign keys: {e}")))?;

    let mut foreign_keys = Vec::new();
    for row in rows {
        let name: String = row.get("name");
        let referenced_table: String = row.get("referenced_table");
        let referenced_schema: String = row.get("referenced_schema");
        let columns: Vec<String> = row.get("columns");
        let referenced_columns: Vec<String> = row.get("referenced_columns");
        let confdeltype: i8 = row.get::<i8, _>("confdeltype");
        let confupdtype: i8 = row.get::<i8, _>("confupdtype");

        foreign_keys.push(ForeignKey {
            name,
            columns,
            referenced_table,
            referenced_schema,
            referenced_columns,
            on_delete: map_referential_action(confdeltype as u8 as char),
            on_update: map_referential_action(confupdtype as u8 as char),
        });
    }

    Ok(foreign_keys)
}

async fn introspect_check_constraints(
    connection: &PgConnection,
    table_schema: &str,
    table_name: &str,
) -> Result<Vec<CheckConstraint>> {
    let rows = sqlx::query(
        r#"
        SELECT
            con.conname as name,
            pg_get_constraintdef(con.oid) as definition
        FROM pg_constraint con
        JOIN pg_class class ON con.conrelid = class.oid
        JOIN pg_namespace n ON n.oid = class.relnamespace
        WHERE class.relname = $1 AND n.nspname = $2 AND con.contype = 'c'
        "#,
    )
    .bind(table_name)
    .bind(table_schema)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch check constraints: {e}")))?;

    let mut check_constraints = Vec::new();
    for row in rows {
        let name: String = row.get("name");
        let definition: String = row.get("definition");

        // pg_get_constraintdef returns "CHECK ((expression))" - extract the inner expression
        let expression = definition
            .strip_prefix("CHECK (")
            .and_then(|s| s.strip_suffix(")"))
            .map(|s| s.to_string())
            .unwrap_or(definition);

        check_constraints.push(CheckConstraint { name, expression });
    }

    Ok(check_constraints)
}

fn map_referential_action(action: char) -> ReferentialAction {
    match action {
        'a' => ReferentialAction::NoAction,
        'r' => ReferentialAction::Restrict,
        'c' => ReferentialAction::Cascade,
        'n' => ReferentialAction::SetNull,
        'd' => ReferentialAction::SetDefault,
        _ => panic!("Unknown referential action code from PostgreSQL: '{action}'"),
    }
}

async fn introspect_rls_enabled(
    connection: &PgConnection,
    table_schema: &str,
    table_name: &str,
) -> Result<bool> {
    let row = sqlx::query(
        r#"
        SELECT c.relrowsecurity
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = $1 AND n.nspname = $2
        "#,
    )
    .bind(table_name)
    .bind(table_schema)
    .fetch_optional(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch RLS status: {e}")))?;

    let row = row.ok_or_else(|| {
        SchemaError::DatabaseError(format!(
            "Table {table_schema}.{table_name} not found in pg_class while checking RLS"
        ))
    })?;

    Ok(row.get::<bool, _>("relrowsecurity"))
}

async fn introspect_policies(
    connection: &PgConnection,
    table_schema: &str,
    table_name: &str,
) -> Result<Vec<Policy>> {
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
        WHERE c.relname = $1 AND n.nspname = $2
        "#,
    )
    .bind(table_name)
    .bind(table_schema)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch policies: {e}")))?;

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
            table_schema: table_schema.to_string(),
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

async fn introspect_functions(
    connection: &PgConnection,
    target_schemas: &[String],
    include_extension_objects: bool,
) -> Result<BTreeMap<String, Function>> {
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
            p.prosecdef as security_definer,
            p.proconfig as config_params,
            r.rolname as owner
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        JOIN pg_language l ON p.prolang = l.oid
        JOIN pg_roles r ON p.proowner = r.oid
        WHERE n.nspname = ANY($1::text[])
          AND p.prokind = 'f'
          AND ($2::boolean OR NOT EXISTS (
              SELECT 1 FROM pg_depend d
              WHERE d.objid = p.oid
              AND d.deptype = 'e'
          ))
        "#,
    )
    .bind(target_schemas)
    .bind(include_extension_objects)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch functions: {e}")))?;

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

        let config_params_raw: Option<Vec<String>> = row.get("config_params");
        let config_params: Vec<(String, String)> = config_params_raw
            .unwrap_or_default()
            .into_iter()
            .map(|param| {
                let parts: Vec<&str> = param.splitn(2, '=').collect();
                if parts.len() == 2 {
                    Ok((parts[0].to_string(), parts[1].to_string()))
                } else {
                    Err(SchemaError::DatabaseError(format!(
                        "Malformed config parameter in function {}.{}: '{}'",
                        schema, name, param
                    )))
                }
            })
            .collect::<crate::util::Result<Vec<_>>>()?;

        let owner: String = row.get("owner");

        let func = Function {
            name: name.clone(),
            schema: schema.clone(),
            arguments,
            return_type: crate::model::normalize_pg_type(&return_type),
            language,
            body: body.trim().to_string(),
            volatility,
            security,
            config_params,
            owner: Some(owner),
        };

        let key = qualified_name(&schema, &func.signature());
        functions.insert(key, func);
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

            // Split off DEFAULT clause if present (case-insensitive)
            // Normalize default value to lowercase for consistent comparison
            let (arg_without_default, default) =
                if let Some(idx) = arg.to_uppercase().find(" DEFAULT ") {
                    let default_value = arg[idx + 9..].trim().to_lowercase();
                    (arg[..idx].trim(), Some(default_value))
                } else {
                    (arg, None)
                };

            // Parse mode (IN, OUT, INOUT)
            let (mode, arg_rest) = if let Some(rest) = arg_without_default.strip_prefix("INOUT ") {
                (ArgMode::InOut, rest)
            } else if let Some(rest) = arg_without_default.strip_prefix("OUT ") {
                (ArgMode::Out, rest)
            } else if let Some(rest) = arg_without_default.strip_prefix("IN ") {
                (ArgMode::In, rest)
            } else {
                (ArgMode::In, arg_without_default)
            };

            let parts: Vec<&str> = arg_rest.trim().splitn(2, ' ').collect();
            if parts.len() == 2 {
                FunctionArg {
                    name: Some(strip_ident_quotes(parts[0])),
                    data_type: crate::model::normalize_pg_type(parts[1]),
                    mode,
                    default,
                }
            } else {
                FunctionArg {
                    name: None,
                    data_type: crate::model::normalize_pg_type(arg_rest.trim()),
                    mode,
                    default,
                }
            }
        })
        .collect()
}

async fn introspect_views(
    connection: &PgConnection,
    target_schemas: &[String],
    include_extension_objects: bool,
) -> Result<BTreeMap<String, View>> {
    let mut views = BTreeMap::new();

    let regular_views = sqlx::query(
        r#"
        SELECT v.schemaname, v.viewname, v.definition
        FROM pg_views v
        JOIN pg_class c ON c.relname = v.viewname
        JOIN pg_namespace n ON c.relnamespace = n.oid AND n.nspname = v.schemaname
        WHERE v.schemaname = ANY($1::text[])
          AND ($2::boolean OR NOT EXISTS (
              SELECT 1 FROM pg_depend d
              WHERE d.objid = c.oid
              AND d.deptype = 'e'
          ))
        "#,
    )
    .bind(target_schemas)
    .bind(include_extension_objects)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch views: {e}")))?;

    for row in regular_views {
        let schema: String = row.get("schemaname");
        let name: String = row.get("viewname");
        let definition: String = row.get("definition");

        let view = View {
            name: name.clone(),
            schema: schema.clone(),
            query: normalize_sql_whitespace(definition.trim_end_matches(';')),
            materialized: false,
        };
        let qualified_name = format!("{schema}.{name}");
        views.insert(qualified_name, view);
    }

    let materialized_views = sqlx::query(
        r#"
        SELECT v.schemaname, v.matviewname, v.definition
        FROM pg_matviews v
        JOIN pg_class c ON c.relname = v.matviewname
        JOIN pg_namespace n ON c.relnamespace = n.oid AND n.nspname = v.schemaname
        WHERE v.schemaname = ANY($1::text[])
          AND ($2::boolean OR NOT EXISTS (
              SELECT 1 FROM pg_depend d
              WHERE d.objid = c.oid
              AND d.deptype = 'e'
          ))
        "#,
    )
    .bind(target_schemas)
    .bind(include_extension_objects)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch materialized views: {e}")))?;

    for row in materialized_views {
        let schema: String = row.get("schemaname");
        let name: String = row.get("matviewname");
        let definition: String = row.get("definition");

        let view = View {
            name: name.clone(),
            schema: schema.clone(),
            query: normalize_sql_whitespace(definition.trim_end_matches(';')),
            materialized: true,
        };
        let qualified_name = format!("{schema}.{name}");
        views.insert(qualified_name, view);
    }

    Ok(views)
}

async fn introspect_triggers(
    connection: &PgConnection,
    target_schemas: &[String],
    include_extension_objects: bool,
) -> Result<BTreeMap<String, Trigger>> {
    let mut triggers = BTreeMap::new();

    let rows = sqlx::query(
        r#"
        SELECT
            t.tgname AS trigger_name,
            ns.nspname AS table_schema,
            c.relname AS table_name,
            t.tgtype AS trigger_type,
            t.tgenabled AS trigger_enabled,
            pns.nspname AS function_schema,
            p.proname AS function_name,
            pg_get_triggerdef(t.oid) AS trigger_def,
            (
                SELECT array_agg(a.attname ORDER BY a.attnum)
                FROM unnest(t.tgattr) AS attr_num
                JOIN pg_attribute a ON a.attrelid = t.tgrelid AND a.attnum = attr_num
            ) AS update_columns,
            t.tgoldtable AS old_table_name,
            t.tgnewtable AS new_table_name
        FROM pg_trigger t
        JOIN pg_class c ON t.tgrelid = c.oid
        JOIN pg_namespace ns ON c.relnamespace = ns.oid
        JOIN pg_proc p ON t.tgfoid = p.oid
        JOIN pg_namespace pns ON p.pronamespace = pns.oid
        WHERE NOT t.tgisinternal
          AND ns.nspname = ANY($1::text[])
          AND ($2::boolean OR NOT EXISTS (
              SELECT 1 FROM pg_depend d
              WHERE d.objid = t.oid
              AND d.deptype = 'e'
          ))
        ORDER BY ns.nspname, c.relname, t.tgname
        "#,
    )
    .bind(target_schemas)
    .bind(include_extension_objects)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch triggers: {e}")))?;

    for row in rows {
        let trigger_name: String = row.get("trigger_name");
        let table_schema: String = row.get("table_schema");
        let table_name: String = row.get("table_name");
        let tgtype: i16 = row.get("trigger_type");
        let tgenabled: i8 = row.get::<i8, _>("trigger_enabled");
        let function_schema: String = row.get("function_schema");
        let function_name: String = row.get("function_name");
        let trigger_def: String = row.get("trigger_def");
        let update_columns: Option<Vec<String>> = row.get("update_columns");
        let old_table_name: Option<String> = row.get("old_table_name");
        let new_table_name: Option<String> = row.get("new_table_name");

        let timing = if tgtype & 0x0040 != 0 {
            TriggerTiming::InsteadOf
        } else if tgtype & 0x0002 != 0 {
            TriggerTiming::Before
        } else {
            TriggerTiming::After
        };

        let for_each_row = tgtype & 0x0001 != 0;

        let mut events = Vec::new();
        if tgtype & 0x0004 != 0 {
            events.push(TriggerEvent::Insert);
        }
        if tgtype & 0x0010 != 0 {
            events.push(TriggerEvent::Update);
        }
        if tgtype & 0x0008 != 0 {
            events.push(TriggerEvent::Delete);
        }
        if tgtype & 0x0020 != 0 {
            events.push(TriggerEvent::Truncate);
        }

        let when_clause =
            extract_when_clause(&trigger_def).map(|w| crate::util::normalize_type_casts(&w));

        let enabled = match tgenabled as u8 as char {
            'D' => TriggerEnabled::Disabled,
            'R' => TriggerEnabled::Replica,
            'A' => TriggerEnabled::Always,
            _ => TriggerEnabled::Origin,
        };

        let trigger = Trigger {
            name: trigger_name.clone(),
            target_schema: table_schema.clone(),
            target_name: table_name.clone(),
            timing,
            events: {
                let mut sorted = events;
                sorted.sort();
                sorted
            },
            update_columns: update_columns.unwrap_or_default(),
            for_each_row,
            when_clause,
            function_schema,
            function_name,
            function_args: vec![],
            enabled,
            old_table_name,
            new_table_name,
        };

        let key = format!("{table_schema}.{table_name}.{trigger_name}");
        triggers.insert(key, trigger);
    }

    Ok(triggers)
}

fn extract_when_clause(trigger_def: &str) -> Option<String> {
    let upper = trigger_def.to_uppercase();
    if let Some(when_pos) = upper.find(" WHEN (") {
        let after_when = &trigger_def[when_pos + 7..];
        let mut depth = 1;
        let mut end_pos = 0;
        for (i, c) in after_when.chars().enumerate() {
            match c {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        end_pos = i;
                        break;
                    }
                }
                _ => {}
            }
        }
        if end_pos > 0 {
            return Some(after_when[..end_pos].to_string());
        }
    }
    None
}

async fn introspect_sequences(
    connection: &PgConnection,
    target_schemas: &[String],
    include_extension_objects: bool,
) -> Result<BTreeMap<String, Sequence>> {
    let rows = sqlx::query(
        r#"
        SELECT
            s.schemaname as schema,
            s.sequencename as name,
            s.data_type::text,
            s.start_value,
            s.increment_by,
            s.min_value,
            s.max_value,
            s.cycle,
            s.cache_size,
            c.relname as owned_table,
            cn.nspname as owned_schema,
            a.attname as owned_column
        FROM pg_sequences s
        JOIN pg_namespace n ON n.nspname = s.schemaname
        LEFT JOIN pg_class seq_class ON seq_class.relname = s.sequencename
            AND seq_class.relnamespace = n.oid
            AND seq_class.relkind = 'S'
        LEFT JOIN pg_depend d ON d.objid = seq_class.oid
            AND d.deptype = 'a'
        LEFT JOIN pg_class c ON c.oid = d.refobjid
        LEFT JOIN pg_namespace cn ON cn.oid = c.relnamespace
        LEFT JOIN pg_attribute a ON a.attrelid = d.refobjid
            AND a.attnum = d.refobjsubid
        WHERE s.schemaname = ANY($1::text[])
          AND ($2::boolean OR NOT EXISTS (
              SELECT 1 FROM pg_depend ext_d
              WHERE ext_d.objid = seq_class.oid
              AND ext_d.deptype = 'e'
          ))
        "#,
    )
    .bind(target_schemas)
    .bind(include_extension_objects)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch sequences: {e}")))?;

    let mut sequences = BTreeMap::new();
    for row in rows {
        let schema: String = row.get("schema");
        let name: String = row.get("name");
        let data_type: String = row.get("data_type");
        let start_value: Option<i64> = row.get("start_value");
        let increment_by: Option<i64> = row.get("increment_by");
        let min_value: Option<i64> = row.get("min_value");
        let max_value: Option<i64> = row.get("max_value");
        let cycle: Option<bool> = row.get("cycle");
        let cache_size: Option<i64> = row.get("cache_size");

        let owned_table: Option<String> = row.get("owned_table");
        let owned_schema: Option<String> = row.get("owned_schema");
        let owned_column: Option<String> = row.get("owned_column");

        let owned_by = match (owned_schema, owned_table, owned_column) {
            (Some(ts), Some(t), Some(c)) => Some(SequenceOwner {
                table_schema: ts,
                table_name: t,
                column_name: c,
            }),
            _ => None,
        };

        let seq_data_type = match data_type.as_str() {
            "smallint" => SequenceDataType::SmallInt,
            "integer" => SequenceDataType::Integer,
            "bigint" => SequenceDataType::BigInt,
            _ => panic!("Unknown sequence data type from PostgreSQL: '{data_type}'"),
        };

        let qualified_name = format!("{schema}.{name}");
        sequences.insert(
            qualified_name,
            Sequence {
                name,
                schema,
                data_type: seq_data_type,
                start: start_value,
                increment: increment_by,
                min_value,
                max_value,
                cycle: cycle.unwrap_or(false),
                cache: cache_size,
                owned_by,
            },
        );
    }

    Ok(sequences)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_function_arguments_strips_quotes_from_names() {
        let args = parse_function_arguments("\"p_role_name\" text, \"p_enterprise_id\" uuid");

        assert_eq!(args.len(), 2);
        assert_eq!(args[0].name, Some("p_role_name".to_string()));
        assert_eq!(args[1].name, Some("p_enterprise_id".to_string()));
    }

    #[test]
    fn parse_function_arguments_handles_unquoted_names() {
        let args = parse_function_arguments("role_name text, enterprise_id uuid");

        assert_eq!(args.len(), 2);
        assert_eq!(args[0].name, Some("role_name".to_string()));
        assert_eq!(args[1].name, Some("enterprise_id".to_string()));
    }
}
