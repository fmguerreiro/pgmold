use crate::model::*;
use crate::pg::connection::PgConnection;
use crate::pg::sqlgen::strip_ident_quotes;
use crate::util::{normalize_sql_whitespace, Result, SchemaError};
use sqlx::Row;
use std::collections::{BTreeMap, BTreeSet};

/// Queries run concurrently via try_join! — requires a connection pool
/// with enough capacity (default max_connections=5 handles the concurrency
/// since sqlx queues excess acquires).
pub async fn introspect_schema(
    connection: &PgConnection,
    target_schemas: &[String],
    include_extension_objects: bool,
) -> Result<Schema> {
    let (
        schemas,
        extensions,
        enums,
        domains,
        tables,
        functions,
        views,
        triggers,
        sequences,
        table_view_grants,
        sequence_grants,
        function_grants,
        schema_grants,
        type_grants,
        partition_keys,
        partitions,
        mut all_columns,
        mut all_primary_keys,
        mut all_indexes,
        mut all_foreign_keys,
        mut all_check_constraints,
        mut all_exclusion_constraints,
        mut all_rls,
        mut all_force_rls,
        mut all_policies,
        default_privileges,
    ) = tokio::try_join!(
        introspect_schemas(connection, target_schemas),
        introspect_extensions(connection),
        introspect_enums(connection, target_schemas, include_extension_objects),
        introspect_domains(connection, target_schemas, include_extension_objects),
        introspect_tables(connection, target_schemas, include_extension_objects),
        introspect_functions(connection, target_schemas, include_extension_objects),
        introspect_views(connection, target_schemas, include_extension_objects),
        introspect_triggers(connection, target_schemas, include_extension_objects),
        introspect_sequences(connection, target_schemas, include_extension_objects),
        introspect_table_view_grants(connection, target_schemas),
        introspect_sequence_grants(connection, target_schemas),
        introspect_function_grants(connection, target_schemas),
        introspect_schema_grants(connection, target_schemas),
        introspect_type_grants(connection, target_schemas),
        introspect_partition_keys(connection, target_schemas),
        introspect_partitions(connection, target_schemas),
        introspect_all_columns(connection, target_schemas),
        introspect_all_primary_keys(connection, target_schemas),
        introspect_all_indexes(connection, target_schemas),
        introspect_all_foreign_keys(connection, target_schemas),
        introspect_all_check_constraints(connection, target_schemas),
        introspect_all_exclusion_constraints(connection, target_schemas),
        introspect_all_rls(connection, target_schemas),
        introspect_all_force_rls(connection, target_schemas),
        introspect_all_policies(connection, target_schemas),
        introspect_default_privileges(connection, target_schemas),
    )?;

    let mut schema = Schema::new();
    schema.schemas = schemas;
    schema.extensions = extensions;
    schema.enums = enums;
    schema.domains = domains;
    schema.tables = tables;
    schema.functions = functions;
    schema.views = views;
    schema.triggers = triggers;
    schema.sequences = sequences;
    schema.partitions = partitions;
    schema.default_privileges = default_privileges;

    for (qualified_name, grants) in table_view_grants {
        if let Some(table) = schema.tables.get_mut(&qualified_name) {
            table.grants = grants;
        } else if let Some(view) = schema.views.get_mut(&qualified_name) {
            view.grants = grants;
        }
    }

    for (qualified_name, grants) in sequence_grants {
        if let Some(sequence) = schema.sequences.get_mut(&qualified_name) {
            sequence.grants = grants;
        }
    }

    for (qualified_name, grants) in function_grants {
        if let Some(function) = schema.functions.get_mut(&qualified_name) {
            function.grants = grants;
        }
    }

    for (schema_name, grants) in schema_grants {
        if let Some(pg_schema) = schema.schemas.get_mut(&schema_name) {
            pg_schema.grants = grants;
        }
    }

    for (qualified_name, grants) in type_grants {
        if let Some(enum_type) = schema.enums.get_mut(&qualified_name) {
            enum_type.grants = grants;
        } else if let Some(domain) = schema.domains.get_mut(&qualified_name) {
            domain.grants = grants;
        }
    }

    for (qualified_name, partition_key) in partition_keys {
        if let Some(table) = schema.tables.get_mut(&qualified_name) {
            table.partition_by = Some(partition_key);
        }
    }

    for (qualified_name, table) in &mut schema.tables {
        if let Some(columns) = all_columns.remove(qualified_name) {
            table.columns = columns;
        }
        table.primary_key = all_primary_keys.remove(qualified_name);
        if let Some(mut indexes) = all_indexes.remove(qualified_name) {
            indexes.sort();
            table.indexes = indexes;
        }
        if let Some(mut foreign_keys) = all_foreign_keys.remove(qualified_name) {
            foreign_keys.sort();
            table.foreign_keys = foreign_keys;
        }
        if let Some(mut check_constraints) = all_check_constraints.remove(qualified_name) {
            check_constraints.sort();
            table.check_constraints = check_constraints;
        }
        if let Some(mut exclusion_constraints) = all_exclusion_constraints.remove(qualified_name) {
            exclusion_constraints.sort();
            table.exclusion_constraints = exclusion_constraints;
        }
        if let Some(rls) = all_rls.remove(qualified_name) {
            table.row_level_security = rls;
        }
        if let Some(force_rls) = all_force_rls.remove(qualified_name) {
            table.force_row_level_security = force_rls;
        }
        if let Some(mut policies) = all_policies.remove(qualified_name) {
            policies.sort();
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
            schemas.insert(
                name.clone(),
                PgSchema {
                    name,
                    grants: Vec::new(),
                    // TODO: read schema comment from pg_description
                    comment: None,
                },
            );
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
        SELECT n.nspname, t.typname, array_agg(e.enumlabel ORDER BY e.enumsortorder) as labels, r.rolname AS owner
        FROM pg_type t
        JOIN pg_enum e ON t.oid = e.enumtypid
        JOIN pg_namespace n ON t.typnamespace = n.oid
        JOIN pg_roles r ON t.typowner = r.oid
        WHERE n.nspname = ANY($1::text[])
          AND ($2::boolean OR NOT EXISTS (
              SELECT 1 FROM pg_depend d
              WHERE d.objid = t.oid
              AND d.deptype = 'e'
          ))
        GROUP BY n.nspname, t.typname, r.rolname
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
        let owner: String = row.get("owner");
        let enum_type = EnumType {
            name: name.clone(),
            schema: schema.clone(),
            values: labels,
            owner: Some(owner),
            grants: Vec::new(),
            // TODO: read enum type comment from pg_description
            comment: None,
        };
        enums.insert(qualified_name(&schema, &name), enum_type);
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
            bt.typcategory::text AS base_category,
            t.typnotnull AS not_null,
            pg_get_expr(t.typdefaultbin, 0) AS default_expr,
            r.rolname AS owner
        FROM pg_type t
        JOIN pg_namespace n ON t.typnamespace = n.oid
        JOIN pg_type bt ON t.typbasetype = bt.oid
        JOIN pg_roles r ON t.typowner = r.oid
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

    if rows.is_empty() {
        return Ok(BTreeMap::new());
    }

    let all_constraints =
        introspect_all_domain_constraints(connection, target_schemas, include_extension_objects)
            .await?;

    let mut domains = BTreeMap::new();
    for row in rows {
        let schema: String = row.get("schema_name");
        let name: String = row.get("domain_name");
        let base_type: String = row.get("base_type");
        let base_category: String = row.get("base_category");
        let not_null: bool = row.get("not_null");
        let default_expr: Option<String> = row
            .get::<Option<String>, &str>("default_expr")
            .filter(|s| !s.is_empty());
        let owner: String = row.get("owner");

        let data_type = if base_category == "A" {
            let base_udt = base_type.strip_prefix('_').ok_or_else(|| {
                SchemaError::ParseError(format!(
                    "expected array base_type to start with '_', got: {base_type}"
                ))
            })?;
            let element_type = map_domain_element_type(base_udt, &schema);
            PgType::Array(Box::new(element_type))
        } else {
            match base_type.as_str() {
                "integer" | "int4" => PgType::Integer,
                "bigint" | "int8" => PgType::BigInt,
                "smallint" | "int2" => PgType::SmallInt,
                "real" | "float4" => PgType::Real,
                "double precision" | "float8" => PgType::DoublePrecision,
                "numeric" => PgType::BuiltinNamed("numeric".to_string()),
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
                        PgType::UserDefined(base_type)
                    } else {
                        PgType::UserDefined(qualified)
                    }
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
            check_constraints: all_constraints
                .get(&qualified_name(&schema, &name))
                .cloned()
                .unwrap_or_default(),
            owner: Some(owner),
            grants: Vec::new(),
            // TODO: read domain comment from pg_description
            comment: None,
        };
        domains.insert(qualified_name(&schema, &name), domain);
    }

    Ok(domains)
}

async fn introspect_all_domain_constraints(
    connection: &PgConnection,
    target_schemas: &[String],
    include_extension_objects: bool,
) -> Result<BTreeMap<String, Vec<DomainConstraint>>> {
    let rows = sqlx::query(
        r#"
        SELECT
            n.nspname AS schema_name,
            t.typname AS domain_name,
            con.conname AS constraint_name,
            pg_get_constraintdef(con.oid) AS constraint_def
        FROM pg_constraint con
        JOIN pg_type t ON con.contypid = t.oid
        JOIN pg_namespace n ON t.typnamespace = n.oid
        WHERE con.contype = 'c'
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
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch domain constraints: {e}")))?;

    let mut constraints_by_domain: BTreeMap<String, Vec<DomainConstraint>> = BTreeMap::new();
    for row in rows {
        let schema: String = row.get("schema_name");
        let domain_name: String = row.get("domain_name");
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

        constraints_by_domain
            .entry(qualified_name(&schema, &domain_name))
            .or_default()
            .push(DomainConstraint {
                name: constraint_name,
                expression,
            });
    }

    Ok(constraints_by_domain)
}

async fn introspect_tables(
    connection: &PgConnection,
    target_schemas: &[String],
    include_extension_objects: bool,
) -> Result<BTreeMap<String, Table>> {
    let rows = sqlx::query(
        r#"
        SELECT n.nspname AS table_schema, c.relname AS table_name, r.rolname AS owner
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_roles r ON c.relowner = r.oid
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
        let owner: String = row.get("owner");
        let table = Table {
            name: name.clone(),
            schema: schema.clone(),
            columns: BTreeMap::new(),
            indexes: Vec::new(),
            primary_key: None,
            foreign_keys: Vec::new(),
            check_constraints: Vec::new(),
            exclusion_constraints: Vec::new(),
            // TODO: read table comment from pg_description
            comment: None,
            row_level_security: false,
            force_row_level_security: false,
            policies: Vec::new(),
            partition_by: None,
            owner: Some(owner),
            grants: Vec::new(),
        };
        tables.insert(qualified_name(&schema, &name), table);
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

        let columns = extract_paren_values(&key_def);

        let partition_key = PartitionKey {
            strategy,
            columns,
            expressions: Vec::new(),
        };

        partition_keys.insert(qualified_name(&schema, &name), partition_key);
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
            pg_get_expr(c.relpartbound, c.oid) as partition_bound,
            r.rolname as owner
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_roles r ON c.relowner = r.oid
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
        let bound_expr: Option<String> = row.get("partition_bound");
        let owner: String = row.get("owner");

        let bound_expr = match bound_expr {
            Some(expr) => expr,
            None => continue,
        };
        let bound = parse_partition_bound(&bound_expr)?;

        let partition = Partition {
            schema: schema.clone(),
            name: name.clone(),
            parent_schema,
            parent_name,
            bound,
            indexes: Vec::new(),
            check_constraints: Vec::new(),
            owner: Some(owner),
        };

        partitions.insert(qualified_name(&schema, &name), partition);
    }

    Ok(partitions)
}

/// Parse a partition bound expression like "FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')"
fn parse_partition_bound(expr: &str) -> Result<PartitionBound> {
    let expr_upper = expr.to_uppercase();

    if expr_upper.contains("DEFAULT") {
        return Ok(PartitionBound::Default);
    }

    if expr_upper.contains("FROM") && expr_upper.contains("TO") {
        if let Some(from_start) = expr_upper.find("FROM ") {
            let after_from_upper = &expr_upper[from_start + 5..];
            if let Some(to_pos) = after_from_upper.find(" TO ") {
                let from_part_raw = &expr[from_start + 5..from_start + 5 + to_pos];
                let to_part_raw = &expr[from_start + 5 + to_pos + 4..];
                let from_values = extract_paren_values(from_part_raw.trim());
                let to_values = extract_paren_values(to_part_raw.trim());
                return Ok(PartitionBound::Range {
                    from: from_values,
                    to: to_values,
                });
            }
        }
    }

    if expr_upper.contains("IN") {
        if let Some(in_pos) = expr.find("IN") {
            let values_part = &expr[in_pos + 2..].trim();
            let values = extract_paren_values(values_part);
            return Ok(PartitionBound::List { values });
        }
    }

    if expr_upper.contains("MODULUS") && expr_upper.contains("REMAINDER") {
        if let Some(with_pos) = expr.find("WITH") {
            let params_part = &expr[with_pos + 4..].trim();
            let params = extract_paren_values(params_part);
            let mut modulus = 0u32;
            let mut remainder = 0u32;

            for param in params {
                let param_upper = param.to_uppercase();
                if param_upper.contains("MODULUS") {
                    if let Some(val) = param.split_whitespace().last() {
                        modulus = val.parse().map_err(|_| {
                            SchemaError::DatabaseError(format!(
                                "invalid hash partition MODULUS value: {val}"
                            ))
                        })?;
                    }
                } else if param_upper.contains("REMAINDER") {
                    if let Some(val) = param.split_whitespace().last() {
                        remainder = val.parse().map_err(|_| {
                            SchemaError::DatabaseError(format!(
                                "invalid hash partition REMAINDER value: {val}"
                            ))
                        })?;
                    }
                }
            }

            return Ok(PartitionBound::Hash { modulus, remainder });
        }
    }

    Err(SchemaError::ParseError(format!(
        "unrecognized partition bound expression: {expr}"
    )))
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

async fn introspect_all_columns(
    connection: &PgConnection,
    target_schemas: &[String],
) -> Result<BTreeMap<String, BTreeMap<String, Column>>> {
    let rows = sqlx::query(
        r#"
        SELECT
            c.table_schema,
            c.table_name,
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
        WHERE c.table_schema = ANY($1::text[])
          AND t.relkind IN ('r', 'p')
          AND t.relispartition = false
        ORDER BY c.table_schema, c.table_name, c.ordinal_position
        "#,
    )
    .bind(target_schemas)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch columns: {e}")))?;

    let mut result: BTreeMap<String, BTreeMap<String, Column>> = BTreeMap::new();
    for row in rows {
        let table_schema: String = row.get("table_schema");
        let table_name: String = row.get("table_name");
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
        )?;

        result
            .entry(qualified_name(&table_schema, &table_name))
            .or_default()
            .insert(
                name.clone(),
                Column {
                    name,
                    data_type: pg_type,
                    nullable: is_nullable == "YES",
                    default: column_default,
                    // TODO: read column comment from pg_description
                    comment: None,
                },
            );
    }

    Ok(result)
}

fn map_udt_name_to_pg_type(udt_name: &str, udt_schema: &str, atttypmod: Option<i32>) -> PgType {
    match udt_name {
        "bool" => PgType::Boolean,
        "int4" | "int" => PgType::Integer,
        "int8" => PgType::BigInt,
        "int2" => PgType::SmallInt,
        "float4" => PgType::Real,
        "float8" => PgType::DoublePrecision,
        "text" => PgType::Text,
        "varchar" => {
            let length = atttypmod.and_then(|m| if m > 0 { Some((m - 4) as u32) } else { None });
            PgType::Varchar(length)
        }
        "uuid" => PgType::Uuid,
        "timestamptz" => PgType::TimestampTz,
        "timestamp" => PgType::Timestamp,
        "time" => PgType::Time,
        "timetz" => PgType::TimeTz,
        "date" => PgType::Date,
        "interval" => PgType::Interval,
        "bytea" => PgType::Bytea,
        "json" => PgType::Json,
        "jsonb" => PgType::Jsonb,
        "numeric" => PgType::BuiltinNamed("numeric".to_string()),
        "inet" => PgType::Inet,
        "cidr" => PgType::Cidr,
        "macaddr" => PgType::Macaddr,
        "macaddr8" => PgType::Macaddr8,
        "bpchar" => {
            let length = atttypmod.and_then(|m| if m > 4 { Some((m - 4) as u32) } else { None });
            PgType::Char(length)
        }
        "point" => PgType::Point,
        "xml" => PgType::Xml,
        "int4range" | "int8range" | "numrange" | "tsrange" | "tstzrange" | "daterange"
        | "int4multirange" | "int8multirange" | "nummultirange" | "tsmultirange"
        | "tstzmultirange" | "datemultirange" => PgType::BuiltinNamed(udt_name.to_string()),
        _ => PgType::UserDefined(format!("{udt_schema}.{udt_name}")),
    }
}

fn map_pg_type(
    data_type: &str,
    char_max_length: Option<i32>,
    udt_schema: &str,
    udt_name: &str,
    atttypmod: i32,
) -> Result<PgType> {
    match data_type {
        "integer" => Ok(PgType::Integer),
        "bigint" => Ok(PgType::BigInt),
        "smallint" => Ok(PgType::SmallInt),
        "real" => Ok(PgType::Real),
        "double precision" => Ok(PgType::DoublePrecision),
        "numeric" => Ok(PgType::BuiltinNamed("numeric".to_string())),
        "character varying" => Ok(PgType::Varchar(char_max_length.map(|l| l as u32))),
        "text" => Ok(PgType::Text),
        "boolean" => Ok(PgType::Boolean),
        "timestamp with time zone" => Ok(PgType::TimestampTz),
        "timestamp without time zone" => Ok(PgType::Timestamp),
        "time without time zone" => Ok(PgType::Time),
        "time with time zone" => Ok(PgType::TimeTz),
        "date" => Ok(PgType::Date),
        "interval" => Ok(PgType::Interval),
        "bytea" => Ok(PgType::Bytea),
        "character" => Ok(PgType::Char(char_max_length.map(|l| l as u32))),
        "uuid" => Ok(PgType::Uuid),
        "json" => Ok(PgType::Json),
        "jsonb" => Ok(PgType::Jsonb),
        "inet" => Ok(PgType::Inet),
        "cidr" => Ok(PgType::Cidr),
        "macaddr" => Ok(PgType::Macaddr),
        "macaddr8" => Ok(PgType::Macaddr8),
        "point" => Ok(PgType::Point),
        "xml" => Ok(PgType::Xml),
        "int4range" | "int8range" | "numrange" | "tsrange" | "tstzrange" | "daterange"
        | "int4multirange" | "int8multirange" | "nummultirange" | "tsmultirange"
        | "tstzmultirange" | "datemultirange" => {
            Ok(PgType::BuiltinNamed(data_type.to_string()))
        }
        "USER-DEFINED" => {
            if udt_name == "vector" {
                // pgvector stores dimension directly in atttypmod
                // -1 means no dimension constraint (e.g., vector vs vector(1536))
                let dimension = if atttypmod != -1 {
                    Some(atttypmod as u32)
                } else {
                    None
                };
                Ok(PgType::Vector(dimension))
            } else {
                Ok(PgType::UserDefined(format!("{udt_schema}.{udt_name}")))
            }
        }
        "ARRAY" => {
            // PostgreSQL array types have udt_name prefixed with underscore (e.g., "_bool", "_int4")
            let base_udt = udt_name.strip_prefix('_').ok_or_else(|| {
                SchemaError::ParseError(format!(
                    "expected array udt_name to start with '_', got: {udt_name}"
                ))
            })?;
            let element_type = map_udt_name_to_pg_type(base_udt, udt_schema, Some(atttypmod));
            Ok(PgType::Array(Box::new(element_type)))
        }
        other => Err(SchemaError::ParseError(format!(
            "unsupported column type from database: {other} (udt_name: {udt_name})"
        ))),
    }
}

fn map_domain_element_type(base_udt: &str, domain_schema: &str) -> PgType {
    map_udt_name_to_pg_type(base_udt, domain_schema, None)
}

async fn introspect_all_primary_keys(
    connection: &PgConnection,
    target_schemas: &[String],
) -> Result<BTreeMap<String, PrimaryKey>> {
    let rows = sqlx::query(
        r#"
        SELECT
            n.nspname AS table_schema,
            c.relname AS table_name,
            array_agg(a.attname ORDER BY array_position(i.indkey, a.attnum)) as columns
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indrelid
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = ANY($1::text[])
          AND i.indisprimary
          AND c.relkind IN ('r', 'p')
          AND c.relispartition = false
        GROUP BY n.nspname, c.relname, i.indexrelid
        "#,
    )
    .bind(target_schemas)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch primary keys: {e}")))?;

    let mut result = BTreeMap::new();
    for row in rows {
        let table_schema: String = row.get("table_schema");
        let table_name: String = row.get("table_name");
        let columns: Vec<String> = row.get("columns");
        result.insert(
            qualified_name(&table_schema, &table_name),
            PrimaryKey { columns },
        );
    }

    Ok(result)
}

async fn introspect_all_indexes(
    connection: &PgConnection,
    target_schemas: &[String],
) -> Result<BTreeMap<String, Vec<Index>>> {
    let rows = sqlx::query(
        r#"
        SELECT
            n.nspname AS table_schema,
            t.relname AS table_name,
            i.relname as index_name,
            ix.indisunique,
            am.amname,
            COALESCE((SELECT array_agg(
                CASE WHEN ix.indkey[k] = 0
                     THEN pg_get_indexdef(ix.indexrelid, k + 1, false)
                     ELSE (SELECT a.attname::text FROM pg_attribute a WHERE a.attrelid = t.oid AND a.attnum = ix.indkey[k])
                END ORDER BY k
            ) FROM generate_series(0, array_length(ix.indkey, 1) - 1) AS k), ARRAY[]::text[]) as columns,
            pg_get_expr(ix.indpred, ix.indrelid) as predicate,
            (uc.oid IS NOT NULL) AS is_constraint
        FROM pg_index ix
        JOIN pg_class t ON t.oid = ix.indrelid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_am am ON am.oid = i.relam
        JOIN pg_namespace n ON n.oid = t.relnamespace
        LEFT JOIN pg_constraint uc ON uc.conindid = ix.indexrelid AND uc.contype = 'u'
        WHERE n.nspname = ANY($1::text[])
          AND NOT ix.indisprimary
          AND t.relkind IN ('r', 'p')
          AND t.relispartition = false
        "#,
    )
    .bind(target_schemas)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch indexes: {e}")))?;

    let mut result: BTreeMap<String, Vec<Index>> = BTreeMap::new();
    for row in rows {
        let table_schema: String = row.get("table_schema");
        let table_name: String = row.get("table_name");
        let name: String = row.get("index_name");
        let unique: bool = row.get("indisunique");
        let am_name: String = row.get("amname");
        let columns: Vec<String> = row.get("columns");
        let predicate: Option<String> = row.get("predicate");
        let is_constraint: bool = row.get("is_constraint");

        let index_type = match am_name.as_str() {
            "btree" => IndexType::BTree,
            "hash" => IndexType::Hash,
            "gin" => IndexType::Gin,
            "gist" => IndexType::Gist,
            _ => panic!("unsupported index type: {am_name}"),
        };

        result
            .entry(qualified_name(&table_schema, &table_name))
            .or_default()
            .push(Index {
                name,
                columns,
                unique,
                index_type,
                predicate,
                is_constraint,
            });
    }

    Ok(result)
}

async fn introspect_all_foreign_keys(
    connection: &PgConnection,
    target_schemas: &[String],
) -> Result<BTreeMap<String, Vec<ForeignKey>>> {
    let rows = sqlx::query(
        r#"
        SELECT
            n.nspname AS table_schema,
            class.relname AS table_name,
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
        WHERE n.nspname = ANY($1::text[])
          AND con.contype = 'f'
          AND class.relkind IN ('r', 'p')
          AND class.relispartition = false
        GROUP BY n.nspname, class.relname, con.conname, ref_class.relname, ref_n.nspname, con.confdeltype, con.confupdtype
        "#,
    )
    .bind(target_schemas)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch foreign keys: {e}")))?;

    let mut result: BTreeMap<String, Vec<ForeignKey>> = BTreeMap::new();
    for row in rows {
        let table_schema: String = row.get("table_schema");
        let table_name: String = row.get("table_name");
        let name: String = row.get("name");
        let referenced_table: String = row.get("referenced_table");
        let referenced_schema: String = row.get("referenced_schema");
        let columns: Vec<String> = row.get("columns");
        let referenced_columns: Vec<String> = row.get("referenced_columns");
        let confdeltype: i8 = row.get::<i8, _>("confdeltype");
        let confupdtype: i8 = row.get::<i8, _>("confupdtype");

        result
            .entry(qualified_name(&table_schema, &table_name))
            .or_default()
            .push(ForeignKey {
                name,
                columns,
                referenced_table,
                referenced_schema,
                referenced_columns,
                on_delete: map_referential_action(pg_char(confdeltype)),
                on_update: map_referential_action(pg_char(confupdtype)),
            });
    }

    Ok(result)
}

async fn introspect_all_check_constraints(
    connection: &PgConnection,
    target_schemas: &[String],
) -> Result<BTreeMap<String, Vec<CheckConstraint>>> {
    let rows = sqlx::query(
        r#"
        SELECT
            n.nspname AS table_schema,
            class.relname AS table_name,
            con.conname as name,
            pg_get_constraintdef(con.oid) as definition
        FROM pg_constraint con
        JOIN pg_class class ON con.conrelid = class.oid
        JOIN pg_namespace n ON n.oid = class.relnamespace
        WHERE n.nspname = ANY($1::text[])
          AND con.contype = 'c'
          AND class.relkind IN ('r', 'p')
          AND class.relispartition = false
        "#,
    )
    .bind(target_schemas)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch check constraints: {e}")))?;

    let mut result: BTreeMap<String, Vec<CheckConstraint>> = BTreeMap::new();
    for row in rows {
        let table_schema: String = row.get("table_schema");
        let table_name: String = row.get("table_name");
        let name: String = row.get("name");
        let definition: String = row.get("definition");

        let expression = definition
            .strip_prefix("CHECK (")
            .and_then(|s| s.strip_suffix(")"))
            .map(|s| s.to_string())
            .unwrap_or(definition);

        result
            .entry(qualified_name(&table_schema, &table_name))
            .or_default()
            .push(CheckConstraint { name, expression });
    }

    Ok(result)
}

async fn introspect_all_exclusion_constraints(
    connection: &PgConnection,
    target_schemas: &[String],
) -> Result<BTreeMap<String, Vec<ExclusionConstraint>>> {
    let rows = sqlx::query(
        r#"
        SELECT
            n.nspname AS table_schema,
            class.relname AS table_name,
            con.conname AS name,
            COALESCE(am.amname, 'gist') AS index_method,
            pg_get_constraintdef(con.oid) AS definition,
            con.condeferrable AS deferrable,
            con.condeferred AS initially_deferred
        FROM pg_constraint con
        JOIN pg_class class ON con.conrelid = class.oid
        JOIN pg_namespace n ON n.oid = class.relnamespace
        LEFT JOIN pg_class idx ON con.conindid = idx.oid
        LEFT JOIN pg_am am ON idx.relam = am.oid
        WHERE n.nspname = ANY($1::text[])
          AND con.contype = 'x'
          AND class.relkind IN ('r', 'p')
          AND class.relispartition = false
        "#,
    )
    .bind(target_schemas)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| {
        SchemaError::DatabaseError(format!("Failed to fetch exclusion constraints: {e}"))
    })?;

    let mut result: BTreeMap<String, Vec<ExclusionConstraint>> = BTreeMap::new();
    for row in rows {
        let table_schema: String = row.get("table_schema");
        let table_name: String = row.get("table_name");
        let name: String = row.get("name");
        let index_method: String = row.get("index_method");
        let definition: String = row.get("definition");
        let deferrable: bool = row.get("deferrable");
        let initially_deferred: bool = row.get("initially_deferred");

        let (elements, where_clause) = parse_exclusion_definition(&definition);

        result
            .entry(qualified_name(&table_schema, &table_name))
            .or_default()
            .push(ExclusionConstraint {
                name,
                index_method,
                elements,
                where_clause,
                deferrable,
                initially_deferred,
            });
    }

    Ok(result)
}

/// Parses a `pg_get_constraintdef` output for an EXCLUDE constraint.
///
/// Input looks like:
///   `EXCLUDE USING gist (col1 WITH &&, col2 WITH =) WHERE (predicate)`
///
/// Returns the parsed elements and optional WHERE clause.
fn parse_exclusion_definition(definition: &str) -> (Vec<ExclusionElement>, Option<String>) {
    let rest = definition.trim();
    let rest = rest.strip_prefix("EXCLUDE USING ").unwrap_or(rest);

    // Skip the access method name up to the first '('
    let paren_start = match rest.find('(') {
        Some(p) => p,
        None => return (Vec::new(), None),
    };
    let after_method = &rest[paren_start + 1..];

    // Find the matching closing paren for the elements list
    let mut depth = 1usize;
    let mut end = 0;
    for (i, ch) in after_method.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    end = i;
                    break;
                }
            }
            _ => {}
        }
    }

    let elements_str = &after_method[..end];
    let tail = after_method[end + 1..].trim();

    let where_clause = if let Some(rest) = tail.strip_prefix("WHERE ") {
        let rest = rest.trim();
        let inner = rest.strip_prefix('(').and_then(|s| s.strip_suffix(')'));
        Some(inner.unwrap_or(rest).to_string())
    } else {
        None
    };

    let elements = parse_exclusion_elements(elements_str);
    (elements, where_clause)
}

/// Parses the comma-separated element list inside `EXCLUDE USING gist (...)`.
///
/// Each element looks like `col WITH operator` or `(expr) WITH operator`.
/// Commas inside nested parens are treated as part of the expression.
fn parse_exclusion_elements(elements_str: &str) -> Vec<ExclusionElement> {
    let mut elements = Vec::new();
    let mut depth = 0usize;
    let mut start = 0;

    for (i, ch) in elements_str.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                let segment = elements_str[start..i].trim();
                if let Some(element) = parse_single_exclusion_element(segment) {
                    elements.push(element);
                }
                start = i + 1;
            }
            _ => {}
        }
    }
    let last = elements_str[start..].trim();
    if !last.is_empty() {
        if let Some(element) = parse_single_exclusion_element(last) {
            elements.push(element);
        }
    }

    elements
}

fn parse_single_exclusion_element(segment: &str) -> Option<ExclusionElement> {
    // Pattern: `<expr> WITH <operator>`
    // Find " WITH " (case-insensitive) from the right to handle expressions containing WITH
    let upper = segment.to_uppercase();
    let with_pos = upper.rfind(" WITH ")?;
    let column_or_expression = segment[..with_pos].trim().to_string();
    let operator = segment[with_pos + " WITH ".len()..].trim().to_string();
    Some(ExclusionElement {
        column_or_expression,
        operator,
    })
}

/// Normalize a proconfig value from PostgreSQL's GUC format to valid SQL.
///
/// PostgreSQL stores `SET search_path = ''` as `search_path=""` in proconfig.
/// The `""` is PostgreSQL's GUC representation of an empty string — but in SQL,
/// `""` is a zero-length delimited identifier (invalid). Convert double-quoted
/// GUC values to single-quoted SQL string literals so they match what the SQL
/// parser produces from schema files.
fn normalize_proconfig_value(value: &str) -> String {
    if value.starts_with('"') && value.ends_with('"') && value.len() >= 2 {
        let inner = &value[1..value.len() - 1];
        format!("'{inner}'")
    } else {
        value.to_string()
    }
}

fn pg_char(value: i8) -> char {
    value as u8 as char
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

async fn introspect_all_rls(
    connection: &PgConnection,
    target_schemas: &[String],
) -> Result<BTreeMap<String, bool>> {
    let rows = sqlx::query(
        r#"
        SELECT
            n.nspname AS table_schema,
            c.relname AS table_name,
            c.relrowsecurity
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = ANY($1::text[])
          AND c.relkind IN ('r', 'p')
          AND c.relispartition = false
        "#,
    )
    .bind(target_schemas)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch RLS status: {e}")))?;

    let mut result = BTreeMap::new();
    for row in rows {
        let table_schema: String = row.get("table_schema");
        let table_name: String = row.get("table_name");
        let rls: bool = row.get("relrowsecurity");
        result.insert(qualified_name(&table_schema, &table_name), rls);
    }

    Ok(result)
}

async fn introspect_all_force_rls(
    connection: &PgConnection,
    target_schemas: &[String],
) -> Result<BTreeMap<String, bool>> {
    let rows = sqlx::query(
        r#"
        SELECT
            n.nspname AS table_schema,
            c.relname AS table_name,
            c.relforcerowsecurity
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = ANY($1::text[])
          AND c.relkind IN ('r', 'p')
          AND c.relispartition = false
        "#,
    )
    .bind(target_schemas)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch FORCE RLS status: {e}")))?;

    let mut result = BTreeMap::new();
    for row in rows {
        let table_schema: String = row.get("table_schema");
        let table_name: String = row.get("table_name");
        let force_rls: bool = row.get("relforcerowsecurity");
        result.insert(qualified_name(&table_schema, &table_name), force_rls);
    }

    Ok(result)
}

async fn introspect_all_policies(
    connection: &PgConnection,
    target_schemas: &[String],
) -> Result<BTreeMap<String, Vec<Policy>>> {
    let rows = sqlx::query(
        r#"
        SELECT
            n.nspname AS table_schema,
            c.relname AS table_name,
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
        WHERE n.nspname = ANY($1::text[])
          AND c.relkind IN ('r', 'p')
          AND c.relispartition = false
        "#,
    )
    .bind(target_schemas)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch policies: {e}")))?;

    let mut result: BTreeMap<String, Vec<Policy>> = BTreeMap::new();
    for row in rows {
        let table_schema: String = row.get("table_schema");
        let table_name: String = row.get("table_name");
        let name: String = row.get("name");
        let command: i8 = row.get::<i8, _>("command");
        let roles: Vec<String> = row.get("roles");
        let using_expr: Option<String> = row.get("using_expr");
        let check_expr: Option<String> = row.get("check_expr");

        let roles = if roles.is_empty() {
            vec!["public".to_string()]
        } else {
            roles
        };

        result
            .entry(qualified_name(&table_schema, &table_name))
            .or_default()
            .push(Policy {
                name,
                table: table_name,
                table_schema,
                command: map_policy_command(pg_char(command)),
                roles,
                using_expr,
                check_expr,
            });
    }

    Ok(result)
}

fn map_policy_command(cmd: char) -> PolicyCommand {
    match cmd {
        '*' => PolicyCommand::All,
        'r' => PolicyCommand::Select,
        'a' => PolicyCommand::Insert,
        'w' => PolicyCommand::Update,
        'd' => PolicyCommand::Delete,
        _ => panic!("Unknown policy command code from PostgreSQL: '{cmd}'"),
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
            r.rolname as owner,
            p.proargmodes as arg_modes
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

        let volatility = match pg_char(volatility_char) {
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

        let arg_modes_raw: Option<Vec<i8>> = row.get("arg_modes");
        let arguments = if let Some(modes) = arg_modes_raw {
            arguments
                .into_iter()
                .zip(modes.into_iter())
                .map(|(mut arg, mode)| {
                    arg.mode = match pg_char(mode) {
                        'o' => crate::model::ArgMode::Out,
                        'b' => crate::model::ArgMode::InOut,
                        'v' => crate::model::ArgMode::Variadic,
                        _ => arg.mode,
                    };
                    arg
                })
                .collect()
        } else {
            arguments
        };

        let config_params_raw: Option<Vec<String>> = row.get("config_params");
        let config_params: Vec<(String, String)> = config_params_raw
            .unwrap_or_default()
            .into_iter()
            .map(|param| {
                let parts: Vec<&str> = param.splitn(2, '=').collect();
                if parts.len() == 2 {
                    let key = parts[0].to_string();
                    let value = normalize_proconfig_value(parts[1]);
                    Ok((key, value))
                } else {
                    Err(SchemaError::DatabaseError(format!(
                        "Malformed config parameter in function {schema}.{name}: '{param}'"
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
            grants: Vec::new(),
            // TODO: read function comment from pg_description
            comment: None,
        };

        let key = qualified_name(&schema, &func.signature());
        functions.insert(key, func);
    }

    Ok(functions)
}

/// Splits a function argument list on commas, skipping commas inside
/// parentheses, square brackets, or single-quoted strings.
fn split_arguments(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut in_quotes = false;
    let mut start = 0;
    for (i, ch) in s.char_indices() {
        if in_quotes {
            if ch == '\'' {
                in_quotes = false;
            }
            continue;
        }
        match ch {
            '\'' => in_quotes = true,
            '(' | '[' => depth += 1,
            ')' | ']' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(&s[start..]);
    parts
}

/// Finds the byte offset of ` DEFAULT ` in an argument string,
/// skipping occurrences inside single-quoted strings.
fn find_default_keyword(arg: &str) -> Option<usize> {
    let upper = arg.to_uppercase();
    let keyword = " DEFAULT ";
    let mut in_quotes = false;
    for (i, ch) in arg.char_indices() {
        if ch == '\'' {
            in_quotes = !in_quotes;
            continue;
        }
        if !in_quotes && i + keyword.len() <= upper.len() && &upper[i..i + keyword.len()] == keyword
        {
            return Some(i);
        }
    }
    None
}

fn parse_function_arguments(args_str: &str) -> Vec<FunctionArg> {
    if args_str.is_empty() {
        return Vec::new();
    }

    split_arguments(args_str)
        .iter()
        .map(|arg| {
            let arg = arg.trim();

            let (arg_without_default, default) = if let Some(idx) = find_default_keyword(arg) {
                let keyword = " DEFAULT ";
                let default_value = arg[idx + keyword.len()..].trim().to_string();
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

async fn fetch_views(
    connection: &PgConnection,
    target_schemas: &[String],
    include_extension_objects: bool,
    query: &str,
    name_column: &str,
    materialized: bool,
) -> Result<Vec<View>> {
    let rows = sqlx::query(query)
        .bind(target_schemas)
        .bind(include_extension_objects)
        .fetch_all(connection.pool())
        .await
        .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch views: {e}")))?;

    let mut result = Vec::new();
    for row in rows {
        let schema: String = row.get("schemaname");
        let name: String = row.get(name_column);
        let definition: String = row.get("definition");
        let owner: String = row.get("owner");

        result.push(View {
            name,
            schema,
            query: normalize_sql_whitespace(definition.trim_end_matches(';')),
            materialized,
            owner: Some(owner),
            grants: Vec::new(),
            // TODO: read view comment from pg_description
            comment: None,
        });
    }
    Ok(result)
}

async fn introspect_views(
    connection: &PgConnection,
    target_schemas: &[String],
    include_extension_objects: bool,
) -> Result<BTreeMap<String, View>> {
    let mut views = BTreeMap::new();

    let regular_views = fetch_views(
        connection,
        target_schemas,
        include_extension_objects,
        r#"
        SELECT v.schemaname, v.viewname, v.definition, r.rolname AS owner
        FROM pg_views v
        JOIN pg_class c ON c.relname = v.viewname
        JOIN pg_namespace n ON c.relnamespace = n.oid AND n.nspname = v.schemaname
        JOIN pg_roles r ON c.relowner = r.oid
        WHERE v.schemaname = ANY($1::text[])
          AND ($2::boolean OR NOT EXISTS (
              SELECT 1 FROM pg_depend d
              WHERE d.objid = c.oid
              AND d.deptype = 'e'
          ))
        "#,
        "viewname",
        false,
    )
    .await?;

    for view in regular_views {
        views.insert(qualified_name(&view.schema, &view.name), view);
    }

    let materialized_views = fetch_views(
        connection,
        target_schemas,
        include_extension_objects,
        r#"
        SELECT v.schemaname, v.matviewname, v.definition, r.rolname AS owner
        FROM pg_matviews v
        JOIN pg_class c ON c.relname = v.matviewname
        JOIN pg_namespace n ON c.relnamespace = n.oid AND n.nspname = v.schemaname
        JOIN pg_roles r ON c.relowner = r.oid
        WHERE v.schemaname = ANY($1::text[])
          AND ($2::boolean OR NOT EXISTS (
              SELECT 1 FROM pg_depend d
              WHERE d.objid = c.oid
              AND d.deptype = 'e'
          ))
        "#,
        "matviewname",
        true,
    )
    .await?;

    for view in materialized_views {
        views.insert(qualified_name(&view.schema, &view.name), view);
    }

    Ok(views)
}

const TRIGGER_TYPE_ROW: i16 = 0x0001;
const TRIGGER_TYPE_BEFORE: i16 = 0x0002;
const TRIGGER_TYPE_INSERT: i16 = 0x0004;
const TRIGGER_TYPE_DELETE: i16 = 0x0008;
const TRIGGER_TYPE_UPDATE: i16 = 0x0010;
const TRIGGER_TYPE_TRUNCATE: i16 = 0x0020;
const TRIGGER_TYPE_INSTEAD: i16 = 0x0040;

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
          AND c.relispartition = false
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

        let timing = if tgtype & TRIGGER_TYPE_INSTEAD != 0 {
            TriggerTiming::InsteadOf
        } else if tgtype & TRIGGER_TYPE_BEFORE != 0 {
            TriggerTiming::Before
        } else {
            TriggerTiming::After
        };

        let for_each_row = tgtype & TRIGGER_TYPE_ROW != 0;

        let mut events = Vec::new();
        if tgtype & TRIGGER_TYPE_INSERT != 0 {
            events.push(TriggerEvent::Insert);
        }
        if tgtype & TRIGGER_TYPE_UPDATE != 0 {
            events.push(TriggerEvent::Update);
        }
        if tgtype & TRIGGER_TYPE_DELETE != 0 {
            events.push(TriggerEvent::Delete);
        }
        if tgtype & TRIGGER_TYPE_TRUNCATE != 0 {
            events.push(TriggerEvent::Truncate);
        }

        let when_clause =
            extract_when_clause(&trigger_def).map(|w| crate::util::normalize_type_casts(&w));

        let enabled = match pg_char(tgenabled) {
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
            // TODO: read trigger comment from pg_description
            comment: None,
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
            a.attname as owned_column,
            r.rolname as owner
        FROM pg_sequences s
        JOIN pg_namespace n ON n.nspname = s.schemaname
        LEFT JOIN pg_class seq_class ON seq_class.relname = s.sequencename
            AND seq_class.relnamespace = n.oid
            AND seq_class.relkind = 'S'
        LEFT JOIN pg_roles r ON seq_class.relowner = r.oid
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
        let owner: Option<String> = row.get("owner");

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

        let key = qualified_name(&schema, &name);
        sequences.insert(
            key,
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
                owner,
                grants: Vec::new(),
                // TODO: read sequence comment from pg_description
                comment: None,
            },
        );
    }

    Ok(sequences)
}

fn accumulate_grant(
    map: &mut BTreeMap<String, BTreeMap<(String, bool), BTreeSet<Privilege>>>,
    key: String,
    grantee: String,
    is_grantable: bool,
    privilege: Privilege,
) {
    map.entry(key)
        .or_default()
        .entry((grantee, is_grantable))
        .or_default()
        .insert(privilege);
}

fn collect_grants(
    accumulated: BTreeMap<String, BTreeMap<(String, bool), BTreeSet<Privilege>>>,
) -> BTreeMap<String, Vec<Grant>> {
    accumulated
        .into_iter()
        .map(|(key, grants_map)| {
            let mut grants: Vec<Grant> = grants_map
                .into_iter()
                .map(|((grantee, with_grant_option), privileges)| Grant {
                    grantee,
                    privileges,
                    with_grant_option,
                })
                .collect();
            grants.sort();
            (key, grants)
        })
        .collect()
}

/// Query and collect grants from a SQL query. The query must SELECT columns:
/// `grantee`, `privilege_type`, `is_grantable`. `extract_key` builds the
/// object key from each row.
async fn query_grants<F>(
    connection: &PgConnection,
    target_schemas: &[String],
    sql: &str,
    context: &str,
    extract_key: F,
) -> Result<BTreeMap<String, Vec<Grant>>>
where
    F: Fn(&sqlx::postgres::PgRow) -> String,
{
    let rows = sqlx::query(sql)
        .bind(target_schemas)
        .fetch_all(connection.pool())
        .await
        .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch {context}: {e}")))?;

    let mut grants_by_object: BTreeMap<String, BTreeMap<(String, bool), BTreeSet<Privilege>>> =
        BTreeMap::new();

    for row in rows {
        let key = extract_key(&row);
        let grantee: String = row.get("grantee");
        let privilege_type: String = row.get("privilege_type");
        let is_grantable: bool = row.get("is_grantable");

        if let Some(privilege) = privilege_from_pg_string(&privilege_type) {
            accumulate_grant(&mut grants_by_object, key, grantee, is_grantable, privilege);
        }
    }

    Ok(collect_grants(grants_by_object))
}

fn privilege_from_pg_string(s: &str) -> Option<Privilege> {
    match s {
        "SELECT" => Some(Privilege::Select),
        "INSERT" => Some(Privilege::Insert),
        "UPDATE" => Some(Privilege::Update),
        "DELETE" => Some(Privilege::Delete),
        "TRUNCATE" => Some(Privilege::Truncate),
        "REFERENCES" => Some(Privilege::References),
        "TRIGGER" => Some(Privilege::Trigger),
        "USAGE" => Some(Privilege::Usage),
        "EXECUTE" => Some(Privilege::Execute),
        "CREATE" => Some(Privilege::Create),
        _ => None,
    }
}

async fn introspect_table_view_grants(
    connection: &PgConnection,
    target_schemas: &[String],
) -> Result<BTreeMap<String, Vec<Grant>>> {
    query_grants(
        connection,
        target_schemas,
        r#"
        SELECT
            n.nspname AS schema_name,
            c.relname AS object_name,
            CASE
                WHEN acl.grantee = 0 THEN 'PUBLIC'
                ELSE pg_get_userbyid(acl.grantee)
            END AS grantee,
            acl.privilege_type AS privilege_type,
            acl.is_grantable AS is_grantable
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        CROSS JOIN LATERAL aclexplode(c.relacl) acl
        WHERE c.relkind IN ('r', 'v', 'm')
          AND n.nspname = ANY($1::text[])
          AND c.relacl IS NOT NULL
          AND acl.grantee != c.relowner
        "#,
        "table/view grants",
        |row| {
            let schema_name: String = row.get("schema_name");
            let object_name: String = row.get("object_name");
            qualified_name(&schema_name, &object_name)
        },
    )
    .await
}

async fn introspect_sequence_grants(
    connection: &PgConnection,
    target_schemas: &[String],
) -> Result<BTreeMap<String, Vec<Grant>>> {
    query_grants(
        connection,
        target_schemas,
        r#"
        SELECT
            n.nspname AS schema_name,
            c.relname AS object_name,
            CASE
                WHEN acl.grantee = 0 THEN 'PUBLIC'
                ELSE pg_get_userbyid(acl.grantee)
            END AS grantee,
            acl.privilege_type AS privilege_type,
            acl.is_grantable AS is_grantable
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        CROSS JOIN LATERAL aclexplode(c.relacl) acl
        WHERE c.relkind = 'S'
          AND n.nspname = ANY($1::text[])
          AND c.relacl IS NOT NULL
          AND acl.grantee != c.relowner
        "#,
        "sequence grants",
        |row| {
            let schema_name: String = row.get("schema_name");
            let object_name: String = row.get("object_name");
            qualified_name(&schema_name, &object_name)
        },
    )
    .await
}

async fn introspect_function_grants(
    connection: &PgConnection,
    target_schemas: &[String],
) -> Result<BTreeMap<String, Vec<Grant>>> {
    query_grants(
        connection,
        target_schemas,
        r#"
        SELECT
            n.nspname AS schema_name,
            p.proname AS function_name,
            pg_get_function_identity_arguments(p.oid) AS args,
            CASE
                WHEN acl.grantee = 0 THEN 'PUBLIC'
                ELSE pg_get_userbyid(acl.grantee)
            END AS grantee,
            acl.privilege_type AS privilege_type,
            acl.is_grantable AS is_grantable
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        CROSS JOIN LATERAL aclexplode(p.proacl) AS acl
        WHERE n.nspname = ANY($1::text[])
          AND p.proacl IS NOT NULL
          AND acl.grantee != p.proowner
        "#,
        "function grants",
        |row| {
            let schema_name: String = row.get("schema_name");
            let function_name: String = row.get("function_name");
            let args_str: String = row.get("args");
            let parsed_args = parse_function_arguments(&args_str);
            let type_signature = parsed_args
                .iter()
                .map(|arg| crate::model::normalize_pg_type(&arg.data_type))
                .collect::<Vec<_>>()
                .join(", ");
            format!("{schema_name}.{function_name}({type_signature})")
        },
    )
    .await
}

async fn introspect_schema_grants(
    connection: &PgConnection,
    target_schemas: &[String],
) -> Result<BTreeMap<String, Vec<Grant>>> {
    query_grants(
        connection,
        target_schemas,
        r#"
        SELECT
            n.nspname AS schema_name,
            CASE
                WHEN acl.grantee = 0 THEN 'PUBLIC'
                ELSE pg_get_userbyid(acl.grantee)
            END AS grantee,
            acl.privilege_type AS privilege_type,
            acl.is_grantable AS is_grantable
        FROM pg_namespace n
        CROSS JOIN LATERAL aclexplode(n.nspacl) AS acl
        WHERE n.nspname = ANY($1::text[])
          AND n.nspacl IS NOT NULL
          AND acl.grantee != n.nspowner
        "#,
        "schema grants",
        |row| row.get("schema_name"),
    )
    .await
}

async fn introspect_type_grants(
    connection: &PgConnection,
    target_schemas: &[String],
) -> Result<BTreeMap<String, Vec<Grant>>> {
    query_grants(
        connection,
        target_schemas,
        r#"
        SELECT
            n.nspname AS schema_name,
            t.typname AS type_name,
            CASE
                WHEN acl.grantee = 0 THEN 'PUBLIC'
                ELSE pg_get_userbyid(acl.grantee)
            END AS grantee,
            acl.privilege_type AS privilege_type,
            acl.is_grantable AS is_grantable
        FROM pg_type t
        JOIN pg_namespace n ON t.typnamespace = n.oid
        CROSS JOIN LATERAL aclexplode(t.typacl) AS acl
        WHERE n.nspname = ANY($1::text[])
          AND t.typtype IN ('e', 'd')
          AND t.typacl IS NOT NULL
          AND acl.grantee != t.typowner
        "#,
        "type grants",
        |row| {
            let schema_name: String = row.get("schema_name");
            let type_name: String = row.get("type_name");
            qualified_name(&schema_name, &type_name)
        },
    )
    .await
}

async fn introspect_default_privileges(
    connection: &PgConnection,
    target_schemas: &[String],
) -> Result<Vec<DefaultPrivilege>> {
    let rows = sqlx::query(
        r#"
        SELECT
            r.rolname AS target_role,
            CASE WHEN d.defaclnamespace = 0 THEN NULL
                 ELSE n.nspname
            END AS schema_name,
            d.defaclobjtype AS object_type,
            CASE
                WHEN acl.grantee = 0 THEN 'PUBLIC'
                ELSE pg_get_userbyid(acl.grantee)
            END AS grantee,
            acl.privilege_type AS privilege_type,
            acl.is_grantable AS with_grant_option
        FROM pg_default_acl d
        JOIN pg_roles r ON d.defaclrole = r.oid
        LEFT JOIN pg_namespace n ON d.defaclnamespace = n.oid
        CROSS JOIN LATERAL aclexplode(d.defaclacl) AS acl
        WHERE (d.defaclnamespace = 0 OR n.nspname = ANY($1))
        ORDER BY r.rolname, n.nspname, d.defaclobjtype, acl.grantee
        "#,
    )
    .bind(target_schemas)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch default privileges: {e}")))?;

    // Key: (target_role, schema, object_type, grantee, with_grant_option)
    #[allow(clippy::type_complexity)]
    let mut grouped: BTreeMap<
        (
            String,
            Option<String>,
            DefaultPrivilegeObjectType,
            String,
            bool,
        ),
        BTreeSet<Privilege>,
    > = BTreeMap::new();

    for row in rows {
        let target_role: String = row.get("target_role");
        let schema_name: Option<String> = row.get("schema_name");
        let object_type_char: i8 = row.get("object_type");
        let grantee: String = row.get("grantee");
        let privilege_type: String = row.get("privilege_type");
        let with_grant_option: bool = row.get("with_grant_option");

        let object_type = match pg_char(object_type_char) {
            'r' => DefaultPrivilegeObjectType::Tables,
            'S' => DefaultPrivilegeObjectType::Sequences,
            'f' => DefaultPrivilegeObjectType::Functions,
            'T' => DefaultPrivilegeObjectType::Types,
            'n' => DefaultPrivilegeObjectType::Schemas,
            _ => continue,
        };

        if let Some(privilege) = privilege_from_pg_string(&privilege_type) {
            grouped
                .entry((
                    target_role,
                    schema_name,
                    object_type,
                    grantee,
                    with_grant_option,
                ))
                .or_default()
                .insert(privilege);
        }
    }

    let mut result = Vec::new();
    for ((target_role, schema, object_type, grantee, with_grant_option), privileges) in grouped {
        result.push(DefaultPrivilege {
            target_role,
            schema,
            object_type,
            grantee,
            privileges,
            with_grant_option,
        });
    }

    result.sort();
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_arguments_handles_commas_in_types() {
        let args = split_arguments("p_amount numeric(10,2), p_name text");
        assert_eq!(args, vec!["p_amount numeric(10,2)", " p_name text"]);
    }

    #[test]
    fn split_arguments_handles_commas_in_quoted_defaults() {
        let args = split_arguments("p_list text DEFAULT 'a,b,c'::text, p_id uuid");
        assert_eq!(
            args,
            vec!["p_list text DEFAULT 'a,b,c'::text", " p_id uuid"]
        );
    }

    #[test]
    fn split_arguments_handles_commas_in_array_defaults() {
        let args = split_arguments("p_ids integer[] DEFAULT ARRAY[1,2,3], p_name text");
        assert_eq!(
            args,
            vec!["p_ids integer[] DEFAULT ARRAY[1,2,3]", " p_name text"]
        );
    }

    #[test]
    fn find_default_keyword_skips_quoted_occurrences() {
        assert_eq!(
            find_default_keyword("p_val text DEFAULT 'USE DEFAULT'::text"),
            Some(10)
        );
    }

    #[test]
    fn find_default_keyword_returns_none_when_absent() {
        assert_eq!(find_default_keyword("p_name text"), None);
    }

    #[test]
    fn parse_function_arguments_with_commas_in_type() {
        let args = parse_function_arguments("p_amount numeric(10,2), p_name text");
        assert_eq!(args.len(), 2);
        assert_eq!(args[0].name, Some("p_amount".to_string()));
        assert_eq!(args[0].data_type, "numeric(10,2)");
        assert_eq!(args[1].name, Some("p_name".to_string()));
    }

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

    #[test]
    fn parse_function_arguments_preserves_uppercase_default() {
        let args = parse_function_arguments("p_role text DEFAULT 'ADMIN'::text");
        assert_eq!(args.len(), 1);
        assert_eq!(args[0].default.as_deref(), Some("'ADMIN'::text"));
    }

    #[test]
    fn privilege_from_pg_string_maps_all_privileges() {
        assert_eq!(privilege_from_pg_string("SELECT"), Some(Privilege::Select));
        assert_eq!(privilege_from_pg_string("INSERT"), Some(Privilege::Insert));
        assert_eq!(privilege_from_pg_string("UPDATE"), Some(Privilege::Update));
        assert_eq!(privilege_from_pg_string("DELETE"), Some(Privilege::Delete));
        assert_eq!(
            privilege_from_pg_string("TRUNCATE"),
            Some(Privilege::Truncate)
        );
        assert_eq!(
            privilege_from_pg_string("REFERENCES"),
            Some(Privilege::References)
        );
        assert_eq!(
            privilege_from_pg_string("TRIGGER"),
            Some(Privilege::Trigger)
        );
        assert_eq!(privilege_from_pg_string("USAGE"), Some(Privilege::Usage));
        assert_eq!(
            privilege_from_pg_string("EXECUTE"),
            Some(Privilege::Execute)
        );
        assert_eq!(privilege_from_pg_string("CREATE"), Some(Privilege::Create));
        assert_eq!(privilege_from_pg_string("UNKNOWN"), None);
    }

    #[test]
    fn normalize_proconfig_empty_string() {
        assert_eq!(normalize_proconfig_value(r#""""#), "''");
    }

    #[test]
    fn normalize_proconfig_quoted_value() {
        assert_eq!(
            normalize_proconfig_value(r#""pg_temp, public""#),
            "'pg_temp, public'"
        );
    }

    #[test]
    fn normalize_proconfig_unquoted_value() {
        assert_eq!(normalize_proconfig_value("off"), "off");
    }

    #[test]
    fn normalize_proconfig_single_quoted_passthrough() {
        assert_eq!(normalize_proconfig_value("'64MB'"), "'64MB'");
    }
}
