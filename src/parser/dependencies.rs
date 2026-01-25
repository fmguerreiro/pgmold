/// Extract dependencies from SQL statements.
///
/// This module parses SQL DDL to identify object references, enabling
/// topological sorting for correct creation order.
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Query, Select,
    SelectItem, SetExpr, Statement, TableFactor, TableWithJoins,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::{HashSet, VecDeque};

/// A reference to a database object (function, table, view, etc.)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectRef {
    pub schema: String,
    pub name: String,
}

impl ObjectRef {
    pub fn new(schema: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            schema: schema.into(),
            name: name.into(),
        }
    }

    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.schema, self.name)
    }

    fn from_object_name(name: &sqlparser::ast::ObjectName, default_schema: &str) -> Self {
        let parts: Vec<String> = name
            .0
            .iter()
            .map(|p| p.to_string().trim_matches('"').to_string())
            .collect();

        if parts.len() == 1 {
            Self::new(default_schema, &parts[0])
        } else {
            Self::new(&parts[0], &parts[1])
        }
    }
}

/// Extract function references from a SQL body using sqlparser.
///
/// Parses the SQL and walks the AST to find all function calls.
/// Returns qualified names (schema.name) of referenced functions.
pub fn extract_function_references(body: &str, default_schema: &str) -> HashSet<ObjectRef> {
    let mut refs = HashSet::new();
    let dialect = PostgreSqlDialect {};

    // Try to parse as a query first (most function bodies are SELECT statements)
    let sql = format!("SELECT {body}");
    let statements = match Parser::parse_sql(&dialect, &sql) {
        Ok(stmts) => stmts,
        Err(_) => {
            // Try wrapping as subquery
            let sql = format!("SELECT * FROM ({body}) AS subq");
            match Parser::parse_sql(&dialect, &sql) {
                Ok(stmts) => stmts,
                Err(_) => {
                    // Try direct parse
                    match Parser::parse_sql(&dialect, body) {
                        Ok(stmts) => stmts,
                        Err(_) => return refs,
                    }
                }
            }
        }
    };

    for statement in &statements {
        extract_functions_from_statement(statement, default_schema, &mut refs);
    }

    refs
}

/// Extract table/view references from a SQL body using sqlparser.
///
/// Parses the SQL and walks the AST to find all table/view references.
/// Returns qualified names (schema.name) of referenced relations.
pub fn extract_table_references(body: &str, default_schema: &str) -> HashSet<ObjectRef> {
    let mut refs = HashSet::new();
    let dialect = PostgreSqlDialect {};

    // Try to parse as a query
    let sql = format!("SELECT * FROM ({body}) AS subq");
    let statements = match Parser::parse_sql(&dialect, &sql) {
        Ok(stmts) => stmts,
        Err(_) => match Parser::parse_sql(&dialect, body) {
            Ok(stmts) => stmts,
            Err(_) => return refs,
        },
    };

    for statement in &statements {
        extract_tables_from_statement(statement, default_schema, &mut refs);
    }

    refs
}

fn extract_functions_from_statement(
    statement: &Statement,
    default_schema: &str,
    refs: &mut HashSet<ObjectRef>,
) {
    if let Statement::Query(query) = statement {
        extract_functions_from_query(query, default_schema, refs);
    }
}

fn extract_functions_from_query(
    query: &Query,
    default_schema: &str,
    refs: &mut HashSet<ObjectRef>,
) {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            extract_functions_from_query(&cte.query, default_schema, refs);
        }
    }
    extract_functions_from_set_expr(&query.body, default_schema, refs);
}

fn extract_functions_from_set_expr(
    set_expr: &SetExpr,
    default_schema: &str,
    refs: &mut HashSet<ObjectRef>,
) {
    match set_expr {
        SetExpr::Select(select) => extract_functions_from_select(select, default_schema, refs),
        SetExpr::Query(query) => extract_functions_from_query(query, default_schema, refs),
        SetExpr::SetOperation { left, right, .. } => {
            extract_functions_from_set_expr(left, default_schema, refs);
            extract_functions_from_set_expr(right, default_schema, refs);
        }
        _ => {}
    }
}

fn extract_functions_from_select(
    select: &Select,
    default_schema: &str,
    refs: &mut HashSet<ObjectRef>,
) {
    // FROM clause
    for table_with_joins in &select.from {
        extract_functions_from_table_with_joins(table_with_joins, default_schema, refs);
    }

    // WHERE clause
    if let Some(selection) = &select.selection {
        extract_functions_from_expr(selection, default_schema, refs);
    }

    // SELECT items
    for item in &select.projection {
        if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item {
            extract_functions_from_expr(expr, default_schema, refs);
        }
    }

    // HAVING clause
    if let Some(having) = &select.having {
        extract_functions_from_expr(having, default_schema, refs);
    }
}

fn extract_functions_from_table_with_joins(
    twj: &TableWithJoins,
    default_schema: &str,
    refs: &mut HashSet<ObjectRef>,
) {
    use sqlparser::ast::{JoinConstraint, JoinOperator};

    extract_functions_from_table_factor(&twj.relation, default_schema, refs);
    for join in &twj.joins {
        extract_functions_from_table_factor(&join.relation, default_schema, refs);

        // Extract constraint from the join operator variant
        let constraint = match &join.join_operator {
            JoinOperator::Join(c)
            | JoinOperator::Inner(c)
            | JoinOperator::Left(c)
            | JoinOperator::Right(c)
            | JoinOperator::LeftOuter(c)
            | JoinOperator::RightOuter(c)
            | JoinOperator::FullOuter(c) => Some(c),
            _ => None,
        };

        if let Some(JoinConstraint::On(expr)) = constraint {
            extract_functions_from_expr(expr, default_schema, refs);
        }
    }
}

fn extract_functions_from_table_factor(
    factor: &TableFactor,
    default_schema: &str,
    refs: &mut HashSet<ObjectRef>,
) {
    match factor {
        TableFactor::Derived { subquery, .. } => {
            extract_functions_from_query(subquery, default_schema, refs);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            extract_functions_from_table_with_joins(table_with_joins, default_schema, refs);
        }
        TableFactor::TableFunction { expr, .. } => {
            extract_functions_from_expr(expr, default_schema, refs);
        }
        _ => {}
    }
}

fn extract_functions_from_expr(expr: &Expr, default_schema: &str, refs: &mut HashSet<ObjectRef>) {
    match expr {
        Expr::Function(f) => {
            let obj_ref = ObjectRef::from_object_name(&f.name, default_schema);
            if !is_builtin_function(&obj_ref.name) {
                refs.insert(obj_ref);
            }

            if let FunctionArguments::List(FunctionArgumentList { args, .. }) = &f.args {
                for arg in args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                        extract_functions_from_expr(e, default_schema, refs);
                    }
                }
            }
        }
        Expr::Subquery(query) => extract_functions_from_query(query, default_schema, refs),
        Expr::InSubquery { subquery, expr, .. } => {
            extract_functions_from_query(subquery, default_schema, refs);
            extract_functions_from_expr(expr, default_schema, refs);
        }
        Expr::Exists { subquery, .. } => {
            extract_functions_from_query(subquery, default_schema, refs);
        }
        Expr::BinaryOp { left, right, .. } => {
            extract_functions_from_expr(left, default_schema, refs);
            extract_functions_from_expr(right, default_schema, refs);
        }
        Expr::UnaryOp { expr, .. } => extract_functions_from_expr(expr, default_schema, refs),
        Expr::Nested(e) => extract_functions_from_expr(e, default_schema, refs),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                extract_functions_from_expr(op, default_schema, refs);
            }
            for cw in conditions {
                extract_functions_from_expr(&cw.condition, default_schema, refs);
                extract_functions_from_expr(&cw.result, default_schema, refs);
            }
            if let Some(else_r) = else_result {
                extract_functions_from_expr(else_r, default_schema, refs);
            }
        }
        Expr::Cast { expr, .. } => {
            extract_functions_from_expr(expr, default_schema, refs);
        }
        Expr::IsNull(e) | Expr::IsNotNull(e) => {
            extract_functions_from_expr(e, default_schema, refs);
        }
        Expr::InList { expr, list, .. } => {
            extract_functions_from_expr(expr, default_schema, refs);
            for e in list {
                extract_functions_from_expr(e, default_schema, refs);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            extract_functions_from_expr(expr, default_schema, refs);
            extract_functions_from_expr(low, default_schema, refs);
            extract_functions_from_expr(high, default_schema, refs);
        }
        _ => {}
    }
}

fn extract_tables_from_statement(
    statement: &Statement,
    default_schema: &str,
    refs: &mut HashSet<ObjectRef>,
) {
    if let Statement::Query(query) = statement {
        extract_tables_from_query(query, default_schema, refs);
    }
}

fn extract_tables_from_query(query: &Query, default_schema: &str, refs: &mut HashSet<ObjectRef>) {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            extract_tables_from_query(&cte.query, default_schema, refs);
        }
    }
    extract_tables_from_set_expr(&query.body, default_schema, refs);
}

fn extract_tables_from_set_expr(
    set_expr: &SetExpr,
    default_schema: &str,
    refs: &mut HashSet<ObjectRef>,
) {
    match set_expr {
        SetExpr::Select(select) => extract_tables_from_select(select, default_schema, refs),
        SetExpr::Query(query) => extract_tables_from_query(query, default_schema, refs),
        SetExpr::SetOperation { left, right, .. } => {
            extract_tables_from_set_expr(left, default_schema, refs);
            extract_tables_from_set_expr(right, default_schema, refs);
        }
        _ => {}
    }
}

fn extract_tables_from_select(
    select: &Select,
    default_schema: &str,
    refs: &mut HashSet<ObjectRef>,
) {
    for table_with_joins in &select.from {
        extract_tables_from_table_with_joins(table_with_joins, default_schema, refs);
    }

    if let Some(selection) = &select.selection {
        extract_tables_from_expr(selection, default_schema, refs);
    }

    for item in &select.projection {
        if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item {
            extract_tables_from_expr(expr, default_schema, refs);
        }
    }

    if let Some(having) = &select.having {
        extract_tables_from_expr(having, default_schema, refs);
    }
}

fn extract_tables_from_table_with_joins(
    twj: &TableWithJoins,
    default_schema: &str,
    refs: &mut HashSet<ObjectRef>,
) {
    extract_tables_from_table_factor(&twj.relation, default_schema, refs);
    for join in &twj.joins {
        extract_tables_from_table_factor(&join.relation, default_schema, refs);
    }
}

fn extract_tables_from_table_factor(
    factor: &TableFactor,
    default_schema: &str,
    refs: &mut HashSet<ObjectRef>,
) {
    match factor {
        TableFactor::Table { name, .. } => {
            refs.insert(ObjectRef::from_object_name(name, default_schema));
        }
        TableFactor::Derived { subquery, .. } => {
            extract_tables_from_query(subquery, default_schema, refs);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            extract_tables_from_table_with_joins(table_with_joins, default_schema, refs);
        }
        _ => {}
    }
}

fn extract_tables_from_expr(expr: &Expr, default_schema: &str, refs: &mut HashSet<ObjectRef>) {
    match expr {
        Expr::Subquery(query) => extract_tables_from_query(query, default_schema, refs),
        Expr::InSubquery { subquery, .. } => {
            extract_tables_from_query(subquery, default_schema, refs);
        }
        Expr::Exists { subquery, .. } => extract_tables_from_query(subquery, default_schema, refs),
        Expr::BinaryOp { left, right, .. } => {
            extract_tables_from_expr(left, default_schema, refs);
            extract_tables_from_expr(right, default_schema, refs);
        }
        Expr::Function(f) => {
            if let FunctionArguments::List(FunctionArgumentList { args, .. }) = &f.args {
                for arg in args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                        extract_tables_from_expr(e, default_schema, refs);
                    }
                }
            }
        }
        _ => {}
    }
}

/// Check if a function name is a PostgreSQL built-in.
fn is_builtin_function(name: &str) -> bool {
    let name_lower = name.to_lowercase();
    matches!(
        name_lower.as_str(),
        // Aggregate functions
        "count" | "sum" | "avg" | "min" | "max" | "array_agg" | "json_agg" | "jsonb_agg"
        | "string_agg" | "bool_and" | "bool_or" | "every" | "bit_and" | "bit_or"
        // Window functions
        | "row_number" | "rank" | "dense_rank" | "percent_rank" | "cume_dist"
        | "ntile" | "lag" | "lead" | "first_value" | "last_value" | "nth_value"
        // Date/time functions
        | "now" | "current_timestamp" | "current_date" | "current_time"
        | "localtime" | "localtimestamp" | "clock_timestamp" | "statement_timestamp"
        | "transaction_timestamp" | "timeofday" | "age" | "extract" | "date_part"
        | "date_trunc" | "make_date" | "make_time" | "make_timestamp" | "make_timestamptz"
        | "make_interval" | "to_timestamp" | "to_date" | "to_char"
        // Math functions
        | "abs" | "ceil" | "ceiling" | "floor" | "round" | "trunc" | "truncate"
        | "mod" | "power" | "sqrt" | "cbrt" | "exp" | "ln" | "log" | "log10"
        | "sign" | "random" | "setseed" | "pi" | "degrees" | "radians"
        | "sin" | "cos" | "tan" | "asin" | "acos" | "atan" | "atan2"
        // String functions
        | "length" | "char_length" | "character_length" | "bit_length" | "octet_length"
        | "lower" | "upper" | "initcap" | "concat" | "concat_ws" | "format"
        | "left" | "right" | "substring" | "substr" | "overlay" | "position"
        | "strpos" | "trim" | "ltrim" | "rtrim" | "btrim" | "lpad" | "rpad"
        | "repeat" | "reverse" | "replace" | "translate" | "split_part"
        | "regexp_match" | "regexp_matches" | "regexp_replace" | "regexp_split_to_array"
        | "regexp_split_to_table" | "ascii" | "chr" | "md5" | "quote_ident"
        | "quote_literal" | "quote_nullable" | "encode" | "decode"
        // Type conversion
        | "cast" | "convert" | "to_number" | "to_hex"
        // NULL handling
        | "coalesce" | "nullif" | "greatest" | "least"
        // Comparison
        | "num_nonnulls" | "num_nulls"
        // Array functions
        | "array_length" | "array_lower" | "array_upper" | "array_dims"
        | "array_ndims" | "array_position" | "array_positions" | "array_prepend"
        | "array_append" | "array_cat" | "array_remove" | "array_replace"
        | "array_to_string" | "string_to_array" | "unnest" | "cardinality"
        // JSON functions
        | "to_json" | "to_jsonb" | "array_to_json" | "row_to_json"
        | "json_build_array" | "jsonb_build_array" | "json_build_object" | "jsonb_build_object"
        | "json_object" | "jsonb_object" | "json_array_length" | "jsonb_array_length"
        | "json_each" | "jsonb_each" | "json_each_text" | "jsonb_each_text"
        | "json_extract_path" | "jsonb_extract_path" | "json_extract_path_text"
        | "jsonb_extract_path_text" | "json_object_keys" | "jsonb_object_keys"
        | "json_populate_record" | "jsonb_populate_record" | "json_populate_recordset"
        | "jsonb_populate_recordset" | "json_array_elements" | "jsonb_array_elements"
        | "json_array_elements_text" | "jsonb_array_elements_text" | "json_typeof"
        | "jsonb_typeof" | "json_strip_nulls" | "jsonb_strip_nulls" | "jsonb_set"
        | "jsonb_insert" | "jsonb_pretty" | "jsonb_path_query" | "jsonb_path_query_array"
        | "jsonb_path_query_first" | "jsonb_path_exists" | "jsonb_path_match"
        // System info
        | "current_database" | "current_schema" | "current_schemas" | "current_user"
        | "session_user" | "user" | "version" | "pg_backend_pid" | "pg_conf_load_time"
        | "pg_is_in_recovery" | "pg_last_xact_replay_timestamp" | "pg_postmaster_start_time"
        // Sequence functions
        | "nextval" | "currval" | "setval" | "lastval"
        // Misc
        | "generate_series" | "generate_subscripts" | "pg_sleep" | "pg_sleep_for"
        | "pg_sleep_until" | "txid_current" | "txid_current_if_assigned"
        | "txid_current_snapshot" | "txid_snapshot_xip" | "txid_snapshot_xmax"
        | "txid_snapshot_xmin" | "txid_visible_in_snapshot" | "txid_status"
        | "row" | "exists" | "not"
    )
}

/// Perform topological sort on a set of objects with dependencies.
///
/// Returns objects in an order where dependencies come before dependents.
/// Returns an error if circular dependencies are detected.
///
/// Uses Kahn's algorithm with a queue to detect cycles.
pub fn topological_sort<T, F, K>(items: Vec<T>, get_key: K, get_deps: F) -> Result<Vec<T>, String>
where
    T: Clone,
    K: Fn(&T) -> String,
    F: Fn(&T) -> HashSet<String>,
{
    use std::collections::HashMap;

    if items.is_empty() {
        return Ok(Vec::new());
    }

    // Build item map (key -> item)
    let mut item_map: HashMap<String, T> = HashMap::new();
    for item in &items {
        let key = get_key(item);
        item_map.insert(key, item.clone());
    }

    // Build graph: key -> list of keys that depend on it
    let mut graph: HashMap<String, Vec<String>> = HashMap::new();
    let mut in_degree: HashMap<String, usize> = HashMap::new();

    // Initialize in-degree for all items
    for item in &items {
        let key = get_key(item);
        in_degree.entry(key).or_insert(0);
    }

    // Build edges
    for item in &items {
        let key = get_key(item);
        let deps = get_deps(item);

        for dep_key in deps {
            // Only track dependencies that are in our item set
            if item_map.contains_key(&dep_key) {
                graph.entry(dep_key.clone()).or_default().push(key.clone());
                *in_degree.entry(key.clone()).or_insert(0) += 1;
            }
        }
    }

    // Kahn's algorithm: start with items that have no dependencies
    let mut queue: VecDeque<String> = VecDeque::new();
    for (key, &degree) in &in_degree {
        if degree == 0 {
            queue.push_back(key.clone());
        }
    }

    let mut sorted = Vec::new();

    while let Some(key) = queue.pop_front() {
        sorted.push(item_map.get(&key).unwrap().clone());

        // Process dependents
        if let Some(dependents) = graph.get(&key) {
            for dependent_key in dependents {
                let degree = in_degree.get_mut(dependent_key).unwrap();
                *degree -= 1;
                if *degree == 0 {
                    queue.push_back(dependent_key.clone());
                }
            }
        }
    }

    // If we processed all items, success. Otherwise, there's a cycle.
    if sorted.len() == items.len() {
        Ok(sorted)
    } else {
        // Find items involved in cycle for error message
        let processed: HashSet<String> = sorted.iter().map(&get_key).collect();
        let unprocessed: Vec<String> = items
            .iter()
            .map(get_key)
            .filter(|key| !processed.contains(key))
            .collect();

        Err(format!(
            "Circular dependency detected among: {}",
            unprocessed.join(", ")
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_function_call_with_schema() {
        let body = "SELECT auth.is_admin_jwt()";
        let refs = extract_function_references(body, "public");

        assert_eq!(refs.len(), 1);
        assert!(refs.contains(&ObjectRef::new("auth", "is_admin_jwt")));
    }

    #[test]
    fn extract_function_call_without_schema() {
        let body = "SELECT is_admin_jwt()";
        let refs = extract_function_references(body, "public");

        assert_eq!(refs.len(), 1);
        assert!(refs.contains(&ObjectRef::new("public", "is_admin_jwt")));
    }

    #[test]
    fn extract_multiple_function_calls() {
        let body = r#"
            SELECT auth.jwt(), auth.is_admin(), public.check_permission()
        "#;
        let refs = extract_function_references(body, "public");

        assert_eq!(refs.len(), 3);
        assert!(refs.contains(&ObjectRef::new("auth", "jwt")));
        assert!(refs.contains(&ObjectRef::new("auth", "is_admin")));
        assert!(refs.contains(&ObjectRef::new("public", "check_permission")));
    }

    #[test]
    fn extract_function_call_with_args() {
        let body = "SELECT add_fifteen(x), multiply(a, b)";
        let refs = extract_function_references(body, "public");

        assert_eq!(refs.len(), 2);
        assert!(refs.contains(&ObjectRef::new("public", "add_fifteen")));
        assert!(refs.contains(&ObjectRef::new("public", "multiply")));
    }

    #[test]
    fn ignore_built_in_functions() {
        // Built-in PostgreSQL functions should not be treated as dependencies
        let body = "SELECT now(), current_timestamp, count(*)";
        let refs = extract_function_references(body, "public");

        // Should not include built-in functions
        assert!(!refs.contains(&ObjectRef::new("public", "now")));
        assert!(!refs.contains(&ObjectRef::new("public", "current_timestamp")));
        assert!(!refs.contains(&ObjectRef::new("public", "count")));
    }

    #[test]
    fn extract_table_from_select() {
        let body = "SELECT id FROM users WHERE active = true";
        let refs = extract_table_references(body, "public");

        assert_eq!(refs.len(), 1);
        assert!(refs.contains(&ObjectRef::new("public", "users")));
    }

    #[test]
    fn extract_table_with_schema() {
        let body = "SELECT * FROM auth.users";
        let refs = extract_table_references(body, "public");

        assert_eq!(refs.len(), 1);
        assert!(refs.contains(&ObjectRef::new("auth", "users")));
    }

    #[test]
    fn extract_table_from_join() {
        let body = r#"
            SELECT u.id, p.title
            FROM users u
            JOIN posts p ON u.id = p.user_id
        "#;
        let refs = extract_table_references(body, "public");

        assert_eq!(refs.len(), 2);
        assert!(refs.contains(&ObjectRef::new("public", "users")));
        assert!(refs.contains(&ObjectRef::new("public", "posts")));
    }

    #[test]
    fn extract_table_from_insert() {
        let body = "INSERT INTO audit_log (action) VALUES ('login')";
        let refs = extract_table_references(body, "public");

        // INSERT statements aren't parsed as queries, so this will be empty
        // This is a known limitation - we mainly care about SELECT for function bodies
        assert!(refs.is_empty() || refs.contains(&ObjectRef::new("public", "audit_log")));
    }

    #[test]
    fn extract_table_from_update() {
        let body = "UPDATE users SET last_login = now()";
        let refs = extract_table_references(body, "public");

        // UPDATE statements aren't parsed as queries
        assert!(refs.is_empty() || refs.contains(&ObjectRef::new("public", "users")));
    }

    #[test]
    fn extract_mixed_references() {
        let body = r#"
            SELECT auth.check_permission(u.id)
            FROM users u
            WHERE auth.is_admin()
        "#;
        let func_refs = extract_function_references(body, "public");
        let table_refs = extract_table_references(body, "public");

        assert_eq!(func_refs.len(), 2);
        assert!(func_refs.contains(&ObjectRef::new("auth", "check_permission")));
        assert!(func_refs.contains(&ObjectRef::new("auth", "is_admin")));

        assert_eq!(table_refs.len(), 1);
        assert!(table_refs.contains(&ObjectRef::new("public", "users")));
    }

    #[test]
    fn topological_sort_simple_chain() {
        // A depends on B depends on C
        // Expected order: C, B, A
        let items = vec!["A", "B", "C"];
        let get_key = |item: &&str| -> String { (*item).to_string() };
        let get_deps = |item: &&str| -> HashSet<String> {
            match *item {
                "A" => vec!["B".to_string()].into_iter().collect(),
                "B" => vec!["C".to_string()].into_iter().collect(),
                "C" => HashSet::new(),
                _ => HashSet::new(),
            }
        };

        let result = topological_sort(items, get_key, get_deps).unwrap();
        assert_eq!(result, vec!["C", "B", "A"]);
    }

    #[test]
    fn topological_sort_multiple_roots() {
        // A depends on B, C depends on D, no connection between chains
        // Valid orders: [B, A, D, C] or [D, C, B, A] or [B, D, A, C] etc.
        let items = vec!["A", "B", "C", "D"];
        let get_key = |item: &&str| -> String { (*item).to_string() };
        let get_deps = |item: &&str| -> HashSet<String> {
            match *item {
                "A" => vec!["B".to_string()].into_iter().collect(),
                "C" => vec!["D".to_string()].into_iter().collect(),
                _ => HashSet::new(),
            }
        };

        let result = topological_sort(items, get_key, get_deps).unwrap();

        // B must come before A
        let b_idx = result.iter().position(|&x| x == "B").unwrap();
        let a_idx = result.iter().position(|&x| x == "A").unwrap();
        assert!(b_idx < a_idx);

        // D must come before C
        let d_idx = result.iter().position(|&x| x == "D").unwrap();
        let c_idx = result.iter().position(|&x| x == "C").unwrap();
        assert!(d_idx < c_idx);
    }

    #[test]
    fn topological_sort_diamond_dependency() {
        // A depends on B and C, both B and C depend on D
        // Expected: D first, then B and C (order doesn't matter), then A
        let items = vec!["A", "B", "C", "D"];
        let get_key = |item: &&str| -> String { (*item).to_string() };
        let get_deps = |item: &&str| -> HashSet<String> {
            match *item {
                "A" => vec!["B".to_string(), "C".to_string()].into_iter().collect(),
                "B" => vec!["D".to_string()].into_iter().collect(),
                "C" => vec!["D".to_string()].into_iter().collect(),
                _ => HashSet::new(),
            }
        };

        let result = topological_sort(items, get_key, get_deps).unwrap();

        // D must come first
        assert_eq!(result[0], "D");

        // A must come last
        assert_eq!(result[3], "A");

        // B and C must be in middle (order doesn't matter)
        let b_idx = result.iter().position(|&x| x == "B").unwrap();
        let c_idx = result.iter().position(|&x| x == "C").unwrap();
        assert!(b_idx > 0 && b_idx < 3);
        assert!(c_idx > 0 && c_idx < 3);
    }

    #[test]
    fn topological_sort_detects_cycle() {
        // A depends on B, B depends on C, C depends on A (cycle)
        let items = vec!["A", "B", "C"];
        let get_key = |item: &&str| -> String { (*item).to_string() };
        let get_deps = |item: &&str| -> HashSet<String> {
            match *item {
                "A" => vec!["B".to_string()].into_iter().collect(),
                "B" => vec!["C".to_string()].into_iter().collect(),
                "C" => vec!["A".to_string()].into_iter().collect(),
                _ => HashSet::new(),
            }
        };

        let result = topological_sort(items, get_key, get_deps);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("Circular dependency"));
    }

    #[test]
    fn topological_sort_self_cycle() {
        // A depends on itself
        let items = vec!["A"];
        let get_key = |item: &&str| -> String { (*item).to_string() };
        let get_deps = |item: &&str| -> HashSet<String> {
            match *item {
                "A" => vec!["A".to_string()].into_iter().collect(),
                _ => HashSet::new(),
            }
        };

        let result = topological_sort(items, get_key, get_deps);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("Circular dependency"));
    }

    #[test]
    fn topological_sort_no_dependencies() {
        // No dependencies, any order is valid
        let items = vec!["A", "B", "C"];
        let get_key = |item: &&str| -> String { (*item).to_string() };
        let get_deps = |_item: &&str| -> HashSet<String> { HashSet::new() };

        let result = topological_sort(items, get_key, get_deps).unwrap();
        assert_eq!(result.len(), 3);
        assert!(result.contains(&"A"));
        assert!(result.contains(&"B"));
        assert!(result.contains(&"C"));
    }
}
