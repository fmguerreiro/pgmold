/// Extract dependencies from SQL statements.
///
/// This module parses SQL DDL to identify object references, enabling
/// topological sorting for correct creation order.

use regex::Regex;
use std::collections::HashSet;

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
}

/// Extract function references from a function body.
///
/// Detects patterns like:
/// - schema.function_name()
/// - function_name()
/// - schema.function_name(args)
pub fn extract_function_references(body: &str, default_schema: &str) -> HashSet<ObjectRef> {
    let mut refs = HashSet::new();

    // Match function calls: schema.name() or name()
    // Matches: auth.jwt(), is_admin(), check_permission(args)
    let func_pattern = Regex::new(r"(?i)(?:([a-z_][a-z0-9_]*)\.)?\s*([a-z_][a-z0-9_]*)\s*\(").unwrap();

    for cap in func_pattern.captures_iter(body) {
        let schema = cap.get(1).map(|m| m.as_str()).unwrap_or(default_schema);
        let name = cap.get(2).map(|m| m.as_str()).unwrap();

        // Filter out common PostgreSQL built-in functions
        if !is_builtin_function(name) {
            refs.insert(ObjectRef::new(schema, name));
        }
    }

    refs
}

/// Check if a function name is a PostgreSQL built-in.
fn is_builtin_function(name: &str) -> bool {
    let name_lower = name.to_lowercase();
    matches!(
        name_lower.as_str(),
        "now"
            | "current_timestamp"
            | "current_date"
            | "current_time"
            | "count"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "coalesce"
            | "nullif"
            | "greatest"
            | "least"
            | "abs"
            | "ceil"
            | "floor"
            | "round"
            | "trunc"
            | "upper"
            | "lower"
            | "length"
            | "substring"
            | "concat"
            | "replace"
            | "trim"
            | "ltrim"
            | "rtrim"
            | "to_char"
            | "to_date"
            | "to_timestamp"
            | "extract"
            | "date_trunc"
            | "age"
            | "array_agg"
            | "json_agg"
            | "jsonb_agg"
            | "string_agg"
            | "bool_and"
            | "bool_or"
            | "every"
            | "row_number"
            | "rank"
            | "dense_rank"
            | "percent_rank"
            | "cume_dist"
            | "ntile"
            | "lag"
            | "lead"
            | "first_value"
            | "last_value"
            | "nth_value"
    )
}

/// Extract table/view references from SQL (for views, triggers, functions).
///
/// Detects patterns like:
/// - FROM schema.table
/// - JOIN table
/// - INSERT INTO table
/// - UPDATE table
pub fn extract_table_references(body: &str, default_schema: &str) -> HashSet<ObjectRef> {
    let mut refs = HashSet::new();

    // Pattern 1: FROM clause - FROM schema.table or FROM table
    let from_pattern = Regex::new(r"(?i)\bFROM\s+(?:([a-z_][a-z0-9_]*)\.)?\s*([a-z_][a-z0-9_]*)\b").unwrap();
    for cap in from_pattern.captures_iter(body) {
        let schema = cap.get(1).map(|m| m.as_str()).unwrap_or(default_schema);
        let name = cap.get(2).map(|m| m.as_str()).unwrap();
        refs.insert(ObjectRef::new(schema, name));
    }

    // Pattern 2: JOIN clause - JOIN schema.table or JOIN table
    let join_pattern = Regex::new(r"(?i)\bJOIN\s+(?:([a-z_][a-z0-9_]*)\.)?\s*([a-z_][a-z0-9_]*)\b").unwrap();
    for cap in join_pattern.captures_iter(body) {
        let schema = cap.get(1).map(|m| m.as_str()).unwrap_or(default_schema);
        let name = cap.get(2).map(|m| m.as_str()).unwrap();
        refs.insert(ObjectRef::new(schema, name));
    }

    // Pattern 3: INSERT INTO - INSERT INTO schema.table or INSERT INTO table
    let insert_pattern = Regex::new(r"(?i)\bINSERT\s+INTO\s+(?:([a-z_][a-z0-9_]*)\.)?\s*([a-z_][a-z0-9_]*)\b").unwrap();
    for cap in insert_pattern.captures_iter(body) {
        let schema = cap.get(1).map(|m| m.as_str()).unwrap_or(default_schema);
        let name = cap.get(2).map(|m| m.as_str()).unwrap();
        refs.insert(ObjectRef::new(schema, name));
    }

    // Pattern 4: UPDATE - UPDATE schema.table or UPDATE table
    let update_pattern = Regex::new(r"(?i)\bUPDATE\s+(?:([a-z_][a-z0-9_]*)\.)?\s*([a-z_][a-z0-9_]*)\b").unwrap();
    for cap in update_pattern.captures_iter(body) {
        let schema = cap.get(1).map(|m| m.as_str()).unwrap_or(default_schema);
        let name = cap.get(2).map(|m| m.as_str()).unwrap();
        refs.insert(ObjectRef::new(schema, name));
    }

    refs
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
    use std::collections::{HashMap, VecDeque};

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
                graph.entry(dep_key.clone()).or_insert_with(Vec::new).push(key.clone());
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
        let processed: HashSet<String> = sorted.iter().map(|item| get_key(item)).collect();
        let unprocessed: Vec<String> = items
            .iter()
            .map(|item| get_key(item))
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
            SELECT auth.jwt()->'rbac_context'->'level'
            WHERE auth.is_admin() AND public.check_permission()
        "#;
        let refs = extract_function_references(body, "public");

        assert_eq!(refs.len(), 3);
        assert!(refs.contains(&ObjectRef::new("auth", "jwt")));
        assert!(refs.contains(&ObjectRef::new("auth", "is_admin")));
        assert!(refs.contains(&ObjectRef::new("public", "check_permission")));
    }

    #[test]
    fn extract_function_call_with_args() {
        let body = "SELECT add_fifteen(x) + multiply(a, b)";
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

        assert_eq!(refs.len(), 1);
        assert!(refs.contains(&ObjectRef::new("public", "audit_log")));
    }

    #[test]
    fn extract_table_from_update() {
        let body = "UPDATE users SET last_login = now()";
        let refs = extract_table_references(body, "public");

        assert_eq!(refs.len(), 1);
        assert!(refs.contains(&ObjectRef::new("public", "users")));
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

        let result = super::topological_sort(items, get_key, get_deps).unwrap();
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

        let result = super::topological_sort(items, get_key, get_deps).unwrap();

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

        let result = super::topological_sort(items, get_key, get_deps).unwrap();

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

        let result = super::topological_sort(items, get_key, get_deps);
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

        let result = super::topological_sort(items, get_key, get_deps);
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

        let result = super::topological_sort(items, get_key, get_deps).unwrap();
        assert_eq!(result.len(), 3);
        assert!(result.contains(&"A"));
        assert!(result.contains(&"B"));
        assert!(result.contains(&"C"));
    }
}
