use super::MigrationOp;
use crate::model::qualified_name;
use std::collections::{HashMap, HashSet, VecDeque};

/// Plan and order migration operations for safe execution.
/// Creates are ordered first (with tables topologically sorted by FK dependencies),
/// then drops are ordered last (in reverse dependency order).
pub fn plan_migration(ops: Vec<MigrationOp>) -> Vec<MigrationOp> {
    let mut create_schemas = Vec::new();
    let mut drop_schemas = Vec::new();
    let mut create_extensions = Vec::new();
    let mut drop_extensions = Vec::new();
    let mut create_enums = Vec::new();
    let mut add_enum_values = Vec::new();
    let mut create_tables = Vec::new();
    let mut create_partitions = Vec::new();
    let mut add_columns = Vec::new();
    let mut add_primary_keys = Vec::new();
    let mut add_indexes = Vec::new();
    let mut alter_columns = Vec::new();
    let mut add_foreign_keys = Vec::new();
    let mut add_check_constraints = Vec::new();

    let mut drop_check_constraints = Vec::new();
    let mut drop_foreign_keys = Vec::new();
    let mut drop_indexes = Vec::new();
    let mut drop_primary_keys = Vec::new();
    let mut drop_columns = Vec::new();
    let mut drop_partitions = Vec::new();
    let mut drop_tables = Vec::new();
    let mut drop_enums = Vec::new();
    let mut enable_rls = Vec::new();
    let mut disable_rls = Vec::new();
    let mut create_policies = Vec::new();
    let mut drop_policies = Vec::new();
    let mut alter_policies = Vec::new();
    let mut create_functions = Vec::new();
    let mut drop_functions = Vec::new();
    let mut alter_functions = Vec::new();
    let mut create_views = Vec::new();
    let mut drop_views = Vec::new();
    let mut alter_views = Vec::new();
    let mut create_triggers = Vec::new();
    let mut drop_triggers = Vec::new();
    let mut alter_triggers = Vec::new();
    let mut create_sequences = Vec::new();
    let mut drop_sequences = Vec::new();
    let mut alter_sequences = Vec::new();
    let mut create_domains = Vec::new();
    let mut drop_domains = Vec::new();
    let mut alter_domains = Vec::new();
    let mut alter_owners = Vec::new();
    let mut backfill_hints = Vec::new();
    let mut set_column_not_nulls = Vec::new();
    let mut grant_privileges = Vec::new();
    let mut revoke_privileges = Vec::new();
    let mut create_version_schemas = Vec::new();
    let mut drop_version_schemas = Vec::new();
    let mut create_version_views = Vec::new();
    let mut drop_version_views = Vec::new();

    for op in ops {
        match op {
            MigrationOp::CreateSchema(_) => create_schemas.push(op),
            MigrationOp::DropSchema(_) => drop_schemas.push(op),
            MigrationOp::CreateExtension(_) => create_extensions.push(op),
            MigrationOp::DropExtension(_) => drop_extensions.push(op),
            MigrationOp::CreateEnum(_) => create_enums.push(op),
            MigrationOp::AddEnumValue { .. } => add_enum_values.push(op),
            MigrationOp::CreateTable(_) => create_tables.push(op),
            MigrationOp::CreatePartition(_) => create_partitions.push(op),
            MigrationOp::AddColumn { .. } => add_columns.push(op),
            MigrationOp::AddPrimaryKey { .. } => add_primary_keys.push(op),
            MigrationOp::AddIndex { .. } => add_indexes.push(op),
            MigrationOp::AlterColumn { .. } => alter_columns.push(op),
            MigrationOp::AddForeignKey { .. } => add_foreign_keys.push(op),
            MigrationOp::AddCheckConstraint { .. } => add_check_constraints.push(op),
            MigrationOp::DropCheckConstraint { .. } => drop_check_constraints.push(op),
            MigrationOp::DropForeignKey { .. } => drop_foreign_keys.push(op),
            MigrationOp::DropIndex { .. } => drop_indexes.push(op),
            MigrationOp::DropPrimaryKey { .. } => drop_primary_keys.push(op),
            MigrationOp::DropColumn { .. } => drop_columns.push(op),
            MigrationOp::DropPartition(_) => drop_partitions.push(op),
            MigrationOp::DropTable(_) => drop_tables.push(op),
            MigrationOp::DropEnum(_) => drop_enums.push(op),
            MigrationOp::EnableRls { .. } => enable_rls.push(op),
            MigrationOp::DisableRls { .. } => disable_rls.push(op),
            MigrationOp::CreatePolicy(_) => create_policies.push(op),
            MigrationOp::DropPolicy { .. } => drop_policies.push(op),
            MigrationOp::AlterPolicy { .. } => alter_policies.push(op),
            MigrationOp::CreateFunction(_) => create_functions.push(op),
            MigrationOp::DropFunction { .. } => drop_functions.push(op),
            MigrationOp::AlterFunction { .. } => alter_functions.push(op),
            MigrationOp::CreateView(_) => create_views.push(op),
            MigrationOp::DropView { .. } => drop_views.push(op),
            MigrationOp::AlterView { .. } => alter_views.push(op),
            MigrationOp::CreateTrigger(_) => create_triggers.push(op),
            MigrationOp::DropTrigger { .. } => drop_triggers.push(op),
            MigrationOp::AlterTriggerEnabled { .. } => alter_triggers.push(op),
            MigrationOp::CreateSequence(_) => create_sequences.push(op),
            MigrationOp::DropSequence(_) => drop_sequences.push(op),
            MigrationOp::AlterSequence { .. } => alter_sequences.push(op),
            MigrationOp::CreateDomain(_) => create_domains.push(op),
            MigrationOp::DropDomain(_) => drop_domains.push(op),
            MigrationOp::AlterDomain { .. } => alter_domains.push(op),
            MigrationOp::AlterOwner { .. } => alter_owners.push(op),
            MigrationOp::BackfillHint { .. } => backfill_hints.push(op),
            MigrationOp::SetColumnNotNull { .. } => set_column_not_nulls.push(op),
            MigrationOp::GrantPrivileges { .. } => grant_privileges.push(op),
            MigrationOp::RevokePrivileges { .. } => revoke_privileges.push(op),
            MigrationOp::CreateVersionSchema { .. } => create_version_schemas.push(op),
            MigrationOp::DropVersionSchema { .. } => drop_version_schemas.push(op),
            MigrationOp::CreateVersionView { .. } => create_version_views.push(op),
            MigrationOp::DropVersionView { .. } => drop_version_views.push(op),
        }
    }

    let mut create_sequences_without_owner = Vec::new();
    let mut set_sequence_owners = Vec::new();

    for op in create_sequences {
        if let MigrationOp::CreateSequence(ref seq) = op {
            if let Some(ref owned_by) = seq.owned_by {
                let mut seq_without_owner = seq.clone();
                seq_without_owner.owned_by = None;
                create_sequences_without_owner.push(MigrationOp::CreateSequence(seq_without_owner));

                let changes = super::SequenceChanges {
                    owned_by: Some(Some(owned_by.clone())),
                    ..Default::default()
                };
                set_sequence_owners.push(MigrationOp::AlterSequence {
                    name: qualified_name(&seq.schema, &seq.name),
                    changes,
                });
            } else {
                create_sequences_without_owner.push(op);
            }
        }
    }

    let create_tables = order_table_creates(create_tables);
    let drop_tables = order_table_drops(drop_tables);
    let create_views = order_view_creates(create_views);

    let mut result = Vec::new();

    result.extend(create_schemas);
    result.extend(create_version_schemas);
    result.extend(create_extensions);
    result.extend(create_enums);
    result.extend(add_enum_values);
    result.extend(create_domains);
    result.extend(create_sequences_without_owner);
    // Drop functions before creating new ones - needed when modifying a function
    // that requires DROP + CREATE (e.g., parameter name changes, return type changes)
    result.extend(drop_functions);
    result.extend(create_functions);
    result.extend(create_tables);
    result.extend(create_partitions);
    result.extend(add_columns);
    result.extend(add_primary_keys);
    // Drop indexes before adding new ones - needed when modifying an index
    // that requires DROP + CREATE (e.g., predicate changes, column changes)
    result.extend(drop_indexes);
    result.extend(add_indexes);
    result.extend(alter_columns);
    result.extend(set_column_not_nulls);
    // Drop check constraints before adding new ones - needed when modifying a constraint
    // (same name, different expression) since PostgreSQL doesn't allow duplicate names
    result.extend(drop_check_constraints);
    result.extend(add_foreign_keys);
    result.extend(add_check_constraints);
    result.extend(set_sequence_owners);
    result.extend(enable_rls);
    result.extend(create_policies);
    result.extend(alter_policies);
    result.extend(alter_sequences);
    result.extend(alter_domains);
    result.extend(alter_functions);
    result.extend(create_views);
    result.extend(alter_views);
    result.extend(create_version_views);
    result.extend(create_triggers);
    result.extend(alter_triggers);
    result.extend(alter_owners);
    result.extend(grant_privileges);

    result.extend(revoke_privileges);
    result.extend(drop_triggers);
    result.extend(drop_version_views);
    result.extend(drop_views);
    result.extend(drop_policies);
    result.extend(disable_rls);
    // Note: drop_check_constraints is handled earlier (before add_check_constraints)
    // to support constraint modifications
    result.extend(drop_foreign_keys);
    // Note: drop_indexes is handled earlier (before add_indexes)
    // to support index modifications that require DROP + CREATE
    result.extend(drop_primary_keys);
    result.extend(drop_columns);
    result.extend(drop_partitions);
    result.extend(drop_tables);
    // Note: drop_functions is handled earlier (before create_functions)
    // to support function modifications that require DROP + CREATE
    result.extend(drop_sequences);
    result.extend(drop_domains);
    result.extend(drop_enums);
    result.extend(drop_extensions);
    result.extend(drop_version_schemas);
    result.extend(drop_schemas);

    result
}

/// Plan operations for a schema dump (not migration).
/// Unlike plan_migration, this keeps OWNED BY inline in CREATE SEQUENCE
/// by placing sequences after tables they reference.
pub fn plan_dump(ops: Vec<MigrationOp>) -> Vec<MigrationOp> {
    let mut create_schemas = Vec::new();
    let mut create_extensions = Vec::new();
    let mut create_enums = Vec::new();
    let mut create_domains = Vec::new();
    let mut create_tables = Vec::new();
    let mut create_sequences = Vec::new();
    let mut create_partitions = Vec::new();
    let mut create_functions = Vec::new();
    let mut create_views = Vec::new();
    let mut create_triggers = Vec::new();
    let mut enable_rls = Vec::new();
    let mut create_policies = Vec::new();
    let mut alter_owners = Vec::new();
    let mut grant_privileges = Vec::new();

    for op in ops {
        match op {
            MigrationOp::CreateSchema(_) => create_schemas.push(op),
            MigrationOp::CreateExtension(_) => create_extensions.push(op),
            MigrationOp::CreateEnum(_) => create_enums.push(op),
            MigrationOp::CreateDomain(_) => create_domains.push(op),
            MigrationOp::CreateTable(_) => create_tables.push(op),
            MigrationOp::CreateSequence(_) => create_sequences.push(op),
            MigrationOp::CreatePartition(_) => create_partitions.push(op),
            MigrationOp::CreateFunction(_) => create_functions.push(op),
            MigrationOp::CreateView(_) => create_views.push(op),
            MigrationOp::CreateTrigger(_) => create_triggers.push(op),
            MigrationOp::EnableRls { .. } => enable_rls.push(op),
            MigrationOp::CreatePolicy(_) => create_policies.push(op),
            MigrationOp::AlterOwner { .. } => alter_owners.push(op),
            MigrationOp::GrantPrivileges { .. } => grant_privileges.push(op),
            _ => {}
        }
    }

    let create_tables = order_table_creates(create_tables);
    let create_views = order_view_creates(create_views);

    let mut result = Vec::new();

    result.extend(create_schemas);
    result.extend(create_extensions);
    result.extend(create_enums);
    result.extend(create_domains);
    result.extend(create_functions);
    result.extend(create_tables);
    result.extend(create_partitions);
    result.extend(create_sequences);
    result.extend(enable_rls);
    result.extend(create_policies);
    result.extend(create_views);
    result.extend(create_triggers);
    result.extend(alter_owners);
    result.extend(grant_privileges);

    result
}

/// Extract table/view references from a SQL query string.
/// Returns qualified names (schema.name) of all referenced relations.
fn extract_relation_references(query: &str) -> HashSet<String> {
    use sqlparser::ast::{
        Expr, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Query, Select,
        SelectItem, SetExpr, Statement, TableFactor, TableWithJoins,
    };
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    let mut refs = HashSet::new();
    let dialect = PostgreSqlDialect {};

    let sql = format!("SELECT * FROM ({query}) AS subq");
    let parse_result = Parser::parse_sql(&dialect, &sql);

    let statements = match parse_result {
        Ok(stmts) => stmts,
        Err(_) => match Parser::parse_sql(&dialect, query) {
            Ok(stmts) => stmts,
            Err(_) => return refs,
        },
    };

    fn extract_from_expr(expr: &Expr, refs: &mut HashSet<String>) {
        match expr {
            Expr::Subquery(query) => extract_from_query(query, refs),
            Expr::InSubquery { subquery, .. } => extract_from_query(subquery, refs),
            Expr::Exists { subquery, .. } => extract_from_query(subquery, refs),
            Expr::BinaryOp { left, right, .. } => {
                extract_from_expr(left, refs);
                extract_from_expr(right, refs);
            }
            Expr::UnaryOp { expr, .. } => extract_from_expr(expr, refs),
            Expr::Nested(e) => extract_from_expr(e, refs),
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if let Some(op) = operand {
                    extract_from_expr(op, refs);
                }
                for cw in conditions {
                    extract_from_expr(&cw.condition, refs);
                    extract_from_expr(&cw.result, refs);
                }
                if let Some(else_r) = else_result {
                    extract_from_expr(else_r, refs);
                }
            }
            Expr::Function(f) => {
                if let FunctionArguments::List(FunctionArgumentList { args, .. }) = &f.args {
                    for arg in args {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                            extract_from_expr(e, refs);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    fn extract_from_select(select: &Select, refs: &mut HashSet<String>) {
        for table_with_joins in &select.from {
            extract_from_table_with_joins(table_with_joins, refs);
        }

        if let Some(selection) = &select.selection {
            extract_from_expr(selection, refs);
        }

        for item in &select.projection {
            if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item {
                extract_from_expr(expr, refs);
            }
        }

        if let Some(having) = &select.having {
            extract_from_expr(having, refs);
        }
    }

    fn extract_from_table_with_joins(twj: &TableWithJoins, refs: &mut HashSet<String>) {
        extract_from_table_factor(&twj.relation, refs);
        for join in &twj.joins {
            extract_from_table_factor(&join.relation, refs);
        }
    }

    fn extract_from_table_factor(factor: &TableFactor, refs: &mut HashSet<String>) {
        match factor {
            TableFactor::Table { name, .. } => {
                let parts: Vec<String> = name
                    .0
                    .iter()
                    .map(|p| p.to_string().trim_matches('"').to_string())
                    .collect();
                let qualified = if parts.len() == 1 {
                    format!("public.{}", parts[0])
                } else {
                    format!("{}.{}", parts[0], parts[1])
                };
                refs.insert(qualified);
            }
            TableFactor::Derived { subquery, .. } => {
                extract_from_query(subquery, refs);
            }
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                extract_from_table_with_joins(table_with_joins, refs);
            }
            _ => {}
        }
    }

    fn extract_from_query(query: &Query, refs: &mut HashSet<String>) {
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                extract_from_query(&cte.query, refs);
            }
        }

        extract_from_set_expr(&query.body, refs);
    }

    fn extract_from_set_expr(set_expr: &SetExpr, refs: &mut HashSet<String>) {
        match set_expr {
            SetExpr::Select(select) => extract_from_select(select, refs),
            SetExpr::Query(query) => extract_from_query(query, refs),
            SetExpr::SetOperation { left, right, .. } => {
                extract_from_set_expr(left, refs);
                extract_from_set_expr(right, refs);
            }
            _ => {}
        }
    }

    for statement in &statements {
        if let Statement::Query(query) = statement {
            extract_from_query(query, &mut refs);
        }
    }

    refs
}

/// Topologically sort CreateView operations by their dependencies.
/// Views that are referenced by other views must be created first.
fn order_view_creates(ops: Vec<MigrationOp>) -> Vec<MigrationOp> {
    if ops.is_empty() {
        return ops;
    }

    let mut view_ops: HashMap<String, MigrationOp> = HashMap::new();
    let mut dependencies: HashMap<String, HashSet<String>> = HashMap::new();

    let view_names: HashSet<String> = ops
        .iter()
        .filter_map(|op| {
            if let MigrationOp::CreateView(ref view) = op {
                Some(qualified_name(&view.schema, &view.name))
            } else {
                None
            }
        })
        .collect();

    for op in ops {
        if let MigrationOp::CreateView(ref view) = op {
            let qualified_view_name = qualified_name(&view.schema, &view.name);

            let all_refs = extract_relation_references(&view.query);

            // Only keep references to views being created; tables are created first
            let deps: HashSet<String> = all_refs
                .into_iter()
                .filter(|r| view_names.contains(r) && *r != qualified_view_name)
                .collect();

            dependencies.insert(qualified_view_name.clone(), deps);
            view_ops.insert(qualified_view_name, op);
        }
    }

    topological_sort(&view_ops, &dependencies)
}

/// Topologically sort CreateTable operations by FK dependencies.
/// Tables that are referenced by other tables must be created first.
fn order_table_creates(ops: Vec<MigrationOp>) -> Vec<MigrationOp> {
    if ops.is_empty() {
        return ops;
    }

    let mut table_ops: HashMap<String, MigrationOp> = HashMap::new();
    let mut dependencies: HashMap<String, HashSet<String>> = HashMap::new();

    for op in ops {
        if let MigrationOp::CreateTable(ref table) = op {
            let qualified_table_name = qualified_name(&table.schema, &table.name);
            let mut deps = HashSet::new();
            for fk in &table.foreign_keys {
                let qualified_ref = qualified_name(&fk.referenced_schema, &fk.referenced_table);
                if qualified_ref != qualified_table_name {
                    deps.insert(qualified_ref);
                }
            }
            dependencies.insert(qualified_table_name.clone(), deps);
            table_ops.insert(qualified_table_name, op);
        }
    }

    topological_sort(&table_ops, &dependencies)
}

/// Reverse topologically sort DropTable operations.
/// Tables that reference other tables must be dropped first.
fn order_table_drops(ops: Vec<MigrationOp>) -> Vec<MigrationOp> {
    if ops.is_empty() {
        return ops;
    }

    let table_names: Vec<String> = ops
        .iter()
        .filter_map(|op| {
            if let MigrationOp::DropTable(name) = op {
                Some(name.clone())
            } else {
                None
            }
        })
        .collect();

    let mut table_ops: HashMap<String, MigrationOp> = HashMap::new();
    let mut dependencies: HashMap<String, HashSet<String>> = HashMap::new();

    for op in ops {
        if let MigrationOp::DropTable(ref name) = op {
            dependencies.insert(name.clone(), HashSet::new());
            table_ops.insert(name.clone(), op);
        }
    }

    let mut sorted = topological_sort(&table_ops, &dependencies);
    sorted.reverse();

    sorted
        .into_iter()
        .filter(|op| {
            if let MigrationOp::DropTable(name) = op {
                table_names.contains(name)
            } else {
                false
            }
        })
        .collect()
}

/// Perform Kahn's algorithm for topological sort.
fn topological_sort(
    table_ops: &HashMap<String, MigrationOp>,
    dependencies: &HashMap<String, HashSet<String>>,
) -> Vec<MigrationOp> {
    let mut in_degree: HashMap<String, usize> = HashMap::new();
    let mut reverse_deps: HashMap<String, Vec<String>> = HashMap::new();

    for name in table_ops.keys() {
        in_degree.insert(name.clone(), 0);
        reverse_deps.insert(name.clone(), Vec::new());
    }

    for (table, deps) in dependencies {
        let count = deps.iter().filter(|d| table_ops.contains_key(*d)).count();
        in_degree.insert(table.clone(), count);
        for dep in deps {
            if table_ops.contains_key(dep) {
                reverse_deps
                    .entry(dep.clone())
                    .or_default()
                    .push(table.clone());
            }
        }
    }

    let mut queue: VecDeque<String> = in_degree
        .iter()
        .filter(|(_, &count)| count == 0)
        .map(|(name, _)| name.clone())
        .collect();

    let mut sorted_names: Vec<String> = Vec::new();

    while let Some(name) = queue.pop_front() {
        sorted_names.push(name.clone());
        if let Some(dependents) = reverse_deps.get(&name) {
            for dependent in dependents {
                if let Some(count) = in_degree.get_mut(dependent) {
                    *count -= 1;
                    if *count == 0 {
                        queue.push_back(dependent.clone());
                    }
                }
            }
        }
    }

    let unsorted: Vec<String> = table_ops
        .keys()
        .filter(|name| !sorted_names.contains(name))
        .cloned()
        .collect();
    sorted_names.extend(unsorted);

    sorted_names
        .into_iter()
        .filter_map(|name| table_ops.get(&name).cloned())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::*;
    use std::collections::BTreeMap;

    fn make_table(name: &str, foreign_keys: Vec<ForeignKey>) -> Table {
        Table {
            name: name.to_string(),
            schema: "public".to_string(),
            columns: BTreeMap::new(),
            indexes: Vec::new(),
            primary_key: None,
            foreign_keys,
            check_constraints: Vec::new(),
            comment: None,
            row_level_security: false,
            policies: Vec::new(),
            partition_by: None,

            owner: None,
            grants: Vec::new(),
        }
    }

    fn make_fk(referenced_table: &str) -> ForeignKey {
        ForeignKey {
            name: format!("fk_{referenced_table}"),
            columns: vec!["id".to_string()],
            referenced_table: referenced_table.to_string(),
            referenced_schema: "public".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: ReferentialAction::NoAction,
            on_update: ReferentialAction::NoAction,
        }
    }

    #[test]
    fn create_tables_ordered_by_fk_dependencies() {
        let posts = make_table("posts", vec![make_fk("users")]);
        let users = make_table("users", vec![]);
        let comments = make_table("comments", vec![make_fk("posts"), make_fk("users")]);

        let ops = vec![
            MigrationOp::CreateTable(comments),
            MigrationOp::CreateTable(posts),
            MigrationOp::CreateTable(users),
        ];

        let planned = plan_migration(ops);

        let table_order: Vec<String> = planned
            .iter()
            .filter_map(|op| {
                if let MigrationOp::CreateTable(t) = op {
                    Some(t.name.clone())
                } else {
                    None
                }
            })
            .collect();

        let users_pos = table_order.iter().position(|n| n == "users").unwrap();
        let posts_pos = table_order.iter().position(|n| n == "posts").unwrap();
        let comments_pos = table_order.iter().position(|n| n == "comments").unwrap();

        assert!(users_pos < posts_pos, "users must be created before posts");
        assert!(
            posts_pos < comments_pos,
            "posts must be created before comments"
        );
        assert!(
            users_pos < comments_pos,
            "users must be created before comments"
        );
    }

    #[test]
    fn creates_before_drops() {
        let users = make_table("users", vec![]);

        let ops = vec![
            MigrationOp::DropTable("old_table".to_string()),
            MigrationOp::CreateTable(users),
            MigrationOp::DropColumn {
                table: "foo".to_string(),
                column: "bar".to_string(),
            },
            MigrationOp::AddColumn {
                table: "foo".to_string(),
                column: Column {
                    name: "baz".to_string(),
                    data_type: PgType::Text,
                    nullable: true,
                    default: None,
                    comment: None,
                },
            },
        ];

        let planned = plan_migration(ops);

        let create_table_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTable(_)))
            .unwrap();
        let drop_table_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropTable(_)))
            .unwrap();
        let add_column_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::AddColumn { .. }))
            .unwrap();
        let drop_column_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropColumn { .. }))
            .unwrap();

        assert!(
            create_table_pos < drop_table_pos,
            "CreateTable must come before DropTable"
        );
        assert!(
            add_column_pos < drop_column_pos,
            "AddColumn must come before DropColumn"
        );
    }

    #[test]
    fn drop_foreign_key_before_drop_column() {
        let ops = vec![
            MigrationOp::DropColumn {
                table: "posts".to_string(),
                column: "user_id".to_string(),
            },
            MigrationOp::DropForeignKey {
                table: "posts".to_string(),
                foreign_key_name: "posts_user_id_fkey".to_string(),
            },
        ];

        let planned = plan_migration(ops);

        let drop_fk_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropForeignKey { .. }))
            .unwrap();
        let drop_column_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropColumn { .. }))
            .unwrap();

        assert!(
            drop_fk_pos < drop_column_pos,
            "DropForeignKey must come before DropColumn"
        );
    }

    #[test]
    fn add_column_before_add_index() {
        let ops = vec![
            MigrationOp::AddIndex {
                table: "users".to_string(),
                index: Index {
                    name: "users_email_idx".to_string(),
                    columns: vec!["email".to_string()],
                    unique: true,
                    index_type: IndexType::BTree,
                    predicate: None,
                },
            },
            MigrationOp::AddColumn {
                table: "users".to_string(),
                column: Column {
                    name: "email".to_string(),
                    data_type: PgType::Text,
                    nullable: false,
                    default: None,
                    comment: None,
                },
            },
        ];

        let planned = plan_migration(ops);

        let add_column_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::AddColumn { .. }))
            .unwrap();
        let add_index_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::AddIndex { .. }))
            .unwrap();

        assert!(
            add_column_pos < add_index_pos,
            "AddColumn must come before AddIndex"
        );
    }

    #[test]
    fn enums_created_before_tables() {
        let ops = vec![
            MigrationOp::CreateTable(make_table("users", vec![])),
            MigrationOp::CreateEnum(EnumType {
                name: "user_role".to_string(),
                schema: "public".to_string(),
                values: vec!["admin".to_string(), "user".to_string()],

                owner: None,
                grants: Vec::new(),
            }),
        ];

        let planned = plan_migration(ops);

        let create_enum_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateEnum(_)))
            .unwrap();
        let create_table_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTable(_)))
            .unwrap();

        assert!(
            create_enum_pos < create_table_pos,
            "CreateEnum must come before CreateTable"
        );
    }

    #[test]
    fn empty_ops_returns_empty() {
        let ops: Vec<MigrationOp> = vec![];
        let planned = plan_migration(ops);
        assert!(planned.is_empty());
    }

    #[test]
    fn add_enum_value_ordered_after_create_enum_before_tables() {
        let ops = vec![
            MigrationOp::CreateTable(make_table("users", vec![])),
            MigrationOp::AddEnumValue {
                enum_name: "user_role".to_string(),
                value: "guest".to_string(),
                position: None,
            },
            MigrationOp::CreateEnum(EnumType {
                name: "user_role".to_string(),
                schema: "public".to_string(),
                values: vec!["admin".to_string(), "user".to_string()],

                owner: None,
                grants: Vec::new(),
            }),
        ];

        let planned = plan_migration(ops);

        let create_enum_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateEnum(_)))
            .unwrap();
        let add_enum_value_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::AddEnumValue { .. }))
            .unwrap();
        let create_table_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTable(_)))
            .unwrap();

        assert!(
            create_enum_pos < add_enum_value_pos,
            "CreateEnum must come before AddEnumValue"
        );
        assert!(
            add_enum_value_pos < create_table_pos,
            "AddEnumValue must come before CreateTable"
        );
    }

    #[test]
    fn create_views_ordered_by_view_dependencies() {
        // view_c depends on view_b which depends on view_a
        let view_a = View {
            name: "view_a".to_string(),
            schema: "public".to_string(),
            query: "SELECT * FROM users".to_string(),
            materialized: false,

            owner: None,
            grants: Vec::new(),
        };
        let view_b = View {
            name: "view_b".to_string(),
            schema: "public".to_string(),
            query: "SELECT * FROM public.view_a".to_string(),
            materialized: false,

            owner: None,
            grants: Vec::new(),
        };
        let view_c = View {
            name: "view_c".to_string(),
            schema: "public".to_string(),
            query: "SELECT * FROM public.view_b JOIN public.view_a ON true".to_string(),
            materialized: false,

            owner: None,
            grants: Vec::new(),
        };

        let ops = vec![
            MigrationOp::CreateView(view_c),
            MigrationOp::CreateView(view_a),
            MigrationOp::CreateView(view_b),
        ];

        let planned = plan_migration(ops);

        let view_order: Vec<String> = planned
            .iter()
            .filter_map(|op| {
                if let MigrationOp::CreateView(v) = op {
                    Some(v.name.clone())
                } else {
                    None
                }
            })
            .collect();

        let view_a_pos = view_order.iter().position(|n| n == "view_a").unwrap();
        let view_b_pos = view_order.iter().position(|n| n == "view_b").unwrap();
        let view_c_pos = view_order.iter().position(|n| n == "view_c").unwrap();

        assert!(
            view_a_pos < view_b_pos,
            "view_a must be created before view_b"
        );
        assert!(
            view_b_pos < view_c_pos,
            "view_b must be created before view_c"
        );
        assert!(
            view_a_pos < view_c_pos,
            "view_a must be created before view_c"
        );
    }

    #[test]
    fn extract_relation_references_from_view_query() {
        let refs = extract_relation_references(
            "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
        );
        assert!(refs.contains("public.users"));
        assert!(refs.contains("public.orders"));
    }

    #[test]
    fn extract_relation_references_with_schema() {
        let refs = extract_relation_references(
            "SELECT * FROM auth.users JOIN public.orders ON auth.users.id = public.orders.user_id",
        );
        assert!(refs.contains("auth.users"));
        assert!(refs.contains("public.orders"));
    }

    #[test]
    fn extract_relation_references_from_subquery() {
        let refs = extract_relation_references("SELECT * FROM (SELECT * FROM inner_table) AS sub");
        assert!(refs.contains("public.inner_table"));
    }

    #[test]
    fn sequences_with_owned_by_after_tables() {
        let seq = Sequence {
            name: "users_id_seq".to_string(),
            schema: "public".to_string(),
            data_type: SequenceDataType::BigInt,
            start: Some(1),
            increment: Some(1),
            min_value: Some(1),
            max_value: Some(9223372036854775807),
            cycle: false,
            owner: None,
            grants: Vec::new(),
            cache: Some(1),
            owned_by: Some(SequenceOwner {
                table_schema: "public".to_string(),
                table_name: "users".to_string(),
                column_name: "id".to_string(),
            }),
        };
        let table = make_table("users", vec![]);

        let ops = vec![
            MigrationOp::CreateSequence(seq.clone()),
            MigrationOp::CreateTable(table),
        ];
        let result = plan_migration(ops);

        let create_seq_pos = result
            .iter()
            .position(|op| {
                matches!(op, MigrationOp::CreateSequence(s) if s.name == "users_id_seq" && s.owned_by.is_none())
            })
            .expect("CreateSequence without OWNED BY should exist");
        let create_table_pos = result
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTable(t) if t.name == "users"))
            .expect("CreateTable should exist");
        let alter_seq_pos = result
            .iter()
            .position(|op| {
                matches!(op, MigrationOp::AlterSequence { name, changes } if name == "public.users_id_seq" && changes.owned_by.is_some())
            })
            .expect("AlterSequence to set OWNED BY should exist");

        assert!(
            create_seq_pos < create_table_pos,
            "CreateSequence (without OWNED BY) must come before CreateTable"
        );
        assert!(
            create_table_pos < alter_seq_pos,
            "AlterSequence (setting OWNED BY) must come after CreateTable"
        );
    }

    #[test]
    fn drop_function_before_create_function() {
        // When a function requires DROP + CREATE (e.g., parameter name change),
        // DROP must come before CREATE to avoid "already exists" error
        let func = Function {
            name: "my_func".to_string(),
            schema: "public".to_string(),
            arguments: vec![],
            return_type: "void".to_string(),
            language: "plpgsql".to_string(),
            body: "BEGIN END;".to_string(),
            volatility: Volatility::Volatile,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
        };

        let ops = vec![
            MigrationOp::DropFunction {
                name: "public.my_func".to_string(),
                args: "".to_string(),
            },
            MigrationOp::CreateFunction(func),
        ];

        let planned = plan_migration(ops);

        let drop_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropFunction { .. }))
            .unwrap();
        let create_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateFunction(_)))
            .unwrap();

        assert!(
            drop_pos < create_pos,
            "DropFunction must come before CreateFunction. DROP at {drop_pos}, CREATE at {create_pos}"
        );
    }

    #[test]
    fn drop_index_before_add_index() {
        // When an index requires DROP + CREATE (e.g., predicate or column changes),
        // DROP must come before CREATE to avoid "already exists" error
        let index = Index {
            name: "users_email_idx".to_string(),
            columns: vec!["email".to_string()],
            unique: true,
            index_type: IndexType::BTree,
            predicate: Some("active = true".to_string()),
        };

        let ops = vec![
            MigrationOp::AddIndex {
                table: "public.users".to_string(),
                index: index.clone(),
            },
            MigrationOp::DropIndex {
                table: "public.users".to_string(),
                index_name: "users_email_idx".to_string(),
            },
        ];

        let planned = plan_migration(ops);

        let drop_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropIndex { .. }))
            .unwrap();
        let add_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::AddIndex { .. }))
            .unwrap();

        assert!(
            drop_pos < add_pos,
            "DropIndex must come before AddIndex. DROP at {drop_pos}, ADD at {add_pos}"
        );
    }
}
