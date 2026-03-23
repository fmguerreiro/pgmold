use super::op_key::{
    add_privilege_dependency_edge, extract_relation_references, extract_setof_type_ref,
    parse_type_ref, OpKey,
};
use super::{MigrationOp, OwnerObjectKind};
use crate::model::{parse_qualified_name, qualified_name, QualifiedName};
use crate::parser::{extract_function_references, extract_rowtype_references};
use petgraph::algo::toposort;
use petgraph::graph::{DiGraph, NodeIndex};
use std::collections::{HashMap, HashSet};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PlanError {
    #[error("Circular dependency detected involving: {0}")]
    CyclicDependency(String),
}

/// Pre-collected node sets used across multiple edge-building methods.
/// Built once in `add_type_level_edges` to avoid repeated traversals.
///
/// ⚠️ When adding a new `OpKey` variant to the planner, add a corresponding field here
/// and populate it in `NodeSets::new`.
struct NodeSets {
    schemas: Vec<NodeIndex>,
    version_schemas: Vec<NodeIndex>,
    extensions: Vec<NodeIndex>,
    enums: Vec<NodeIndex>,
    add_enum_values: Vec<NodeIndex>,
    domains: Vec<NodeIndex>,
    sequences: Vec<NodeIndex>,
    functions: Vec<NodeIndex>,
    alter_functions: Vec<NodeIndex>,
    tables: Vec<NodeIndex>,
    partitions: Vec<NodeIndex>,
    add_columns: Vec<NodeIndex>,
    add_pks: Vec<NodeIndex>,
    add_indexes: Vec<NodeIndex>,
    add_fks: Vec<NodeIndex>,
    add_checks: Vec<NodeIndex>,
    enable_rls: Vec<NodeIndex>,
    policies: Vec<NodeIndex>,
    triggers: Vec<NodeIndex>,
    views: Vec<NodeIndex>,
    version_views: Vec<NodeIndex>,
    alter_columns: Vec<NodeIndex>,
    alter_views: Vec<NodeIndex>,
    alter_sequences: Vec<NodeIndex>,
    drop_functions: Vec<NodeIndex>,
    drop_fks: Vec<NodeIndex>,
    drop_indexes: Vec<NodeIndex>,
    drop_checks: Vec<NodeIndex>,
    drop_policies: Vec<NodeIndex>,
    drop_triggers: Vec<NodeIndex>,
    drop_views: Vec<NodeIndex>,
    drop_columns: Vec<NodeIndex>,
    drop_pks: Vec<NodeIndex>,
    drop_tables: Vec<NodeIndex>,
    drop_partitions: Vec<NodeIndex>,
    drop_sequences: Vec<NodeIndex>,
    drop_domains: Vec<NodeIndex>,
    drop_enums: Vec<NodeIndex>,
    drop_extensions: Vec<NodeIndex>,
    drop_version_schemas: Vec<NodeIndex>,
    drop_schemas: Vec<NodeIndex>,
    drop_version_views: Vec<NodeIndex>,
}

impl NodeSets {
    fn new(graph: &MigrationGraph) -> Self {
        Self {
            schemas: graph.nodes_matching(|k| matches!(k, OpKey::CreateSchema(_))),
            version_schemas: graph
                .nodes_matching(|k| matches!(k, OpKey::CreateVersionSchema { .. })),
            extensions: graph.nodes_matching(|k| matches!(k, OpKey::CreateExtension(_))),
            enums: graph.nodes_matching(|k| matches!(k, OpKey::CreateEnum(_))),
            add_enum_values: graph.nodes_matching(|k| matches!(k, OpKey::AddEnumValue { .. })),
            domains: graph.nodes_matching(|k| matches!(k, OpKey::CreateDomain(_))),
            sequences: graph.nodes_matching(|k| matches!(k, OpKey::CreateSequence(_))),
            functions: graph.nodes_matching(|k| matches!(k, OpKey::CreateFunction { .. })),
            alter_functions: graph.nodes_matching(|k| matches!(k, OpKey::AlterFunction { .. })),
            tables: graph.nodes_matching(|k| matches!(k, OpKey::CreateTable(_))),
            partitions: graph.nodes_matching(|k| matches!(k, OpKey::CreatePartition(_))),
            add_columns: graph.nodes_matching(|k| matches!(k, OpKey::AddColumn { .. })),
            add_pks: graph.nodes_matching(|k| matches!(k, OpKey::AddPrimaryKey { .. })),
            add_indexes: graph.nodes_matching(|k| matches!(k, OpKey::AddIndex { .. })),
            add_fks: graph.nodes_matching(|k| matches!(k, OpKey::AddForeignKey { .. })),
            add_checks: graph.nodes_matching(|k| matches!(k, OpKey::AddCheckConstraint { .. })),
            enable_rls: graph.nodes_matching(|k| matches!(k, OpKey::EnableRls { .. })),
            policies: graph.nodes_matching(|k| matches!(k, OpKey::CreatePolicy { .. })),
            triggers: graph.nodes_matching(|k| matches!(k, OpKey::CreateTrigger { .. })),
            views: graph.nodes_matching(|k| matches!(k, OpKey::CreateView(_))),
            version_views: graph.nodes_matching(|k| matches!(k, OpKey::CreateVersionView { .. })),
            alter_columns: graph.nodes_matching(|k| matches!(k, OpKey::AlterColumn { .. })),
            alter_views: graph.nodes_matching(|k| matches!(k, OpKey::AlterView(_))),
            alter_sequences: graph.nodes_matching(|k| matches!(k, OpKey::AlterSequence(_))),
            drop_functions: graph.nodes_matching(|k| matches!(k, OpKey::DropFunction { .. })),
            drop_fks: graph.nodes_matching(|k| matches!(k, OpKey::DropForeignKey { .. })),
            drop_indexes: graph.nodes_matching(|k| matches!(k, OpKey::DropIndex { .. })),
            drop_checks: graph.nodes_matching(|k| matches!(k, OpKey::DropCheckConstraint { .. })),
            drop_policies: graph.nodes_matching(|k| matches!(k, OpKey::DropPolicy { .. })),
            drop_triggers: graph.nodes_matching(|k| matches!(k, OpKey::DropTrigger { .. })),
            drop_views: graph.nodes_matching(|k| matches!(k, OpKey::DropView(_))),
            drop_columns: graph.nodes_matching(|k| matches!(k, OpKey::DropColumn { .. })),
            drop_pks: graph.nodes_matching(|k| matches!(k, OpKey::DropPrimaryKey { .. })),
            drop_tables: graph.nodes_matching(|k| matches!(k, OpKey::DropTable(_))),
            drop_partitions: graph.nodes_matching(|k| matches!(k, OpKey::DropPartition(_))),
            drop_sequences: graph.nodes_matching(|k| matches!(k, OpKey::DropSequence(_))),
            drop_domains: graph.nodes_matching(|k| matches!(k, OpKey::DropDomain(_))),
            drop_enums: graph.nodes_matching(|k| matches!(k, OpKey::DropEnum(_))),
            drop_extensions: graph.nodes_matching(|k| matches!(k, OpKey::DropExtension(_))),
            drop_version_schemas: graph
                .nodes_matching(|k| matches!(k, OpKey::DropVersionSchema { .. })),
            drop_schemas: graph.nodes_matching(|k| matches!(k, OpKey::DropSchema(_))),
            drop_version_views: graph
                .nodes_matching(|k| matches!(k, OpKey::DropVersionView { .. })),
        }
    }
}

/// Graph-based migration planner using explicit dependency edges.
pub(crate) struct MigrationGraph {
    graph: DiGraph<MigrationOp, ()>,
    nodes: HashMap<OpKey, NodeIndex>,
}

impl MigrationGraph {
    pub fn new() -> Self {
        Self {
            graph: DiGraph::new(),
            nodes: HashMap::new(),
        }
    }

    pub fn add_vertex(&mut self, op: MigrationOp) -> NodeIndex {
        let key = OpKey::from_op(&op);
        assert!(!self.nodes.contains_key(&key), "duplicate OpKey: {key:?}");
        let node = self.graph.add_node(op);
        self.nodes.insert(key, node);
        node
    }

    /// Returns true if both nodes exist and the edge was added (from runs before to).
    pub fn add_edge(&mut self, from: &OpKey, to: &OpKey) -> bool {
        if let (Some(&from_node), Some(&to_node)) = (self.nodes.get(from), self.nodes.get(to)) {
            self.graph.add_edge(from_node, to_node, ());
            true
        } else {
            false
        }
    }

    fn nodes_matching<F>(&self, predicate: F) -> Vec<NodeIndex>
    where
        F: Fn(&OpKey) -> bool,
    {
        self.nodes
            .iter()
            .filter(|(key, _)| predicate(key))
            .map(|(_, &node)| node)
            .collect()
    }

    fn edges_all_to_all(&mut self, from: &[NodeIndex], to: &[NodeIndex]) {
        for &f in from {
            for &t in to {
                if f != t {
                    self.graph.add_edge(f, t, ());
                }
            }
        }
    }

    pub fn add_type_level_edges(&mut self) {
        let ns = NodeSets::new(self);
        self.add_schema_infrastructure_edges(&ns);
        self.add_type_system_edges(&ns);
        self.add_function_edges(&ns);
        self.add_table_and_partition_edges(&ns);
        self.add_table_element_edges(&ns);
        self.add_rls_policy_trigger_view_edges(&ns);
        self.add_drop_edges(&ns);
        self.add_alter_column_edges(&ns);
        self.add_drop_column_edges(&ns);
        self.add_modification_pattern_edges(&ns);
        self.add_creates_before_final_drops_edges(&ns);
    }

    /// Tier 1: Schema infrastructure — schemas and version schemas before everything.
    fn add_schema_infrastructure_edges(&mut self, ns: &NodeSets) {
        self.edges_all_to_all(&ns.schemas, &ns.tables);
        self.edges_all_to_all(&ns.schemas, &ns.enums);
        self.edges_all_to_all(&ns.schemas, &ns.domains);
        self.edges_all_to_all(&ns.schemas, &ns.sequences);
        self.edges_all_to_all(&ns.schemas, &ns.functions);
        self.edges_all_to_all(&ns.schemas, &ns.views);
        self.edges_all_to_all(&ns.version_schemas, &ns.version_views);

        self.edges_all_to_all(&ns.extensions, &ns.enums);
        self.edges_all_to_all(&ns.extensions, &ns.domains);
        self.edges_all_to_all(&ns.extensions, &ns.tables);
    }

    /// Tier 2: Type system — enums, enum values, and domains before tables and columns.
    fn add_type_system_edges(&mut self, ns: &NodeSets) {
        self.edges_all_to_all(&ns.enums, &ns.tables);
        self.edges_all_to_all(&ns.enums, &ns.add_columns);
        self.edges_all_to_all(&ns.enums, &ns.add_enum_values);
        self.edges_all_to_all(&ns.add_enum_values, &ns.tables);
        self.edges_all_to_all(&ns.add_enum_values, &ns.add_columns);
        self.edges_all_to_all(&ns.domains, &ns.tables);
        self.edges_all_to_all(&ns.domains, &ns.add_columns);

        self.edges_all_to_all(&ns.enums, &ns.functions);
        self.edges_all_to_all(&ns.domains, &ns.functions);
        self.edges_all_to_all(&ns.add_enum_values, &ns.functions);
        self.edges_all_to_all(&ns.enums, &ns.alter_functions);
        self.edges_all_to_all(&ns.domains, &ns.alter_functions);
        self.edges_all_to_all(&ns.add_enum_values, &ns.alter_functions);
    }

    /// Tier 3: Sequences and functions before tables.
    /// Functions with RETURNS SETOF <table> or %ROWTYPE references are handled per-table
    /// to avoid introducing cycles.
    fn add_function_edges(&mut self, ns: &NodeSets) {
        self.edges_all_to_all(&ns.sequences, &ns.tables);

        // Functions before tables (used in defaults/checks),
        // except functions with RETURNS SETOF <table> or %ROWTYPE references which depend on
        // the table existing first. Per-table granularity: only skip the func→table edge for
        // the specific tables the function depends on, not all tables.
        // AlterFunction carries no body, so %ROWTYPE/SETOF detection is not needed for it.
        for &func_idx in &ns.functions {
            if let MigrationOp::CreateFunction(f) = &self.graph[func_idx] {
                let setof_table = extract_setof_type_ref(&f.return_type).map(|type_ref| {
                    let (s, n) = parse_type_ref(type_ref, &f.schema);
                    qualified_name(&s, &n)
                });
                let rowtype_tables: HashSet<String> =
                    extract_rowtype_references(&f.body, &f.schema)
                        .iter()
                        .map(|r| qualified_name(&r.schema, &r.name))
                        .collect();

                for &table_idx in &ns.tables {
                    if func_idx == table_idx {
                        continue;
                    }
                    let table_qualified = match &self.graph[table_idx] {
                        MigrationOp::CreateTable(t) => qualified_name(&t.schema, &t.name),
                        _ => continue,
                    };
                    let func_depends_on_this_table = setof_table.as_deref()
                        == Some(table_qualified.as_str())
                        || rowtype_tables.contains(&table_qualified);
                    if !func_depends_on_this_table {
                        self.graph.add_edge(func_idx, table_idx, ());
                    }
                }
            } else {
                for &table_idx in &ns.tables {
                    if func_idx != table_idx {
                        self.graph.add_edge(func_idx, table_idx, ());
                    }
                }
            }
        }

        self.edges_all_to_all(&ns.functions, &ns.add_columns);
        self.edges_all_to_all(&ns.functions, &ns.triggers);
        self.edges_all_to_all(&ns.functions, &ns.policies);
    }

    /// Tier 4: Tables before partitions, and tables before all table-level objects.
    fn add_table_and_partition_edges(&mut self, ns: &NodeSets) {
        self.edges_all_to_all(&ns.tables, &ns.partitions);

        self.edges_all_to_all(&ns.tables, &ns.add_columns);
        self.edges_all_to_all(&ns.tables, &ns.add_pks);
        self.edges_all_to_all(&ns.tables, &ns.add_indexes);
        self.edges_all_to_all(&ns.tables, &ns.add_fks);
        self.edges_all_to_all(&ns.tables, &ns.add_checks);
        self.edges_all_to_all(&ns.tables, &ns.enable_rls);
        self.edges_all_to_all(&ns.tables, &ns.policies);
        self.edges_all_to_all(&ns.tables, &ns.triggers);
        self.edges_all_to_all(&ns.tables, &ns.views);
        self.edges_all_to_all(&ns.tables, &ns.alter_sequences);
    }

    /// Tier 5: Table elements — columns before indexes, FKs, checks, views, policies, triggers.
    fn add_table_element_edges(&mut self, ns: &NodeSets) {
        self.edges_all_to_all(&ns.add_columns, &ns.add_indexes);
        self.edges_all_to_all(&ns.add_columns, &ns.add_fks);
        self.edges_all_to_all(&ns.add_columns, &ns.add_checks);

        self.edges_all_to_all(&ns.add_columns, &ns.views);
        self.edges_all_to_all(&ns.add_columns, &ns.alter_views);
        self.edges_all_to_all(&ns.add_columns, &ns.policies);
        self.edges_all_to_all(&ns.add_columns, &ns.triggers);
    }

    /// Tier 6: RLS, policies, triggers, and views — RLS before policies.
    fn add_rls_policy_trigger_view_edges(&mut self, ns: &NodeSets) {
        self.edges_all_to_all(&ns.enable_rls, &ns.policies);
    }

    /// Tier 8 (reverse): Drop operations in reverse creation order.
    fn add_drop_edges(&mut self, ns: &NodeSets) {
        self.edges_all_to_all(&ns.drop_fks, &ns.drop_tables);
        self.edges_all_to_all(&ns.drop_indexes, &ns.drop_tables);
        self.edges_all_to_all(&ns.drop_checks, &ns.drop_tables);
        self.edges_all_to_all(&ns.drop_policies, &ns.drop_tables);
        self.edges_all_to_all(&ns.drop_triggers, &ns.drop_tables);
        self.edges_all_to_all(&ns.drop_pks, &ns.drop_tables);
        self.edges_all_to_all(&ns.drop_columns, &ns.drop_tables);

        self.edges_all_to_all(&ns.drop_partitions, &ns.drop_tables);

        self.edges_all_to_all(&ns.drop_views, &ns.drop_tables);

        self.edges_all_to_all(&ns.drop_version_views, &ns.drop_version_schemas);

        self.edges_all_to_all(&ns.drop_tables, &ns.drop_schemas);
        self.edges_all_to_all(&ns.drop_tables, &ns.drop_enums);
        self.edges_all_to_all(&ns.drop_tables, &ns.drop_domains);
        self.edges_all_to_all(&ns.drop_tables, &ns.drop_sequences);

        self.edges_all_to_all(&ns.drop_sequences, &ns.drop_extensions);

        self.edges_all_to_all(&ns.drop_enums, &ns.drop_extensions);
        self.edges_all_to_all(&ns.drop_domains, &ns.drop_extensions);

        self.edges_all_to_all(&ns.drop_extensions, &ns.drop_schemas);
    }

    /// ALTER column dependencies: drop constraints before alter, recreate after.
    fn add_alter_column_edges(&mut self, ns: &NodeSets) {
        self.edges_all_to_all(&ns.drop_fks, &ns.alter_columns);
        self.edges_all_to_all(&ns.drop_indexes, &ns.alter_columns);
        self.edges_all_to_all(&ns.drop_policies, &ns.alter_columns);
        self.edges_all_to_all(&ns.drop_triggers, &ns.alter_columns);
        self.edges_all_to_all(&ns.drop_views, &ns.alter_columns);

        // Pattern: DropX → AlterColumn → CreateX
        self.edges_all_to_all(&ns.alter_columns, &ns.add_fks);
        self.edges_all_to_all(&ns.alter_columns, &ns.add_indexes);
        self.edges_all_to_all(&ns.alter_columns, &ns.policies);
        self.edges_all_to_all(&ns.alter_columns, &ns.triggers);
        self.edges_all_to_all(&ns.alter_columns, &ns.views);
        self.edges_all_to_all(&ns.alter_columns, &ns.alter_views);
    }

    /// DROP COLUMN dependencies: drop dependent objects before dropping column, recreate after.
    fn add_drop_column_edges(&mut self, ns: &NodeSets) {
        self.edges_all_to_all(&ns.drop_policies, &ns.drop_columns);
        self.edges_all_to_all(&ns.drop_triggers, &ns.drop_columns);
        self.edges_all_to_all(&ns.drop_views, &ns.drop_columns);

        self.edges_all_to_all(&ns.drop_columns, &ns.policies);
        self.edges_all_to_all(&ns.drop_columns, &ns.triggers);
        self.edges_all_to_all(&ns.drop_columns, &ns.views);
        self.edges_all_to_all(&ns.drop_columns, &ns.alter_views);
    }

    /// Modification patterns: when objects are dropped and recreated, drop before create.
    fn add_modification_pattern_edges(&mut self, ns: &NodeSets) {
        self.edges_all_to_all(&ns.drop_functions, &ns.functions);
        self.edges_all_to_all(&ns.drop_indexes, &ns.add_indexes);
        self.edges_all_to_all(&ns.drop_fks, &ns.add_fks);
        self.edges_all_to_all(&ns.drop_checks, &ns.add_checks);
        self.edges_all_to_all(&ns.drop_policies, &ns.policies);
        self.edges_all_to_all(&ns.drop_triggers, &ns.triggers);
        self.edges_all_to_all(&ns.drop_views, &ns.views);
    }

    /// All create/alter operations must complete before final drop operations.
    /// Excludes drops that must precede creates/alters (DropFunction, DropFK, etc.).
    fn add_creates_before_final_drops_edges(&mut self, ns: &NodeSets) {
        // Create operations that should complete before final drops.
        //
        // policies, triggers, and views are excluded: when columns are dropped,
        // these objects are dropped before the column and recreated after
        // (add_drop_column_edges: drop_columns -> policies/triggers/views).
        // Including them here would create a cycle because drop_columns is in
        // final_drops (policies -> drop_columns -> policies).
        //
        // This is safe because policies/triggers/views never need to precede
        // unrelated final drops — their dependencies are on the tables they
        // belong to, which ARE in all_creates.
        let all_creates: Vec<NodeIndex> = [
            &ns.schemas,
            &ns.version_schemas,
            &ns.extensions,
            &ns.enums,
            &ns.add_enum_values,
            &ns.domains,
            &ns.sequences,
            &ns.functions,
            &ns.tables,
            &ns.partitions,
            &ns.add_columns,
            &ns.add_pks,
            &ns.add_indexes,
            &ns.add_fks,
            &ns.add_checks,
            &ns.enable_rls,
            &ns.version_views,
            &ns.alter_columns,
            &ns.alter_views,
            &ns.alter_sequences,
        ]
        .into_iter()
        .flatten()
        .copied()
        .collect();

        // Final drops (not temporary drops for modifications)
        // Note: DropFK, DropIndex, DropPolicy, DropTrigger, DropView, DropFunction
        // are excluded because they may need to happen before alters/creates
        let final_drops: Vec<NodeIndex> = [
            &ns.drop_columns,
            &ns.drop_pks,
            &ns.drop_tables,
            &ns.drop_partitions,
            &ns.drop_sequences,
            &ns.drop_domains,
            &ns.drop_enums,
            &ns.drop_extensions,
            &ns.drop_version_schemas,
            &ns.drop_schemas,
            &ns.drop_version_views,
        ]
        .into_iter()
        .flatten()
        .copied()
        .collect();

        self.edges_all_to_all(&all_creates, &final_drops);
    }

    fn get_op(&self, key: &OpKey) -> Option<&MigrationOp> {
        self.nodes.get(key).map(|&idx| &self.graph[idx])
    }

    pub fn add_content_aware_edges(&mut self) {
        let keys: Vec<_> = self.nodes.keys().cloned().collect();
        let mut edges_to_add: Vec<(OpKey, OpKey)> = Vec::new();

        for key in &keys {
            match key {
                // CreateTable with FKs depends on referenced tables existing,
                // and column defaults may reference functions
                OpKey::CreateTable(table_name) => {
                    if let Some(MigrationOp::CreateTable(table)) = self.get_op(key) {
                        for fk in &table.foreign_keys {
                            let ref_qualified =
                                qualified_name(&fk.referenced_schema, &fk.referenced_table);
                            // Skip self-referencing FKs - PostgreSQL handles these correctly
                            // when the FK is defined inline with CREATE TABLE. No dependency
                            // edge is needed because the table doesn't depend on itself.
                            if ref_qualified != *table_name {
                                edges_to_add.push((OpKey::CreateTable(ref_qualified), key.clone()));
                            }
                        }

                        for column in table.columns.values() {
                            if let Some(default) = &column.default {
                                push_function_ref_edges(
                                    &mut edges_to_add,
                                    &keys,
                                    default,
                                    &table.schema,
                                    key,
                                );
                            }
                        }
                    }
                }

                // FK depends on referenced table existing (for separate AddForeignKey ops)
                OpKey::AddForeignKey { .. } => {
                    if let Some(MigrationOp::AddForeignKey { foreign_key, .. }) = self.get_op(key) {
                        let ref_table = foreign_key.referenced_table.clone();
                        let ref_schema = foreign_key.referenced_schema.clone();
                        let qualified = qualified_name(&ref_schema, &ref_table);
                        edges_to_add.push((OpKey::CreateTable(qualified), key.clone()));
                    }
                }

                // DropView of a derived view must happen before DropView of its base view.
                // We look up the corresponding CreateView op to find the view's references.
                OpKey::DropView(view_name) => {
                    if let Some(MigrationOp::CreateView(view)) =
                        self.get_op(&OpKey::CreateView(view_name.clone()))
                    {
                        let refs = extract_relation_references(&view.query);
                        for ref_name in refs {
                            if ref_name != *view_name {
                                edges_to_add
                                    .push((key.clone(), OpKey::DropView(ref_name)));
                            }
                        }
                    }
                }

                // CreateView depends on tables/views/functions it references in its query
                OpKey::CreateView(view_name) => {
                    if let Some(MigrationOp::CreateView(view)) = self.get_op(key) {
                        let refs = extract_relation_references(&view.query);
                        for ref_name in refs {
                            if ref_name != *view_name {
                                edges_to_add
                                    .push((OpKey::CreateTable(ref_name.clone()), key.clone()));
                                edges_to_add.push((OpKey::CreateView(ref_name), key.clone()));
                            }
                        }

                        push_function_ref_edges(
                            &mut edges_to_add,
                            &keys,
                            &view.query,
                            &view.schema,
                            key,
                        );
                    }
                }

                // CreateFunction depends on other functions it calls in its body
                OpKey::CreateFunction {
                    name: func_name, ..
                } => {
                    if let Some(MigrationOp::CreateFunction(func)) = self.get_op(key) {
                        for ref_obj in extract_function_references(&func.body, &func.schema) {
                            let ref_qualified = qualified_name(&ref_obj.schema, &ref_obj.name);
                            if ref_qualified != *func_name {
                                push_function_edges(&mut edges_to_add, &keys, &ref_qualified, key);
                            }
                        }

                        // Functions with RETURNS SETOF <table> depend on the referenced table
                        if let Some(type_ref) = extract_setof_type_ref(&func.return_type) {
                            let (ref_schema, ref_name) = parse_type_ref(type_ref, &func.schema);
                            let ref_qualified = qualified_name(&ref_schema, &ref_name);
                            edges_to_add.push((OpKey::CreateTable(ref_qualified), key.clone()));
                        }

                        for ref_obj in extract_rowtype_references(&func.body, &func.schema) {
                            let ref_qualified = qualified_name(&ref_obj.schema, &ref_obj.name);
                            edges_to_add.push((OpKey::CreateTable(ref_qualified), key.clone()));
                        }
                    }
                }

                // Trigger depends on its target table and its trigger function
                OpKey::CreateTrigger { target, .. } => {
                    edges_to_add.push((OpKey::CreateTable(target.to_string()), key.clone()));

                    if let Some(MigrationOp::CreateTrigger(trigger)) = self.get_op(key) {
                        let func_qualified =
                            qualified_name(&trigger.function_schema, &trigger.function_name);
                        push_function_edges(&mut edges_to_add, &keys, &func_qualified, key);
                    }
                }

                // Policy depends on its table, tables/views referenced in expressions, and functions
                OpKey::CreatePolicy { table, .. } => {
                    edges_to_add.push((OpKey::CreateTable(table.to_string()), key.clone()));

                    if let Some(MigrationOp::CreatePolicy(policy)) = self.get_op(key) {
                        let schema = &policy.table_schema;
                        for expr in [&policy.using_expr, &policy.check_expr]
                            .into_iter()
                            .flatten()
                        {
                            push_expression_ref_edges(&mut edges_to_add, &keys, expr, schema, key);
                        }
                    }
                }

                // Index depends on its table and functions in expressions/predicates
                OpKey::AddIndex { table, .. } => {
                    edges_to_add.push((OpKey::CreateTable(table.to_string()), key.clone()));

                    if let Some(MigrationOp::AddIndex { table, index }) = self.get_op(key) {
                        let schema = &table.schema;
                        for col in &index.columns {
                            push_function_ref_edges(&mut edges_to_add, &keys, col, schema, key);
                        }
                        if let Some(predicate) = &index.predicate {
                            push_function_ref_edges(
                                &mut edges_to_add,
                                &keys,
                                predicate,
                                schema,
                                key,
                            );
                        }
                    }
                }

                // AddColumn depends on table and functions in defaults
                OpKey::AddColumn { table, .. } => {
                    edges_to_add.push((OpKey::CreateTable(table.to_string()), key.clone()));

                    if let Some(MigrationOp::AddColumn { table, column }) = self.get_op(key) {
                        if let Some(default) = &column.default {
                            let schema = &table.schema;
                            push_function_ref_edges(&mut edges_to_add, &keys, default, schema, key);
                        }
                    }
                }

                // AddCheckConstraint depends on table and functions in expression
                OpKey::AddCheckConstraint { table, .. } => {
                    edges_to_add.push((OpKey::CreateTable(table.to_string()), key.clone()));

                    if let Some(MigrationOp::AddCheckConstraint {
                        table,
                        check_constraint,
                    }) = self.get_op(key)
                    {
                        let schema = &table.schema;
                        push_function_ref_edges(
                            &mut edges_to_add,
                            &keys,
                            &check_constraint.expression,
                            schema,
                            key,
                        );
                    }
                }

                // NOTE: CreateDomain can reference functions in CHECK constraints and
                // defaults, but adding function→domain edges would conflict with the
                // phase-level domain→function ordering (domains are types used in function
                // signatures). PostgreSQL resolves this naturally: the domain is created
                // first, then the function, and the CHECK is validated lazily.

                // DropColumn must happen after DropFK/DropIndex/DropCheck/DropPolicy/DropTrigger on that table
                OpKey::DropColumn { table, .. } => {
                    for other in &keys {
                        if drop_targets_table(other, table)
                            && matches!(
                                other,
                                OpKey::DropForeignKey { .. }
                                    | OpKey::DropIndex { .. }
                                    | OpKey::DropCheckConstraint { .. }
                                    | OpKey::DropPolicy { .. }
                                    | OpKey::DropTrigger { .. }
                            )
                        {
                            edges_to_add.push((other.clone(), key.clone()));
                        }
                    }
                }

                // DropTable must happen after dropping all table objects
                OpKey::DropTable(table) => {
                    let (schema, name) = parse_qualified_name(table);
                    let qualified = QualifiedName::new(&schema, &name);
                    for other in &keys {
                        if drop_targets_table(other, &qualified) {
                            edges_to_add.push((other.clone(), key.clone()));
                        }
                    }
                }

                // AlterPolicy depends on tables/views and functions in new expressions
                OpKey::AlterPolicy { table, .. } => {
                    if let Some(MigrationOp::AlterPolicy { changes, .. }) = self.get_op(key) {
                        let schema = &table.schema;
                        for expr in [&changes.using_expr, &changes.check_expr]
                            .into_iter()
                            .flatten()
                            .flatten()
                        {
                            push_expression_ref_edges(&mut edges_to_add, &keys, expr, schema, key);
                        }
                    }
                }

                // AlterView depends on functions in replacement query
                OpKey::AlterView(view_name) => {
                    if let Some(MigrationOp::AlterView { new_view, .. }) = self.get_op(key) {
                        let refs = extract_relation_references(&new_view.query);
                        for ref_name in refs {
                            if ref_name != *view_name {
                                edges_to_add
                                    .push((OpKey::CreateTable(ref_name.clone()), key.clone()));
                                edges_to_add.push((OpKey::CreateView(ref_name), key.clone()));
                            }
                        }
                        push_function_ref_edges(
                            &mut edges_to_add,
                            &keys,
                            &new_view.query,
                            &new_view.schema,
                            key,
                        );
                    }
                }

                // AlterColumn must happen after dropping dependent objects,
                // and new defaults may reference functions
                OpKey::AlterColumn { table, .. } => {
                    for other in &keys {
                        if drop_targets_table(other, table)
                            && matches!(
                                other,
                                OpKey::DropForeignKey { .. }
                                    | OpKey::DropIndex { .. }
                                    | OpKey::DropPolicy { .. }
                                    | OpKey::DropTrigger { .. }
                            )
                        {
                            edges_to_add.push((other.clone(), key.clone()));
                        }
                    }

                    if let Some(MigrationOp::AlterColumn { table, changes, .. }) = self.get_op(key)
                    {
                        if let Some(Some(default_expr)) = &changes.default {
                            let schema = &table.schema;
                            push_function_ref_edges(
                                &mut edges_to_add,
                                &keys,
                                default_expr,
                                schema,
                                key,
                            );
                        }
                    }
                }

                // BackfillHint depends on column existing
                OpKey::BackfillHint { table, column } => {
                    edges_to_add.push((
                        OpKey::AddColumn {
                            table: table.clone(),
                            column: column.clone(),
                        },
                        key.clone(),
                    ));
                }

                // SetColumnNotNull depends on backfill completing and column existing
                OpKey::SetColumnNotNull { table, column } => {
                    // Depends on backfill (if present)
                    edges_to_add.push((
                        OpKey::BackfillHint {
                            table: table.clone(),
                            column: column.clone(),
                        },
                        key.clone(),
                    ));
                    // Depends on column existing
                    edges_to_add.push((
                        OpKey::AddColumn {
                            table: table.clone(),
                            column: column.clone(),
                        },
                        key.clone(),
                    ));
                }

                // AlterOwner depends on the object existing
                OpKey::AlterOwner {
                    object_kind,
                    schema,
                    name,
                } => {
                    let qualified = qualified_name(schema, name);
                    match object_kind {
                        OwnerObjectKind::Table => {
                            edges_to_add.push((OpKey::CreateTable(qualified), key.clone()));
                        }
                        OwnerObjectKind::Partition => {
                            edges_to_add.push((OpKey::CreatePartition(qualified), key.clone()));
                        }
                        OwnerObjectKind::View | OwnerObjectKind::MaterializedView => {
                            edges_to_add.push((OpKey::CreateView(qualified), key.clone()));
                        }
                        OwnerObjectKind::Sequence => {
                            edges_to_add.push((OpKey::CreateSequence(qualified), key.clone()));
                        }
                        OwnerObjectKind::Type => {
                            edges_to_add.push((OpKey::CreateEnum(qualified), key.clone()));
                        }
                        OwnerObjectKind::Domain => {
                            edges_to_add.push((OpKey::CreateDomain(qualified), key.clone()));
                        }
                        OwnerObjectKind::Function => {
                            if let Some(MigrationOp::AlterOwner {
                                args: Some(args), ..
                            }) = self.get_op(key)
                            {
                                edges_to_add.push((
                                    OpKey::CreateFunction {
                                        name: qualified,
                                        args: args.clone(),
                                    },
                                    key.clone(),
                                ));
                            }
                        }
                    }
                }

                // Grant/RevokePrivileges depend on the object existing
                OpKey::GrantPrivileges {
                    object_kind,
                    schema,
                    name,
                    ..
                }
                | OpKey::RevokePrivileges {
                    object_kind,
                    schema,
                    name,
                    ..
                } => {
                    let args = match self.get_op(key) {
                        Some(MigrationOp::GrantPrivileges { args: Some(a), .. })
                        | Some(MigrationOp::RevokePrivileges { args: Some(a), .. }) => {
                            Some(a.clone())
                        }
                        _ => None,
                    };
                    add_privilege_dependency_edge(
                        &mut edges_to_add,
                        object_kind,
                        schema,
                        name,
                        args.as_ref(),
                        key,
                    );
                }

                _ => {}
            }
        }

        // Add all collected edges
        for (from, to) in edges_to_add {
            self.add_edge(&from, &to);
        }
    }

    pub fn topological_sort(&self) -> Result<Vec<MigrationOp>, PlanError> {
        let sorted = toposort(&self.graph, None).map_err(|cycle| {
            let node = cycle.node_id();
            let op = &self.graph[node];
            PlanError::CyclicDependency(format!("{op:?}"))
        })?;

        Ok(sorted
            .into_iter()
            .map(|node| self.graph[node].clone())
            .collect())
    }
}

impl Default for MigrationGraph {
    fn default() -> Self {
        Self::new()
    }
}

fn push_function_edges(
    edges: &mut Vec<(OpKey, OpKey)>,
    keys: &[OpKey],
    ref_qualified: &str,
    consumer_key: &OpKey,
) {
    for other_key in keys {
        if let OpKey::CreateFunction {
            name: other_name, ..
        } = other_key
        {
            if *other_name == ref_qualified {
                edges.push((other_key.clone(), consumer_key.clone()));
            }
        }
    }
}

fn push_function_ref_edges(
    edges: &mut Vec<(OpKey, OpKey)>,
    keys: &[OpKey],
    expression: &str,
    default_schema: &str,
    consumer_key: &OpKey,
) {
    for ref_obj in extract_function_references(expression, default_schema) {
        let ref_qualified = qualified_name(&ref_obj.schema, &ref_obj.name);
        push_function_edges(edges, keys, &ref_qualified, consumer_key);
    }
}

fn push_relation_ref_edges(
    edges: &mut Vec<(OpKey, OpKey)>,
    expression: &str,
    consumer_key: &OpKey,
) {
    for ref_name in extract_relation_references(expression) {
        edges.push((OpKey::CreateTable(ref_name.clone()), consumer_key.clone()));
        edges.push((OpKey::CreateView(ref_name), consumer_key.clone()));
    }
}

fn push_expression_ref_edges(
    edges: &mut Vec<(OpKey, OpKey)>,
    keys: &[OpKey],
    expression: &str,
    default_schema: &str,
    consumer_key: &OpKey,
) {
    push_relation_ref_edges(edges, expression, consumer_key);
    push_function_ref_edges(edges, keys, expression, default_schema, consumer_key);
}

fn drop_targets_table(other: &OpKey, table: &QualifiedName) -> bool {
    match other {
        OpKey::DropForeignKey { table: t, .. }
        | OpKey::DropIndex { table: t, .. }
        | OpKey::DropCheckConstraint { table: t, .. }
        | OpKey::DropColumn { table: t, .. }
        | OpKey::DropPolicy { table: t, .. } => t == table,
        OpKey::DropTrigger { target: t, .. } => t == table,
        _ => false,
    }
}

pub fn plan_migration_checked(ops: Vec<MigrationOp>) -> Result<Vec<MigrationOp>, PlanError> {
    let processed_ops = split_sequence_owned_by_ops(ops);

    let mut graph = MigrationGraph::new();
    for op in processed_ops {
        graph.add_vertex(op);
    }
    graph.add_type_level_edges();
    graph.add_content_aware_edges();

    graph.topological_sort()
}

fn split_sequence_owned_by_ops(ops: Vec<MigrationOp>) -> Vec<MigrationOp> {
    let mut result = Vec::new();

    for op in ops {
        match op {
            MigrationOp::CreateSequence(ref seq) if seq.owned_by.is_some() => {
                let mut seq_without_owner = seq.clone();
                let owned_by = seq_without_owner.owned_by.take();
                result.push(MigrationOp::CreateSequence(seq_without_owner));
                result.push(MigrationOp::AlterSequence {
                    name: qualified_name(&seq.schema, &seq.name),
                    changes: super::SequenceChanges {
                        owned_by: Some(owned_by),
                        ..Default::default()
                    },
                });
            }
            _ => result.push(op),
        }
    }

    result
}

/// Test-only convenience wrapper that panics on circular dependencies.
/// Production code should use [`plan_migration_checked`] instead.
pub fn plan_migration(ops: Vec<MigrationOp>) -> Vec<MigrationOp> {
    plan_migration_checked(ops).expect("Circular dependency detected in migration operations")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diff::test_helpers::simple_table_with_fks;
    use crate::diff::{ColumnChanges, OwnerObjectKind, PolicyChanges};
    use crate::model::*;
    use std::collections::BTreeMap;

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
        let posts = simple_table_with_fks("posts", vec![make_fk("users")]);
        let users = simple_table_with_fks("users", vec![]);
        let comments = simple_table_with_fks("comments", vec![make_fk("posts"), make_fk("users")]);

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
        let users = simple_table_with_fks("users", vec![]);

        let ops = vec![
            MigrationOp::DropTable("old_table".to_string()),
            MigrationOp::CreateTable(users),
            MigrationOp::DropColumn {
                table: QualifiedName::new("public", "foo"),
                column: "bar".to_string(),
            },
            MigrationOp::AddColumn {
                table: QualifiedName::new("public", "foo"),
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
                table: QualifiedName::new("public", "posts"),
                column: "user_id".to_string(),
            },
            MigrationOp::DropForeignKey {
                table: QualifiedName::new("public", "posts"),
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
                table: QualifiedName::new("public", "users"),
                index: Index {
                    name: "users_email_idx".to_string(),
                    columns: vec!["email".to_string()],
                    unique: true,
                    index_type: IndexType::BTree,
                    predicate: None,
                    is_constraint: false,
                },
            },
            MigrationOp::AddColumn {
                table: QualifiedName::new("public", "users"),
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
            MigrationOp::CreateTable(simple_table_with_fks("users", vec![])),
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
            MigrationOp::CreateTable(simple_table_with_fks("users", vec![])),
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
        let table = simple_table_with_fks("users", vec![]);

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
            is_constraint: false,
        };

        let ops = vec![
            MigrationOp::AddIndex {
                table: QualifiedName::new("public", "users"),
                index: index.clone(),
            },
            MigrationOp::DropIndex {
                table: QualifiedName::new("public", "users"),
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

    #[test]
    fn drop_fk_before_alter_column_type() {
        // When altering a column's type that is involved in a FK,
        // the FK must be dropped before the ALTER and re-added after.
        // This test verifies: DropForeignKey → AlterColumn → AddForeignKey
        let fk = ForeignKey {
            name: "posts_user_id_fkey".to_string(),
            columns: vec!["user_id".to_string()],
            referenced_table: "users".to_string(),
            referenced_schema: "public".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: ReferentialAction::NoAction,
            on_update: ReferentialAction::NoAction,
        };

        let ops = vec![
            MigrationOp::AlterColumn {
                table: QualifiedName::new("public", "posts"),
                column: "user_id".to_string(),
                changes: crate::diff::ColumnChanges {
                    data_type: Some(PgType::Uuid),
                    nullable: None,
                    default: None,
                },
            },
            MigrationOp::DropForeignKey {
                table: QualifiedName::new("public", "posts"),
                foreign_key_name: "posts_user_id_fkey".to_string(),
            },
            MigrationOp::AddForeignKey {
                table: QualifiedName::new("public", "posts"),
                foreign_key: fk,
            },
        ];

        let planned = plan_migration(ops);

        let drop_fk_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropForeignKey { .. }))
            .unwrap();
        let alter_col_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::AlterColumn { .. }))
            .unwrap();
        let add_fk_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::AddForeignKey { .. }))
            .unwrap();

        assert!(
            drop_fk_pos < alter_col_pos,
            "DropForeignKey must come before AlterColumn. DROP_FK at {drop_fk_pos}, ALTER at {alter_col_pos}"
        );
        assert!(
            alter_col_pos < add_fk_pos,
            "AlterColumn must come before AddForeignKey. ALTER at {alter_col_pos}, ADD_FK at {add_fk_pos}"
        );
    }

    #[test]
    fn drop_policy_before_alter_column_type() {
        // When altering a column's type that is referenced by a policy,
        // the policy must be dropped before the ALTER and re-created after.
        // This test verifies: DropPolicy → AlterColumn → CreatePolicy
        let policy = Policy {
            name: "users_select_policy".to_string(),
            table_schema: "public".to_string(),
            table: "users".to_string(),
            command: PolicyCommand::Select,
            roles: vec!["authenticated".to_string()],
            using_expr: Some("id = current_user_id()".to_string()),
            check_expr: None,
        };

        let ops = vec![
            MigrationOp::AlterColumn {
                table: QualifiedName::new("public", "users"),
                column: "id".to_string(),
                changes: crate::diff::ColumnChanges {
                    data_type: Some(PgType::Uuid),
                    nullable: None,
                    default: None,
                },
            },
            MigrationOp::DropPolicy {
                table: QualifiedName::new("public", "users"),
                name: "users_select_policy".to_string(),
            },
            MigrationOp::CreatePolicy(policy),
        ];

        let planned = plan_migration(ops);

        let drop_policy_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropPolicy { .. }))
            .unwrap();
        let alter_col_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::AlterColumn { .. }))
            .unwrap();
        let create_policy_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreatePolicy(_)))
            .unwrap();

        assert!(
            drop_policy_pos < alter_col_pos,
            "DropPolicy must come before AlterColumn. DROP_POLICY at {drop_policy_pos}, ALTER at {alter_col_pos}"
        );
        assert!(
            alter_col_pos < create_policy_pos,
            "AlterColumn must come before CreatePolicy. ALTER at {alter_col_pos}, CREATE_POLICY at {create_policy_pos}"
        );
    }

    #[test]
    fn drop_trigger_before_alter_column_type() {
        // When altering a column's type that is referenced by a trigger,
        // the trigger must be dropped before the ALTER and re-created after.
        // This test verifies: DropTrigger → AlterColumn → CreateTrigger
        let trigger = Trigger {
            name: "users_update_trigger".to_string(),
            target_schema: "public".to_string(),
            target_name: "users".to_string(),
            timing: TriggerTiming::Before,
            events: vec![TriggerEvent::Update],
            update_columns: vec!["id".to_string()],
            for_each_row: true,
            when_clause: None,
            function_schema: "public".to_string(),
            function_name: "update_timestamp".to_string(),
            function_args: vec![],
            enabled: TriggerEnabled::Origin,
            old_table_name: None,
            new_table_name: None,
        };

        let ops = vec![
            MigrationOp::AlterColumn {
                table: QualifiedName::new("public", "users"),
                column: "id".to_string(),
                changes: crate::diff::ColumnChanges {
                    data_type: Some(PgType::Uuid),
                    nullable: None,
                    default: None,
                },
            },
            MigrationOp::DropTrigger {
                target_schema: "public".to_string(),
                target_name: "users".to_string(),
                name: "users_update_trigger".to_string(),
            },
            MigrationOp::CreateTrigger(trigger),
        ];

        let planned = plan_migration(ops);

        let drop_trigger_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropTrigger { .. }))
            .unwrap();
        let alter_col_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::AlterColumn { .. }))
            .unwrap();
        let create_trigger_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTrigger(_)))
            .unwrap();

        assert!(
            drop_trigger_pos < alter_col_pos,
            "DropTrigger must come before AlterColumn. DROP_TRIGGER at {drop_trigger_pos}, ALTER at {alter_col_pos}"
        );
        assert!(
            alter_col_pos < create_trigger_pos,
            "AlterColumn must come before CreateTrigger. ALTER at {alter_col_pos}, CREATE_TRIGGER at {create_trigger_pos}"
        );
    }

    #[test]
    fn drop_view_before_alter_column_type() {
        // When altering a column's type that is referenced by a view,
        // the view must be dropped before the ALTER and re-created after.
        // This test verifies: DropView → AlterColumn → CreateView
        let view = View {
            name: "users_view".to_string(),
            schema: "public".to_string(),
            query: "SELECT id, name FROM users".to_string(),
            materialized: false,
            owner: None,
            grants: vec![],
        };

        let ops = vec![
            MigrationOp::AlterColumn {
                table: QualifiedName::new("public", "users"),
                column: "id".to_string(),
                changes: crate::diff::ColumnChanges {
                    data_type: Some(PgType::Uuid),
                    nullable: None,
                    default: None,
                },
            },
            MigrationOp::DropView {
                name: "public.users_view".to_string(),
                materialized: false,
            },
            MigrationOp::CreateView(view),
        ];

        let planned = plan_migration(ops);

        let drop_view_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropView { .. }))
            .unwrap();
        let alter_col_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::AlterColumn { .. }))
            .unwrap();
        let create_view_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateView(_)))
            .unwrap();

        assert!(
            drop_view_pos < alter_col_pos,
            "DropView must come before AlterColumn. DROP_VIEW at {drop_view_pos}, ALTER at {alter_col_pos}"
        );
        assert!(
            alter_col_pos < create_view_pos,
            "AlterColumn must come before CreateView. ALTER at {alter_col_pos}, CREATE_VIEW at {create_view_pos}"
        );
    }

    #[test]
    fn drop_policy_before_drop_column() {
        let policy = Policy {
            name: "users_select_policy".to_string(),
            table_schema: "public".to_string(),
            table: "users".to_string(),
            command: PolicyCommand::Select,
            roles: vec!["authenticated".to_string()],
            using_expr: Some("enterprise_id = current_enterprise_id()".to_string()),
            check_expr: None,
        };

        let ops = vec![
            MigrationOp::DropColumn {
                table: QualifiedName::new("public", "users"),
                column: "enterprise_id".to_string(),
            },
            MigrationOp::DropPolicy {
                table: QualifiedName::new("public", "users"),
                name: "users_select_policy".to_string(),
            },
            MigrationOp::CreatePolicy(policy),
        ];

        let planned = plan_migration(ops);

        let drop_policy_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropPolicy { .. }))
            .unwrap();
        let drop_column_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropColumn { .. }))
            .unwrap();
        let create_policy_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreatePolicy(_)))
            .unwrap();

        assert!(
            drop_policy_pos < drop_column_pos,
            "DropPolicy must come before DropColumn. DROP_POLICY at {drop_policy_pos}, DROP_COLUMN at {drop_column_pos}"
        );
        assert!(
            drop_column_pos < create_policy_pos,
            "DropColumn must come before CreatePolicy. DROP_COLUMN at {drop_column_pos}, CREATE_POLICY at {create_policy_pos}"
        );
    }

    #[test]
    fn drop_trigger_before_drop_column() {
        let trigger = Trigger {
            name: "audit_trigger".to_string(),
            target_schema: "public".to_string(),
            target_name: "users".to_string(),
            timing: TriggerTiming::Before,
            events: vec![TriggerEvent::Update],
            update_columns: vec!["enterprise_id".to_string()],
            for_each_row: true,
            when_clause: None,
            function_schema: "public".to_string(),
            function_name: "audit_changes".to_string(),
            function_args: vec![],
            enabled: TriggerEnabled::Origin,
            old_table_name: None,
            new_table_name: None,
        };

        let ops = vec![
            MigrationOp::DropColumn {
                table: QualifiedName::new("public", "users"),
                column: "enterprise_id".to_string(),
            },
            MigrationOp::DropTrigger {
                target_schema: "public".to_string(),
                target_name: "users".to_string(),
                name: "audit_trigger".to_string(),
            },
            MigrationOp::CreateTrigger(trigger),
        ];

        let planned = plan_migration(ops);

        let drop_trigger_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropTrigger { .. }))
            .unwrap();
        let drop_column_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropColumn { .. }))
            .unwrap();

        assert!(
            drop_trigger_pos < drop_column_pos,
            "DropTrigger must come before DropColumn. DROP_TRIGGER at {drop_trigger_pos}, DROP_COLUMN at {drop_column_pos}"
        );
    }

    #[test]
    fn drop_view_before_drop_column() {
        let view = View {
            name: "users_view".to_string(),
            schema: "public".to_string(),
            query: "SELECT id, name FROM users".to_string(),
            materialized: false,
            owner: None,
            grants: vec![],
        };

        let ops = vec![
            MigrationOp::DropColumn {
                table: QualifiedName::new("public", "users"),
                column: "enterprise_id".to_string(),
            },
            MigrationOp::DropView {
                name: "public.users_view".to_string(),
                materialized: false,
            },
            MigrationOp::CreateView(view),
        ];

        let planned = plan_migration(ops);

        let drop_view_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropView { .. }))
            .unwrap();
        let drop_column_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropColumn { .. }))
            .unwrap();

        assert!(
            drop_view_pos < drop_column_pos,
            "DropView must come before DropColumn. DROP_VIEW at {drop_view_pos}, DROP_COLUMN at {drop_column_pos}"
        );
    }

    // === Graph planner v2 tests ===

    #[test]
    fn v2_basic_create_table() {
        let users = simple_table_with_fks("users", vec![]);
        let ops = vec![MigrationOp::CreateTable(users)];

        let v2_result = plan_migration_checked(ops.clone()).unwrap();
        let bucket_result = plan_migration(ops);

        assert_eq!(v2_result.len(), bucket_result.len());
    }

    #[test]
    fn v2_fk_dependencies() {
        let posts = simple_table_with_fks("posts", vec![make_fk("users")]);
        let users = simple_table_with_fks("users", vec![]);

        let ops = vec![
            MigrationOp::CreateTable(posts),
            MigrationOp::CreateTable(users),
        ];

        let v2_result = plan_migration_checked(ops).unwrap();

        // Users should come before posts due to FK dependency
        let users_pos = v2_result
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTable(t) if t.name == "users"))
            .unwrap();
        let posts_pos = v2_result
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTable(t) if t.name == "posts"))
            .unwrap();

        assert!(
            users_pos < posts_pos,
            "users should be created before posts (FK dependency)"
        );
    }

    #[test]
    fn v2_enum_before_table() {
        let my_enum = EnumType {
            name: "status".to_string(),
            schema: "public".to_string(),
            values: vec!["active".to_string(), "inactive".to_string()],
            owner: None,
            grants: vec![],
        };
        let users = simple_table_with_fks("users", vec![]);

        let ops = vec![
            MigrationOp::CreateTable(users),
            MigrationOp::CreateEnum(my_enum),
        ];

        let v2_result = plan_migration_checked(ops).unwrap();

        let enum_pos = v2_result
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateEnum(_)))
            .unwrap();
        let table_pos = v2_result
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTable(_)))
            .unwrap();

        assert!(enum_pos < table_pos, "enum should be created before table");
    }

    #[test]
    fn v2_drop_fk_before_alter_column() {
        let ops = vec![
            MigrationOp::AlterColumn {
                table: QualifiedName::new("public", "users"),
                column: "id".to_string(),
                changes: ColumnChanges {
                    data_type: Some(PgType::Text),
                    nullable: None,
                    default: None,
                },
            },
            MigrationOp::DropForeignKey {
                table: QualifiedName::new("public", "users"),
                foreign_key_name: "fk_id".to_string(),
            },
        ];

        let v2_result = plan_migration_checked(ops).unwrap();

        let drop_fk_pos = v2_result
            .iter()
            .position(|op| matches!(op, MigrationOp::DropForeignKey { .. }))
            .unwrap();
        let alter_pos = v2_result
            .iter()
            .position(|op| matches!(op, MigrationOp::AlterColumn { .. }))
            .unwrap();

        assert!(
            drop_fk_pos < alter_pos,
            "DropForeignKey should come before AlterColumn"
        );
    }

    #[test]
    fn v2_no_cycle_for_simple_ops() {
        let users = simple_table_with_fks("users", vec![]);
        let posts = simple_table_with_fks("posts", vec![make_fk("users")]);

        let ops = vec![
            MigrationOp::CreateTable(users),
            MigrationOp::CreateTable(posts),
        ];

        // Should not return an error
        let result = plan_migration_checked(ops);
        assert!(result.is_ok(), "Simple ops should not have cycles");
    }

    #[test]
    fn v2_equivalence_complex_schema() {
        // Build a complex set of operations
        let my_enum = EnumType {
            name: "status".to_string(),
            schema: "public".to_string(),
            values: vec!["active".to_string()],
            owner: None,
            grants: vec![],
        };

        let users = simple_table_with_fks("users", vec![]);
        let posts = simple_table_with_fks("posts", vec![make_fk("users")]);
        let comments = simple_table_with_fks("comments", vec![make_fk("posts"), make_fk("users")]);

        let ops = vec![
            MigrationOp::CreateEnum(my_enum),
            MigrationOp::CreateTable(comments.clone()),
            MigrationOp::CreateTable(posts.clone()),
            MigrationOp::CreateTable(users.clone()),
            MigrationOp::DropTable("public.old_table".to_string()),
        ];

        let bucket_result = plan_migration(ops.clone());
        let v2_result = plan_migration_checked(ops).unwrap();

        // Both should have same length
        assert_eq!(
            bucket_result.len(),
            v2_result.len(),
            "Both implementations should return same number of ops"
        );

        // Key ordering constraints should be preserved in both:
        // 1. Enum before tables
        let bucket_enum_pos = bucket_result
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateEnum(_)));
        let bucket_first_table_pos = bucket_result
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTable(_)));

        let v2_enum_pos = v2_result
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateEnum(_)));
        let v2_first_table_pos = v2_result
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTable(_)));

        if let (Some(e), Some(t)) = (bucket_enum_pos, bucket_first_table_pos) {
            assert!(e < t, "bucket: enum should be before first table");
        }
        if let (Some(e), Some(t)) = (v2_enum_pos, v2_first_table_pos) {
            assert!(e < t, "v2: enum should be before first table");
        }

        // 2. Users before posts (FK dependency)
        let bucket_users_pos = bucket_result
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTable(t) if t.name == "users"));
        let bucket_posts_pos = bucket_result
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTable(t) if t.name == "posts"));

        let v2_users_pos = v2_result
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTable(t) if t.name == "users"));
        let v2_posts_pos = v2_result
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTable(t) if t.name == "posts"));

        if let (Some(u), Some(p)) = (bucket_users_pos, bucket_posts_pos) {
            assert!(u < p, "bucket: users should be before posts");
        }
        if let (Some(u), Some(p)) = (v2_users_pos, v2_posts_pos) {
            assert!(u < p, "v2: users should be before posts");
        }

        // 3. Creates before drops
        let bucket_last_create = bucket_result
            .iter()
            .rposition(|op| matches!(op, MigrationOp::CreateTable(_) | MigrationOp::CreateEnum(_)));
        let bucket_first_drop = bucket_result
            .iter()
            .position(|op| matches!(op, MigrationOp::DropTable(_)));

        let v2_last_create = v2_result
            .iter()
            .rposition(|op| matches!(op, MigrationOp::CreateTable(_) | MigrationOp::CreateEnum(_)));
        let v2_first_drop = v2_result
            .iter()
            .position(|op| matches!(op, MigrationOp::DropTable(_)));

        if let (Some(c), Some(d)) = (bucket_last_create, bucket_first_drop) {
            assert!(c < d, "bucket: creates should be before drops");
        }
        if let (Some(c), Some(d)) = (v2_last_create, v2_first_drop) {
            assert!(c < d, "v2: creates should be before drops");
        }
    }

    #[test]
    fn planner_orders_default_privileges_at_end() {
        use crate::model::{DefaultPrivilegeObjectType, Privilege};

        let table = simple_table_with_fks("users", vec![]);

        let ops = vec![
            MigrationOp::AlterDefaultPrivileges {
                target_role: "admin".to_string(),
                schema: Some("public".to_string()),
                object_type: DefaultPrivilegeObjectType::Tables,
                grantee: "app_user".to_string(),
                privileges: vec![Privilege::Select],
                with_grant_option: false,
                revoke: false,
            },
            MigrationOp::CreateTable(table),
        ];

        let ordered = plan_migration(ops);

        let create_idx = ordered
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTable(_)));
        let adp_idx = ordered
            .iter()
            .position(|op| matches!(op, MigrationOp::AlterDefaultPrivileges { .. }));

        assert!(
            create_idx.unwrap() < adp_idx.unwrap(),
            "CreateTable should come before AlterDefaultPrivileges"
        );
    }

    #[test]
    fn grant_privileges_on_sequence_after_create_sequence() {
        // Grant on a sequence must come after the sequence is created
        // Bug report: pgmold executes GRANT before CREATE SEQUENCE
        use crate::diff::GrantObjectKind;
        use crate::model::Privilege;

        let seq = Sequence {
            name: "refresh_tokens_id_seq".to_string(),
            schema: "auth".to_string(),
            data_type: SequenceDataType::BigInt,
            start: Some(1),
            increment: Some(1),
            min_value: Some(1),
            max_value: Some(9223372036854775807),
            cycle: false,
            owner: None,
            grants: Vec::new(),
            cache: Some(1),
            owned_by: None,
        };

        // Create table that uses the sequence (like in the bug report)
        let mut columns = BTreeMap::new();
        columns.insert(
            "id".to_string(),
            Column {
                name: "id".to_string(),
                data_type: PgType::BigInt,
                nullable: false,
                default: Some("nextval('auth.refresh_tokens_id_seq'::regclass)".to_string()),
                comment: None,
            },
        );
        columns.insert(
            "token".to_string(),
            Column {
                name: "token".to_string(),
                data_type: PgType::Text,
                nullable: true,
                default: None,
                comment: None,
            },
        );

        let table = Table {
            name: "refresh_tokens".to_string(),
            schema: "auth".to_string(),
            columns,
            indexes: Vec::new(),
            primary_key: Some(PrimaryKey {
                columns: vec!["id".to_string()],
            }),
            foreign_keys: Vec::new(),
            check_constraints: Vec::new(),
            comment: None,
            row_level_security: false,
            policies: Vec::new(),
            partition_by: None,
            owner: None,
            grants: Vec::new(),
        };

        // Input ops in wrong order (grant first, then sequence and table)
        let ops = vec![
            MigrationOp::GrantPrivileges {
                object_kind: GrantObjectKind::Sequence,
                schema: "auth".to_string(),
                name: "refresh_tokens_id_seq".to_string(),
                args: None,
                grantee: "supabase_auth_admin".to_string(),
                privileges: vec![Privilege::Select, Privilege::Update, Privilege::Usage],
                with_grant_option: false,
            },
            MigrationOp::CreateTable(table),
            MigrationOp::CreateSequence(seq),
        ];

        let planned = plan_migration(ops);

        let create_seq_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateSequence(_)))
            .expect("CreateSequence should exist");
        let grant_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::GrantPrivileges { .. }))
            .expect("GrantPrivileges should exist");

        assert!(
            create_seq_pos < grant_pos,
            "CreateSequence must come before GrantPrivileges. CREATE at {create_seq_pos}, GRANT at {grant_pos}"
        );
    }

    #[test]
    fn grant_privileges_on_table_after_create_table() {
        use crate::diff::GrantObjectKind;
        use crate::model::Privilege;

        let table = simple_table_with_fks("users", vec![]);

        let ops = vec![
            MigrationOp::GrantPrivileges {
                object_kind: GrantObjectKind::Table,
                schema: "public".to_string(),
                name: "users".to_string(),
                args: None,
                grantee: "reader".to_string(),
                privileges: vec![Privilege::Select],
                with_grant_option: false,
            },
            MigrationOp::CreateTable(table),
        ];

        let planned = plan_migration(ops);

        let create_table_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTable(_)))
            .expect("CreateTable should exist");
        let grant_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::GrantPrivileges { .. }))
            .expect("GrantPrivileges should exist");

        assert!(
            create_table_pos < grant_pos,
            "CreateTable must come before GrantPrivileges. CREATE at {create_table_pos}, GRANT at {grant_pos}"
        );
    }

    #[test]
    fn grant_privileges_on_view_after_create_view() {
        use crate::diff::GrantObjectKind;
        use crate::model::Privilege;

        let view = View {
            name: "active_users".to_string(),
            schema: "public".to_string(),
            query: "SELECT * FROM users WHERE active = true".to_string(),
            materialized: false,
            owner: None,
            grants: vec![],
        };

        let ops = vec![
            MigrationOp::GrantPrivileges {
                object_kind: GrantObjectKind::View,
                schema: "public".to_string(),
                name: "active_users".to_string(),
                args: None,
                grantee: "reader".to_string(),
                privileges: vec![Privilege::Select],
                with_grant_option: false,
            },
            MigrationOp::CreateView(view),
        ];

        let planned = plan_migration(ops);

        let create_view_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateView(_)))
            .expect("CreateView should exist");
        let grant_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::GrantPrivileges { .. }))
            .expect("GrantPrivileges should exist");

        assert!(
            create_view_pos < grant_pos,
            "CreateView must come before GrantPrivileges. CREATE at {create_view_pos}, GRANT at {grant_pos}"
        );
    }

    #[test]
    fn grant_privileges_on_function_after_create_function() {
        use crate::diff::GrantObjectKind;
        use crate::model::Privilege;

        let func = Function {
            name: "add_numbers".to_string(),
            schema: "public".to_string(),
            arguments: vec![
                FunctionArg {
                    name: Some("a".to_string()),
                    data_type: "integer".to_string(),
                    default: None,
                    mode: ArgMode::In,
                },
                FunctionArg {
                    name: Some("b".to_string()),
                    data_type: "integer".to_string(),
                    default: None,
                    mode: ArgMode::In,
                },
            ],
            return_type: "integer".to_string(),
            language: "sql".to_string(),
            body: "SELECT a + b".to_string(),
            volatility: Volatility::Immutable,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: None,
            grants: vec![],
        };

        let ops = vec![
            MigrationOp::GrantPrivileges {
                object_kind: GrantObjectKind::Function,
                schema: "public".to_string(),
                name: "add_numbers".to_string(),
                args: Some("integer, integer".to_string()),
                grantee: "app_user".to_string(),
                privileges: vec![Privilege::Execute],
                with_grant_option: false,
            },
            MigrationOp::CreateFunction(func),
        ];

        let planned = plan_migration(ops);

        let create_func_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateFunction(_)))
            .expect("CreateFunction should exist");
        let grant_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::GrantPrivileges { .. }))
            .expect("GrantPrivileges should exist");

        assert!(
            create_func_pos < grant_pos,
            "CreateFunction must come before GrantPrivileges. CREATE at {create_func_pos}, GRANT at {grant_pos}"
        );
    }

    #[test]
    fn revoke_privileges_on_sequence_after_create_sequence() {
        use crate::diff::GrantObjectKind;
        use crate::model::Privilege;

        let seq = Sequence {
            name: "counter_seq".to_string(),
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
            owned_by: None,
        };

        let ops = vec![
            MigrationOp::RevokePrivileges {
                object_kind: GrantObjectKind::Sequence,
                schema: "public".to_string(),
                name: "counter_seq".to_string(),
                args: None,
                grantee: "public".to_string(),
                privileges: vec![Privilege::Usage],
                revoke_grant_option: false,
            },
            MigrationOp::CreateSequence(seq),
        ];

        let planned = plan_migration(ops);

        let create_seq_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateSequence(_)))
            .expect("CreateSequence should exist");
        let revoke_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::RevokePrivileges { .. }))
            .expect("RevokePrivileges should exist");

        assert!(
            create_seq_pos < revoke_pos,
            "CreateSequence must come before RevokePrivileges. CREATE at {create_seq_pos}, REVOKE at {revoke_pos}"
        );
    }

    #[test]
    fn grant_privileges_on_enum_after_create_enum() {
        use crate::diff::GrantObjectKind;
        use crate::model::Privilege;

        let my_enum = EnumType {
            name: "status".to_string(),
            schema: "public".to_string(),
            values: vec!["active".to_string(), "inactive".to_string()],
            owner: None,
            grants: vec![],
        };

        let ops = vec![
            MigrationOp::GrantPrivileges {
                object_kind: GrantObjectKind::Type,
                schema: "public".to_string(),
                name: "status".to_string(),
                args: None,
                grantee: "app_user".to_string(),
                privileges: vec![Privilege::Usage],
                with_grant_option: false,
            },
            MigrationOp::CreateEnum(my_enum),
        ];

        let planned = plan_migration(ops);

        let create_enum_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateEnum(_)))
            .expect("CreateEnum should exist");
        let grant_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::GrantPrivileges { .. }))
            .expect("GrantPrivileges should exist");

        assert!(
            create_enum_pos < grant_pos,
            "CreateEnum must come before GrantPrivileges. CREATE at {create_enum_pos}, GRANT at {grant_pos}"
        );
    }

    #[test]
    fn grant_privileges_on_domain_after_create_domain() {
        use crate::diff::GrantObjectKind;
        use crate::model::Privilege;

        let domain = Domain {
            name: "email".to_string(),
            schema: "public".to_string(),
            data_type: PgType::Text,
            default: None,
            not_null: false,
            collation: None,
            check_constraints: vec![],
            owner: None,
            grants: vec![],
        };

        let ops = vec![
            MigrationOp::GrantPrivileges {
                object_kind: GrantObjectKind::Domain,
                schema: "public".to_string(),
                name: "email".to_string(),
                args: None,
                grantee: "app_user".to_string(),
                privileges: vec![Privilege::Usage],
                with_grant_option: false,
            },
            MigrationOp::CreateDomain(domain),
        ];

        let planned = plan_migration(ops);

        let create_domain_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateDomain(_)))
            .expect("CreateDomain should exist");
        let grant_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::GrantPrivileges { .. }))
            .expect("GrantPrivileges should exist");

        assert!(
            create_domain_pos < grant_pos,
            "CreateDomain must come before GrantPrivileges. CREATE at {create_domain_pos}, GRANT at {grant_pos}"
        );
    }

    #[test]
    fn grant_privileges_on_schema_after_create_schema() {
        use crate::diff::GrantObjectKind;
        use crate::model::Privilege;

        let schema = PgSchema {
            name: "api".to_string(),
            grants: vec![],
        };

        let ops = vec![
            MigrationOp::GrantPrivileges {
                object_kind: GrantObjectKind::Schema,
                schema: "api".to_string(),
                name: "api".to_string(),
                args: None,
                grantee: "app_user".to_string(),
                privileges: vec![Privilege::Usage],
                with_grant_option: false,
            },
            MigrationOp::CreateSchema(schema),
        ];

        let planned = plan_migration(ops);

        let create_schema_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateSchema(_)))
            .expect("CreateSchema should exist");
        let grant_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::GrantPrivileges { .. }))
            .expect("GrantPrivileges should exist");

        assert!(
            create_schema_pos < grant_pos,
            "CreateSchema must come before GrantPrivileges. CREATE at {create_schema_pos}, GRANT at {grant_pos}"
        );
    }

    #[test]
    fn self_referencing_fk_does_not_create_cycle() {
        // A table with a self-referencing FK (e.g., employees.manager_id -> employees.id)
        // should not create a cycle in the dependency graph. PostgreSQL handles these
        // correctly when the FK is defined inline with CREATE TABLE.
        use crate::model::{Column, ForeignKey, PgType, PrimaryKey, ReferentialAction, Table};
        use std::collections::BTreeMap;

        let mut columns = BTreeMap::new();
        columns.insert(
            "id".to_string(),
            Column {
                name: "id".to_string(),
                data_type: PgType::Integer,
                nullable: false,
                default: None,
                comment: None,
            },
        );
        columns.insert(
            "manager_id".to_string(),
            Column {
                name: "manager_id".to_string(),
                data_type: PgType::Integer,
                nullable: true,
                default: None,
                comment: None,
            },
        );

        let table = Table {
            schema: "public".to_string(),
            name: "employees".to_string(),
            columns,
            indexes: vec![],
            primary_key: Some(PrimaryKey {
                columns: vec!["id".to_string()],
            }),
            foreign_keys: vec![ForeignKey {
                name: "employees_manager_fkey".to_string(),
                columns: vec!["manager_id".to_string()],
                referenced_schema: "public".to_string(),
                referenced_table: "employees".to_string(), // Self-reference
                referenced_columns: vec!["id".to_string()],
                on_delete: ReferentialAction::NoAction,
                on_update: ReferentialAction::NoAction,
            }],
            check_constraints: vec![],
            comment: None,
            row_level_security: false,
            policies: vec![],
            partition_by: None,
            owner: None,
            grants: vec![],
        };

        let ops = vec![MigrationOp::CreateTable(table)];

        // This should not panic with a cycle error
        let result = plan_migration_checked(ops);
        assert!(
            result.is_ok(),
            "Self-referencing FK should not cause a cycle"
        );

        let planned = result.unwrap();
        assert_eq!(planned.len(), 1);
        assert!(matches!(planned[0], MigrationOp::CreateTable(_)));
    }

    #[test]
    fn create_functions_ordered_by_function_dependencies() {
        // Chain: func_c calls func_b, func_b calls func_a
        // Expected order: func_a -> func_b -> func_c
        // Input order: [func_c, func_a, func_b] - completely scrambled

        let func_a = Function {
            name: "base_helper".to_string(),
            schema: "public".to_string(),
            arguments: vec![FunctionArg {
                name: Some("x".to_string()),
                data_type: "integer".to_string(),
                default: None,
                mode: ArgMode::In,
            }],
            return_type: "integer".to_string(),
            language: "sql".to_string(),
            body: "SELECT x * 2".to_string(),
            volatility: Volatility::Immutable,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: None,
            grants: vec![],
        };

        let func_b = Function {
            name: "middle_func".to_string(),
            schema: "public".to_string(),
            arguments: vec![FunctionArg {
                name: Some("n".to_string()),
                data_type: "integer".to_string(),
                default: None,
                mode: ArgMode::In,
            }],
            return_type: "integer".to_string(),
            language: "sql".to_string(),
            body: "SELECT public.base_helper(n) + 1".to_string(),
            volatility: Volatility::Immutable,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: None,
            grants: vec![],
        };

        let func_c = Function {
            name: "top_func".to_string(),
            schema: "public".to_string(),
            arguments: vec![FunctionArg {
                name: Some("m".to_string()),
                data_type: "integer".to_string(),
                default: None,
                mode: ArgMode::In,
            }],
            return_type: "integer".to_string(),
            language: "sql".to_string(),
            body: "SELECT public.middle_func(m) + 10".to_string(),
            volatility: Volatility::Immutable,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: None,
            grants: vec![],
        };

        // Input ops in scrambled order: [top, base, middle]
        let ops = vec![
            MigrationOp::CreateFunction(func_c.clone()),
            MigrationOp::CreateFunction(func_a.clone()),
            MigrationOp::CreateFunction(func_b.clone()),
        ];

        let planned = plan_migration(ops);

        let func_order: Vec<String> = planned
            .iter()
            .filter_map(|op| {
                if let MigrationOp::CreateFunction(f) = op {
                    Some(f.name.clone())
                } else {
                    None
                }
            })
            .collect();

        let base_pos = func_order
            .iter()
            .position(|n| n == "base_helper")
            .expect("base_helper should exist");
        let middle_pos = func_order
            .iter()
            .position(|n| n == "middle_func")
            .expect("middle_func should exist");
        let top_pos = func_order
            .iter()
            .position(|n| n == "top_func")
            .expect("top_func should exist");

        assert!(
            base_pos < middle_pos,
            "base_helper must be created before middle_func. base at {base_pos}, middle at {middle_pos}. Order: {func_order:?}"
        );
        assert!(
            middle_pos < top_pos,
            "middle_func must be created before top_func. middle at {middle_pos}, top at {top_pos}. Order: {func_order:?}"
        );
    }

    #[test]
    fn enums_created_before_functions() {
        let func = Function {
            name: "get_entities".to_string(),
            schema: "mrv".to_string(),
            arguments: vec![],
            return_type: "TABLE(\"entityType\" mrv.\"EntityType\")".to_string(),
            language: "plpgsql".to_string(),
            body: "BEGIN END;".to_string(),
            volatility: Volatility::Stable,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
        };

        let ops = vec![
            MigrationOp::CreateFunction(func),
            MigrationOp::CreateEnum(EnumType {
                name: "EntityType".to_string(),
                schema: "mrv".to_string(),
                values: vec!["project".to_string(), "field".to_string()],
                owner: None,
                grants: Vec::new(),
            }),
        ];

        let planned = plan_migration(ops);

        let create_enum_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateEnum(_)))
            .unwrap();
        let create_func_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateFunction(_)))
            .unwrap();

        assert!(
            create_enum_pos < create_func_pos,
            "CreateEnum must come before CreateFunction. ENUM at {create_enum_pos}, FUNC at {create_func_pos}"
        );
    }

    #[test]
    fn domains_created_before_functions() {
        let func = Function {
            name: "validate_email".to_string(),
            schema: "public".to_string(),
            arguments: vec![FunctionArg {
                name: Some("input".to_string()),
                data_type: "email_address".to_string(),
                mode: ArgMode::In,
                default: None,
            }],
            return_type: "boolean".to_string(),
            language: "plpgsql".to_string(),
            body: "BEGIN RETURN true; END;".to_string(),
            volatility: Volatility::Immutable,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
        };

        let domain = Domain {
            name: "email_address".to_string(),
            schema: "public".to_string(),
            data_type: PgType::Text,
            default: None,
            not_null: false,
            collation: None,
            check_constraints: vec![],
            owner: None,
            grants: vec![],
        };

        let ops = vec![
            MigrationOp::CreateFunction(func),
            MigrationOp::CreateDomain(domain),
        ];

        let planned = plan_migration(ops);

        let create_domain_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateDomain(_)))
            .unwrap();
        let create_func_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateFunction(_)))
            .unwrap();

        assert!(
            create_domain_pos < create_func_pos,
            "CreateDomain must come before CreateFunction. DOMAIN at {create_domain_pos}, FUNC at {create_func_pos}"
        );
    }

    #[test]
    fn add_enum_value_before_functions() {
        let func = Function {
            name: "get_entities".to_string(),
            schema: "mrv".to_string(),
            arguments: vec![],
            return_type: "TABLE(\"entityType\" mrv.\"EntityType\")".to_string(),
            language: "plpgsql".to_string(),
            body: "BEGIN END;".to_string(),
            volatility: Volatility::Stable,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
        };

        let ops = vec![
            MigrationOp::CreateFunction(func),
            MigrationOp::AddEnumValue {
                enum_name: "mrv.EntityType".to_string(),
                value: "monitoring_plot".to_string(),
                position: None,
            },
        ];

        let planned = plan_migration(ops);

        let add_enum_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::AddEnumValue { .. }))
            .unwrap();
        let create_func_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateFunction(_)))
            .unwrap();

        assert!(
            add_enum_pos < create_func_pos,
            "AddEnumValue must come before CreateFunction. ENUM at {add_enum_pos}, FUNC at {create_func_pos}"
        );
    }

    #[test]
    fn returns_setof_table_ordered_after_table() {
        let func = Function {
            name: "get_facilities".to_string(),
            schema: "mrv".to_string(),
            arguments: vec![],
            return_type: "SETOF mrv.\"ProcurementFacility\"".to_string(),
            language: "plpgsql".to_string(),
            body: "BEGIN RETURN QUERY SELECT * FROM mrv.\"ProcurementFacility\"; END;".to_string(),
            volatility: Volatility::Stable,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
        };

        let mut table = simple_table_with_fks("ProcurementFacility", vec![]);
        table.schema = "mrv".to_string();

        let ops = vec![
            MigrationOp::CreateFunction(func),
            MigrationOp::CreateTable(table),
        ];

        let planned = plan_migration(ops);

        let create_table_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTable(_)))
            .unwrap();
        let create_func_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateFunction(_)))
            .unwrap();

        assert!(
            create_table_pos < create_func_pos,
            "CreateTable must come before CreateFunction with RETURNS SETOF. TABLE at {create_table_pos}, FUNC at {create_func_pos}"
        );
    }

    #[test]
    fn returns_setof_unqualified_table_ordered_after_table() {
        let func = Function {
            name: "get_users".to_string(),
            schema: "public".to_string(),
            arguments: vec![],
            return_type: "SETOF \"Users\"".to_string(),
            language: "plpgsql".to_string(),
            body: "BEGIN RETURN QUERY SELECT * FROM \"Users\"; END;".to_string(),
            volatility: Volatility::Stable,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
        };

        let table = simple_table_with_fks("Users", vec![]);

        let ops = vec![
            MigrationOp::CreateFunction(func),
            MigrationOp::CreateTable(table),
        ];

        let planned = plan_migration(ops);

        let create_table_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTable(_)))
            .unwrap();
        let create_func_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateFunction(_)))
            .unwrap();

        assert!(
            create_table_pos < create_func_pos,
            "CreateTable must come before CreateFunction with RETURNS SETOF (unqualified). TABLE at {create_table_pos}, FUNC at {create_func_pos}"
        );
    }

    #[test]
    fn returns_setof_enum_still_before_tables() {
        // A function returning SETOF <enum> should NOT lose its blanket "function before table"
        // edge, because the SETOF target is an enum, not a table in this migration.
        let func = Function {
            name: "get_entity_types".to_string(),
            schema: "mrv".to_string(),
            arguments: vec![],
            return_type: "SETOF mrv.\"EntityType\"".to_string(),
            language: "plpgsql".to_string(),
            body: "BEGIN RETURN QUERY SELECT unnest(ARRAY['project','field']::mrv.\"EntityType\"[]); END;".to_string(),
            volatility: Volatility::Stable,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
        };

        let mut unrelated_table = simple_table_with_fks("Parcel", vec![]);
        unrelated_table.schema = "mrv".to_string();

        let ops = vec![
            MigrationOp::CreateEnum(EnumType {
                name: "EntityType".to_string(),
                schema: "mrv".to_string(),
                values: vec!["project".to_string(), "field".to_string()],
                owner: None,
                grants: Vec::new(),
            }),
            MigrationOp::CreateTable(unrelated_table),
            MigrationOp::CreateFunction(func),
        ];

        let planned = plan_migration(ops);

        let create_func_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateFunction(_)))
            .unwrap();
        let create_table_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTable(_)))
            .unwrap();

        assert!(
            create_func_pos < create_table_pos,
            "CreateFunction returning SETOF enum must still come before unrelated CreateTable. FUNC at {create_func_pos}, TABLE at {create_table_pos}"
        );
    }

    #[test]
    fn regular_function_still_before_tables() {
        let func = Function {
            name: "compute_total".to_string(),
            schema: "public".to_string(),
            arguments: vec![],
            return_type: "integer".to_string(),
            language: "plpgsql".to_string(),
            body: "BEGIN RETURN 42; END;".to_string(),
            volatility: Volatility::Stable,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
        };

        let table = simple_table_with_fks("orders", vec![]);

        let ops = vec![
            MigrationOp::CreateTable(table),
            MigrationOp::CreateFunction(func),
        ];

        let planned = plan_migration(ops);

        let create_func_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateFunction(_)))
            .unwrap();
        let create_table_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTable(_)))
            .unwrap();

        assert!(
            create_func_pos < create_table_pos,
            "Regular CreateFunction must come before CreateTable. FUNC at {create_func_pos}, TABLE at {create_table_pos}"
        );
    }

    // =========================================================================
    // Dependency ordering harness (#84)
    //
    // Systematic coverage of all ordering invariants the planner must enforce.
    // Each test feeds ops in REVERSE of expected order — if the planner doesn't
    // reorder them, the test fails.
    // =========================================================================

    fn make_function_with_body(
        name: &str,
        schema: &str,
        body: &str,
        return_type: &str,
    ) -> Function {
        Function {
            name: name.to_string(),
            schema: schema.to_string(),
            arguments: vec![],
            return_type: return_type.to_string(),
            language: "plpgsql".to_string(),
            body: body.to_string(),
            volatility: Volatility::Stable,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
        }
    }

    fn make_simple_function(name: &str, schema: &str) -> Function {
        make_function_with_body(name, schema, "BEGIN RETURN 1; END;", "integer")
    }

    fn make_trigger(
        name: &str,
        target_schema: &str,
        target_name: &str,
        function_name: &str,
    ) -> Trigger {
        Trigger {
            name: name.to_string(),
            target_schema: target_schema.to_string(),
            target_name: target_name.to_string(),
            timing: TriggerTiming::Before,
            events: vec![TriggerEvent::Insert],
            update_columns: vec![],
            for_each_row: true,
            when_clause: None,
            function_schema: target_schema.to_string(),
            function_name: function_name.to_string(),
            function_args: vec![],
            enabled: TriggerEnabled::Origin,
            old_table_name: None,
            new_table_name: None,
        }
    }

    fn make_view(name: &str, schema: &str, query: &str) -> View {
        View {
            name: name.to_string(),
            schema: schema.to_string(),
            query: query.to_string(),
            materialized: false,
            owner: None,
            grants: Vec::new(),
        }
    }

    fn make_policy(name: &str, table_schema: &str, table: &str) -> Policy {
        Policy {
            name: name.to_string(),
            table_schema: table_schema.to_string(),
            table: table.to_string(),
            command: PolicyCommand::Select,
            roles: vec!["authenticated".to_string()],
            using_expr: Some("true".to_string()),
            check_expr: None,
        }
    }

    fn make_schema(name: &str) -> PgSchema {
        PgSchema {
            name: name.to_string(),
            grants: vec![],
        }
    }

    fn make_extension(name: &str) -> Extension {
        Extension {
            name: name.to_string(),
            version: None,
            schema: None,
        }
    }

    fn make_enum(name: &str, schema: &str) -> EnumType {
        EnumType {
            name: name.to_string(),
            schema: schema.to_string(),
            values: vec!["active".to_string()],
            owner: None,
            grants: Vec::new(),
        }
    }

    fn make_domain(name: &str, schema: &str) -> Domain {
        Domain {
            name: name.to_string(),
            schema: schema.to_string(),
            data_type: PgType::Text,
            default: None,
            not_null: false,
            collation: None,
            check_constraints: vec![],
            owner: None,
            grants: vec![],
        }
    }

    fn make_sequence(name: &str, schema: &str) -> Sequence {
        Sequence {
            name: name.to_string(),
            schema: schema.to_string(),
            data_type: SequenceDataType::BigInt,
            start: Some(1),
            increment: Some(1),
            min_value: Some(1),
            max_value: Some(9223372036854775807),
            cycle: false,
            owner: None,
            grants: Vec::new(),
            cache: Some(1),
            owned_by: None,
        }
    }

    fn make_column(name: &str) -> Column {
        Column {
            name: name.to_string(),
            data_type: PgType::Text,
            nullable: true,
            default: None,
            comment: None,
        }
    }

    /// Asserts that the single op matching `before_finder` appears before
    /// the single op matching `after_finder` in the planned output.
    /// Panics if either predicate matches zero or more than one op.
    fn assert_op_position(
        planned: &[MigrationOp],
        before_name: &str,
        after_name: &str,
        before_finder: impl Fn(&MigrationOp) -> bool,
        after_finder: impl Fn(&MigrationOp) -> bool,
    ) {
        let before_matches: Vec<usize> = planned
            .iter()
            .enumerate()
            .filter(|(_, op)| before_finder(op))
            .map(|(i, _)| i)
            .collect();
        let after_matches: Vec<usize> = planned
            .iter()
            .enumerate()
            .filter(|(_, op)| after_finder(op))
            .map(|(i, _)| i)
            .collect();

        assert_eq!(
            before_matches.len(),
            1,
            "{before_name}: expected exactly 1 match, found {} at positions {before_matches:?}\nPlan: {planned:#?}",
            before_matches.len()
        );
        assert_eq!(
            after_matches.len(),
            1,
            "{after_name}: expected exactly 1 match, found {} at positions {after_matches:?}\nPlan: {planned:#?}",
            after_matches.len()
        );

        let before_pos = before_matches[0];
        let after_pos = after_matches[0];
        assert!(
            before_pos < after_pos,
            "{before_name} (at {before_pos}) must come before {after_name} (at {after_pos})"
        );
    }

    // --- Schema infrastructure ordering ---

    #[test]
    fn schema_before_enum() {
        let ops = vec![
            MigrationOp::CreateEnum(make_enum("status", "api")),
            MigrationOp::CreateSchema(make_schema("api")),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateSchema",
            "CreateEnum",
            |op| matches!(op, MigrationOp::CreateSchema(_)),
            |op| matches!(op, MigrationOp::CreateEnum(_)),
        );
    }

    #[test]
    fn schema_before_domain() {
        let ops = vec![
            MigrationOp::CreateDomain(make_domain("email", "api")),
            MigrationOp::CreateSchema(make_schema("api")),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateSchema",
            "CreateDomain",
            |op| matches!(op, MigrationOp::CreateSchema(_)),
            |op| matches!(op, MigrationOp::CreateDomain(_)),
        );
    }

    #[test]
    fn schema_before_sequence() {
        let ops = vec![
            MigrationOp::CreateSequence(make_sequence("counter", "api")),
            MigrationOp::CreateSchema(make_schema("api")),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateSchema",
            "CreateSequence",
            |op| matches!(op, MigrationOp::CreateSchema(_)),
            |op| matches!(op, MigrationOp::CreateSequence(_)),
        );
    }

    #[test]
    fn schema_before_function() {
        let ops = vec![
            MigrationOp::CreateFunction(make_simple_function("helper", "api")),
            MigrationOp::CreateSchema(make_schema("api")),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateSchema",
            "CreateFunction",
            |op| matches!(op, MigrationOp::CreateSchema(_)),
            |op| matches!(op, MigrationOp::CreateFunction(_)),
        );
    }

    #[test]
    fn schema_before_table() {
        let mut table = simple_table_with_fks("users", vec![]);
        table.schema = "api".to_string();
        let ops = vec![
            MigrationOp::CreateTable(table),
            MigrationOp::CreateSchema(make_schema("api")),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateSchema",
            "CreateTable",
            |op| matches!(op, MigrationOp::CreateSchema(_)),
            |op| matches!(op, MigrationOp::CreateTable(_)),
        );
    }

    #[test]
    fn schema_before_view() {
        let ops = vec![
            MigrationOp::CreateView(make_view("dashboard", "api", "SELECT 1")),
            MigrationOp::CreateSchema(make_schema("api")),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateSchema",
            "CreateView",
            |op| matches!(op, MigrationOp::CreateSchema(_)),
            |op| matches!(op, MigrationOp::CreateView(_)),
        );
    }

    // --- Extension ordering ---

    #[test]
    fn extension_before_enum() {
        let ops = vec![
            MigrationOp::CreateEnum(make_enum("status", "public")),
            MigrationOp::CreateExtension(make_extension("uuid-ossp")),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateExtension",
            "CreateEnum",
            |op| matches!(op, MigrationOp::CreateExtension(_)),
            |op| matches!(op, MigrationOp::CreateEnum(_)),
        );
    }

    #[test]
    fn extension_before_table() {
        let ops = vec![
            MigrationOp::CreateTable(simple_table_with_fks("users", vec![])),
            MigrationOp::CreateExtension(make_extension("uuid-ossp")),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateExtension",
            "CreateTable",
            |op| matches!(op, MigrationOp::CreateExtension(_)),
            |op| matches!(op, MigrationOp::CreateTable(_)),
        );
    }

    // --- Type ordering (enums, domains) ---

    #[test]
    fn enum_before_table() {
        let ops = vec![
            MigrationOp::CreateTable(simple_table_with_fks("users", vec![])),
            MigrationOp::CreateEnum(make_enum("role", "public")),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateEnum",
            "CreateTable",
            |op| matches!(op, MigrationOp::CreateEnum(_)),
            |op| matches!(op, MigrationOp::CreateTable(_)),
        );
    }

    #[test]
    fn domain_before_table() {
        let ops = vec![
            MigrationOp::CreateTable(simple_table_with_fks("users", vec![])),
            MigrationOp::CreateDomain(make_domain("email", "public")),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateDomain",
            "CreateTable",
            |op| matches!(op, MigrationOp::CreateDomain(_)),
            |op| matches!(op, MigrationOp::CreateTable(_)),
        );
    }

    #[test]
    fn sequence_before_table() {
        let ops = vec![
            MigrationOp::CreateTable(simple_table_with_fks("users", vec![])),
            MigrationOp::CreateSequence(make_sequence("users_id_seq", "public")),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateSequence",
            "CreateTable",
            |op| matches!(op, MigrationOp::CreateSequence(_)),
            |op| matches!(op, MigrationOp::CreateTable(_)),
        );
    }

    // --- Function ↔ table ordering ---

    #[test]
    fn function_before_table_default() {
        let ops = vec![
            MigrationOp::CreateTable(simple_table_with_fks("orders", vec![])),
            MigrationOp::CreateFunction(make_simple_function("gen_id", "public")),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateFunction",
            "CreateTable",
            |op| matches!(op, MigrationOp::CreateFunction(_)),
            |op| matches!(op, MigrationOp::CreateTable(_)),
        );
    }

    #[test]
    fn function_with_returns_setof_after_table() {
        let func = make_function_with_body(
            "get_all",
            "public",
            "BEGIN RETURN QUERY SELECT * FROM items; END;",
            "SETOF public.\"items\"",
        );
        let ops = vec![
            MigrationOp::CreateFunction(func),
            MigrationOp::CreateTable(simple_table_with_fks("items", vec![])),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateTable",
            "CreateFunction",
            |op| matches!(op, MigrationOp::CreateTable(_)),
            |op| matches!(op, MigrationOp::CreateFunction(_)),
        );
    }

    // --- %ROWTYPE table dependencies (#112) ---

    #[test]
    fn function_with_rowtype_after_referenced_table() {
        let func = make_function_with_body(
            "process_user",
            "public",
            r#"
            DECLARE
                r public."users"%ROWTYPE;
            BEGIN
                SELECT * INTO r FROM public."users" LIMIT 1;
                RETURN r.id;
            END;
            "#,
            "integer",
        );
        let ops = vec![
            MigrationOp::CreateFunction(func),
            MigrationOp::CreateTable(simple_table_with_fks("users", vec![])),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateTable(users)",
            "CreateFunction(process_user)",
            |op| matches!(op, MigrationOp::CreateTable(t) if t.name == "users"),
            |op| matches!(op, MigrationOp::CreateFunction(f) if f.name == "process_user"),
        );
    }

    #[test]
    fn function_with_rowtype_quoted_table_name() {
        let func = make_function_with_body(
            "process_item",
            "myschema",
            r#"
            DECLARE
                r myschema."MyTable"%ROWTYPE;
            BEGIN
                RETURN r.id;
            END;
            "#,
            "integer",
        );
        let mut table = simple_table_with_fks("MyTable", vec![]);
        table.schema = "myschema".to_string();
        let ops = vec![
            MigrationOp::CreateFunction(func),
            MigrationOp::CreateTable(table),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateTable(MyTable)",
            "CreateFunction(process_item)",
            |op| matches!(op, MigrationOp::CreateTable(t) if t.name == "MyTable"),
            |op| matches!(op, MigrationOp::CreateFunction(f) if f.name == "process_item"),
        );
    }

    #[test]
    fn function_with_rowtype_unqualified_uses_function_schema() {
        let func = make_function_with_body(
            "process_order",
            "public",
            r#"
            DECLARE
                r orders%ROWTYPE;
            BEGIN
                RETURN r.id;
            END;
            "#,
            "integer",
        );
        let ops = vec![
            MigrationOp::CreateFunction(func),
            MigrationOp::CreateTable(simple_table_with_fks("orders", vec![])),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateTable(orders)",
            "CreateFunction(process_order)",
            |op| matches!(op, MigrationOp::CreateTable(t) if t.name == "orders"),
            |op| matches!(op, MigrationOp::CreateFunction(f) if f.name == "process_order"),
        );
    }

    #[test]
    fn function_with_multiple_rowtype_references() {
        let func = make_function_with_body(
            "process_both",
            "public",
            r#"
            DECLARE
                u public.users%ROWTYPE;
                p public.posts%ROWTYPE;
            BEGIN
                RETURN u.id;
            END;
            "#,
            "integer",
        );
        let ops = vec![
            MigrationOp::CreateFunction(func),
            MigrationOp::CreateTable(simple_table_with_fks("users", vec![])),
            MigrationOp::CreateTable(simple_table_with_fks("posts", vec![])),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateTable(users)",
            "CreateFunction(process_both)",
            |op| matches!(op, MigrationOp::CreateTable(t) if t.name == "users"),
            |op| matches!(op, MigrationOp::CreateFunction(f) if f.name == "process_both"),
        );
        assert_op_position(
            &planned,
            "CreateTable(posts)",
            "CreateFunction(process_both)",
            |op| matches!(op, MigrationOp::CreateTable(t) if t.name == "posts"),
            |op| matches!(op, MigrationOp::CreateFunction(f) if f.name == "process_both"),
        );
    }

    #[test]
    fn function_with_rowtype_case_insensitive() {
        let func = make_function_with_body(
            "process_item",
            "public",
            r#"
            DECLARE
                r public.items%rowtype;
            BEGIN
                RETURN r.id;
            END;
            "#,
            "integer",
        );
        let ops = vec![
            MigrationOp::CreateFunction(func),
            MigrationOp::CreateTable(simple_table_with_fks("items", vec![])),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateTable(items)",
            "CreateFunction(process_item)",
            |op| matches!(op, MigrationOp::CreateTable(t) if t.name == "items"),
            |op| matches!(op, MigrationOp::CreateFunction(f) if f.name == "process_item"),
        );
    }

    #[test]
    fn function_with_rowtype_ref_to_table_not_in_migration() {
        let func = make_function_with_body(
            "lookup",
            "public",
            r#"
            DECLARE
                r public.external_table%ROWTYPE;
            BEGIN
                RETURN 1;
            END;
            "#,
            "integer",
        );
        let table = simple_table_with_fks("unrelated", vec![]);
        let ops = vec![
            MigrationOp::CreateFunction(func),
            MigrationOp::CreateTable(table),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateFunction(lookup)",
            "CreateTable(unrelated)",
            |op| matches!(op, MigrationOp::CreateFunction(f) if f.name == "lookup"),
            |op| matches!(op, MigrationOp::CreateTable(t) if t.name == "unrelated"),
        );
    }

    // --- Table-level object ordering ---

    #[test]
    fn table_before_partition() {
        let parent = simple_table_with_fks("events", vec![]);
        let partition = crate::model::Partition {
            name: "events_2024".to_string(),
            schema: "public".to_string(),
            parent_name: "events".to_string(),
            parent_schema: "public".to_string(),
            bound: crate::model::PartitionBound::Range {
                from: vec!["'2024-01-01'".to_string()],
                to: vec!["'2025-01-01'".to_string()],
            },
            indexes: vec![],
            check_constraints: vec![],
            owner: None,
        };
        let ops = vec![
            MigrationOp::CreatePartition(partition),
            MigrationOp::CreateTable(parent),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateTable",
            "CreatePartition",
            |op| matches!(op, MigrationOp::CreateTable(_)),
            |op| matches!(op, MigrationOp::CreatePartition(_)),
        );
    }

    #[test]
    fn table_before_add_column() {
        let ops = vec![
            MigrationOp::AddColumn {
                table: QualifiedName::new("public", "users"),
                column: make_column("email"),
            },
            MigrationOp::CreateTable(simple_table_with_fks("users", vec![])),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateTable",
            "AddColumn",
            |op| matches!(op, MigrationOp::CreateTable(_)),
            |op| matches!(op, MigrationOp::AddColumn { .. }),
        );
    }

    #[test]
    fn table_before_add_index() {
        let ops = vec![
            MigrationOp::AddIndex {
                table: QualifiedName::new("public", "users"),
                index: Index {
                    name: "users_email_idx".to_string(),
                    columns: vec!["email".to_string()],
                    unique: false,
                    index_type: IndexType::BTree,
                    predicate: None,
                    is_constraint: false,
                },
            },
            MigrationOp::CreateTable(simple_table_with_fks("users", vec![])),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateTable",
            "AddIndex",
            |op| matches!(op, MigrationOp::CreateTable(_)),
            |op| matches!(op, MigrationOp::AddIndex { .. }),
        );
    }

    #[test]
    fn table_before_enable_rls() {
        let ops = vec![
            MigrationOp::EnableRls {
                table: QualifiedName::new("public", "users"),
            },
            MigrationOp::CreateTable(simple_table_with_fks("users", vec![])),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateTable",
            "EnableRls",
            |op| matches!(op, MigrationOp::CreateTable(_)),
            |op| matches!(op, MigrationOp::EnableRls { .. }),
        );
    }

    #[test]
    fn enable_rls_before_policy() {
        let ops = vec![
            MigrationOp::CreatePolicy(make_policy("read_all", "public", "users")),
            MigrationOp::EnableRls {
                table: QualifiedName::new("public", "users"),
            },
            MigrationOp::CreateTable(simple_table_with_fks("users", vec![])),
        ];
        let planned = plan_migration(ops);

        let table_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTable(_)))
            .unwrap();
        let rls_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::EnableRls { .. }))
            .unwrap();
        let policy_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreatePolicy(_)))
            .unwrap();

        assert!(table_pos < rls_pos, "CreateTable before EnableRls");
        assert!(rls_pos < policy_pos, "EnableRls before CreatePolicy");
    }

    #[test]
    fn table_before_trigger() {
        let ops = vec![
            MigrationOp::CreateTrigger(make_trigger("audit_insert", "public", "users", "audit_fn")),
            MigrationOp::CreateTable(simple_table_with_fks("users", vec![])),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateTable",
            "CreateTrigger",
            |op| matches!(op, MigrationOp::CreateTable(_)),
            |op| matches!(op, MigrationOp::CreateTrigger(_)),
        );
    }

    #[test]
    fn function_before_trigger() {
        let ops = vec![
            MigrationOp::CreateTrigger(make_trigger("audit_insert", "public", "users", "audit_fn")),
            MigrationOp::CreateFunction(make_simple_function("audit_fn", "public")),
            MigrationOp::CreateTable(simple_table_with_fks("users", vec![])),
        ];
        let planned = plan_migration(ops);

        let table_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTable(_)))
            .expect("CreateTable not found");
        let func_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateFunction(_)))
            .expect("CreateFunction not found");
        let trigger_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTrigger(_)))
            .expect("CreateTrigger not found");

        assert!(
            func_pos < trigger_pos,
            "CreateFunction ({func_pos}) before CreateTrigger ({trigger_pos})"
        );
        assert!(
            table_pos < trigger_pos,
            "CreateTable ({table_pos}) before CreateTrigger ({trigger_pos})"
        );
    }

    #[test]
    fn function_before_policy() {
        let ops = vec![
            MigrationOp::CreatePolicy(make_policy("read_own", "public", "users")),
            MigrationOp::CreateFunction(make_simple_function("auth_uid", "public")),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateFunction",
            "CreatePolicy",
            |op| matches!(op, MigrationOp::CreateFunction(_)),
            |op| matches!(op, MigrationOp::CreatePolicy(_)),
        );
    }

    #[test]
    fn policy_with_function_reference_in_using_expr() {
        let mut policy = make_policy("entity_owner", "public", "items");
        policy.using_expr = Some("auth.user_owns_entity(entity_id, 'items'::text)".to_string());
        let ops = vec![
            MigrationOp::CreatePolicy(policy),
            MigrationOp::CreateFunction(make_simple_function("user_owns_entity", "auth")),
            MigrationOp::CreateTable(simple_table_with_fks("items", vec![])),
        ];
        let planned = plan_migration(ops);

        let func_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateFunction(_)))
            .expect("CreateFunction not found");
        let policy_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreatePolicy(_)))
            .expect("CreatePolicy not found");

        assert!(
            func_pos < policy_pos,
            "CreateFunction ({func_pos}) before CreatePolicy ({policy_pos})"
        );
    }

    #[test]
    fn policy_with_function_reference_in_check_expr() {
        let mut policy = make_policy("entity_insert", "public", "items");
        policy.using_expr = None;
        policy.check_expr = Some("auth.can_insert('items'::text)".to_string());
        let ops = vec![
            MigrationOp::CreatePolicy(policy),
            MigrationOp::CreateFunction(make_simple_function("can_insert", "auth")),
            MigrationOp::CreateTable(simple_table_with_fks("items", vec![])),
        ];
        let planned = plan_migration(ops);

        let func_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateFunction(_)))
            .expect("CreateFunction not found");
        let policy_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreatePolicy(_)))
            .expect("CreatePolicy not found");

        assert!(
            func_pos < policy_pos,
            "CreateFunction ({func_pos}) before CreatePolicy ({policy_pos})"
        );
    }

    #[test]
    fn trigger_cross_schema_function_dependency() {
        let mut trigger = make_trigger("audit_insert", "public", "users", "log_changes");
        trigger.function_schema = "audit".to_string();
        let ops = vec![
            MigrationOp::CreateTrigger(trigger),
            MigrationOp::CreateFunction(make_simple_function("log_changes", "audit")),
            MigrationOp::CreateTable(simple_table_with_fks("users", vec![])),
        ];
        let planned = plan_migration(ops);

        let func_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateFunction(_)))
            .expect("CreateFunction not found");
        let trigger_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTrigger(_)))
            .expect("CreateTrigger not found");

        assert!(
            func_pos < trigger_pos,
            "CreateFunction ({func_pos}) before CreateTrigger ({trigger_pos})"
        );
    }

    #[test]
    fn view_with_function_reference() {
        let ops = vec![
            MigrationOp::CreateView(make_view(
                "active_users",
                "public",
                "SELECT auth.is_active(id) FROM public.users",
            )),
            MigrationOp::CreateFunction(make_simple_function("is_active", "auth")),
            MigrationOp::CreateTable(simple_table_with_fks("users", vec![])),
        ];
        let planned = plan_migration(ops);

        let func_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateFunction(_)))
            .expect("CreateFunction not found");
        let view_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateView(_)))
            .expect("CreateView not found");

        assert!(
            func_pos < view_pos,
            "CreateFunction ({func_pos}) before CreateView ({view_pos})"
        );
    }

    #[test]
    fn check_constraint_with_function_reference() {
        let ops = vec![
            MigrationOp::AddCheckConstraint {
                table: QualifiedName::new("public", "items"),
                check_constraint: CheckConstraint {
                    name: "items_valid".to_string(),
                    expression: "auth.validate_item(price, quantity)".to_string(),
                },
            },
            MigrationOp::CreateFunction(make_simple_function("validate_item", "auth")),
            MigrationOp::CreateTable(simple_table_with_fks("items", vec![])),
        ];
        let planned = plan_migration(ops);

        let func_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateFunction(_)))
            .expect("CreateFunction not found");
        let check_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::AddCheckConstraint { .. }))
            .expect("AddCheckConstraint not found");

        assert!(
            func_pos < check_pos,
            "CreateFunction ({func_pos}) before AddCheckConstraint ({check_pos})"
        );
    }

    #[test]
    fn index_with_function_expression() {
        let ops = vec![
            MigrationOp::AddIndex {
                table: QualifiedName::new("public", "items"),
                index: Index {
                    name: "items_normalized_idx".to_string(),
                    columns: vec!["auth.normalize_name(name)".to_string()],
                    unique: false,
                    index_type: IndexType::BTree,
                    predicate: None,
                    is_constraint: false,
                },
            },
            MigrationOp::CreateFunction(make_simple_function("normalize_name", "auth")),
            MigrationOp::CreateTable(simple_table_with_fks("items", vec![])),
        ];
        let planned = plan_migration(ops);

        let func_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateFunction(_)))
            .expect("CreateFunction not found");
        let index_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::AddIndex { .. }))
            .expect("AddIndex not found");

        assert!(
            func_pos < index_pos,
            "CreateFunction ({func_pos}) before AddIndex ({index_pos})"
        );
    }

    #[test]
    fn index_with_function_predicate() {
        let ops = vec![
            MigrationOp::AddIndex {
                table: QualifiedName::new("public", "items"),
                index: Index {
                    name: "items_active_idx".to_string(),
                    columns: vec!["id".to_string()],
                    unique: false,
                    index_type: IndexType::BTree,
                    predicate: Some("auth.is_active(status)".to_string()),
                    is_constraint: false,
                },
            },
            MigrationOp::CreateFunction(make_simple_function("is_active", "auth")),
            MigrationOp::CreateTable(simple_table_with_fks("items", vec![])),
        ];
        let planned = plan_migration(ops);

        let func_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateFunction(_)))
            .expect("CreateFunction not found");
        let index_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::AddIndex { .. }))
            .expect("AddIndex not found");

        assert!(
            func_pos < index_pos,
            "CreateFunction ({func_pos}) before AddIndex ({index_pos})"
        );
    }

    #[test]
    fn column_default_with_function_reference() {
        let ops = vec![
            MigrationOp::AddColumn {
                table: QualifiedName::new("public", "items"),
                column: Column {
                    name: "tracking_id".to_string(),
                    data_type: PgType::Text,
                    nullable: true,
                    default: Some("auth.generate_tracking_id()".to_string()),
                    comment: None,
                },
            },
            MigrationOp::CreateFunction(make_simple_function("generate_tracking_id", "auth")),
            MigrationOp::CreateTable(simple_table_with_fks("items", vec![])),
        ];
        let planned = plan_migration(ops);

        let func_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateFunction(_)))
            .expect("CreateFunction not found");
        let col_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::AddColumn { .. }))
            .expect("AddColumn not found");

        assert!(
            func_pos < col_pos,
            "CreateFunction ({func_pos}) before AddColumn ({col_pos})"
        );
    }

    #[test]
    fn alter_policy_with_function_reference() {
        let ops = vec![
            MigrationOp::AlterPolicy {
                table: QualifiedName::new("public", "items"),
                name: "entity_owner".to_string(),
                changes: PolicyChanges {
                    roles: None,
                    using_expr: Some(Some(
                        "auth.user_owns_entity(entity_id, 'items'::text)".to_string(),
                    )),
                    check_expr: None,
                },
            },
            MigrationOp::CreateFunction(make_simple_function("user_owns_entity", "auth")),
        ];
        let planned = plan_migration(ops);

        let func_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateFunction(_)))
            .expect("CreateFunction not found");
        let alter_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::AlterPolicy { .. }))
            .expect("AlterPolicy not found");

        assert!(
            func_pos < alter_pos,
            "CreateFunction ({func_pos}) before AlterPolicy ({alter_pos})"
        );
    }

    #[test]
    fn alter_view_with_function_reference() {
        let ops = vec![
            MigrationOp::AlterView {
                name: "public.active_users".to_string(),
                new_view: View {
                    name: "active_users".to_string(),
                    schema: "public".to_string(),
                    query: "SELECT auth.is_active(id) FROM public.users".to_string(),
                    materialized: false,
                    owner: None,
                    grants: Vec::new(),
                },
            },
            MigrationOp::CreateFunction(make_simple_function("is_active", "auth")),
        ];
        let planned = plan_migration(ops);

        let func_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateFunction(_)))
            .expect("CreateFunction not found");
        let alter_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::AlterView { .. }))
            .expect("AlterView not found");

        assert!(
            func_pos < alter_pos,
            "CreateFunction ({func_pos}) before AlterView ({alter_pos})"
        );
    }

    #[test]
    fn alter_column_default_with_function_reference() {
        let ops = vec![
            MigrationOp::AlterColumn {
                table: QualifiedName::new("public", "items"),
                column: "tracking_id".to_string(),
                changes: ColumnChanges {
                    data_type: None,
                    nullable: None,
                    default: Some(Some("auth.generate_tracking_id()".to_string())),
                },
            },
            MigrationOp::CreateFunction(make_simple_function("generate_tracking_id", "auth")),
        ];
        let planned = plan_migration(ops);

        let func_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateFunction(_)))
            .expect("CreateFunction not found");
        let alter_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::AlterColumn { .. }))
            .expect("AlterColumn not found");

        assert!(
            func_pos < alter_pos,
            "CreateFunction ({func_pos}) before AlterColumn ({alter_pos})"
        );
    }

    #[test]
    fn table_before_view() {
        let ops = vec![
            MigrationOp::CreateView(make_view(
                "active_users",
                "public",
                "SELECT * FROM public.users WHERE active",
            )),
            MigrationOp::CreateTable(simple_table_with_fks("users", vec![])),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateTable",
            "CreateView",
            |op| matches!(op, MigrationOp::CreateTable(_)),
            |op| matches!(op, MigrationOp::CreateView(_)),
        );
    }

    #[test]
    fn add_column_before_create_view_referencing_column() {
        // Reproduces #126: functions→add_columns pushes AddColumn later, while
        // CreateFunction→CreateView pulls the view earlier. Without add_columns→views,
        // the view can appear before AddColumn.
        let ops = vec![
            MigrationOp::CreateView(make_view(
                "supplier_users_view",
                "public",
                "SELECT public.some_func(s.is_active) FROM public.suppliers s",
            )),
            MigrationOp::AddColumn {
                table: QualifiedName::new("public", "suppliers"),
                column: Column {
                    name: "is_active".to_string(),
                    data_type: PgType::Boolean,
                    nullable: false,
                    default: Some("true".to_string()),
                    comment: None,
                },
            },
            MigrationOp::CreateFunction(make_simple_function("some_func", "public")),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "AddColumn",
            "CreateView",
            |op| matches!(op, MigrationOp::AddColumn { .. }),
            |op| matches!(op, MigrationOp::CreateView(_)),
        );
    }

    #[test]
    fn add_column_before_alter_view_referencing_column() {
        let ops = vec![
            MigrationOp::AlterView {
                name: "public.supplier_users_view".to_string(),
                new_view: make_view(
                    "supplier_users_view",
                    "public",
                    "SELECT public.some_func(s.is_active) FROM public.suppliers s",
                ),
            },
            MigrationOp::AddColumn {
                table: QualifiedName::new("public", "suppliers"),
                column: Column {
                    name: "is_active".to_string(),
                    data_type: PgType::Boolean,
                    nullable: false,
                    default: Some("true".to_string()),
                    comment: None,
                },
            },
            MigrationOp::CreateFunction(make_simple_function("some_func", "public")),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "AddColumn",
            "AlterView",
            |op| matches!(op, MigrationOp::AddColumn { .. }),
            |op| matches!(op, MigrationOp::AlterView { .. }),
        );
    }

    #[test]
    fn add_column_before_policy_referencing_column() {
        let mut policy = make_policy("active_only", "public", "suppliers");
        policy.using_expr = Some("is_active = true".to_string());
        let ops = vec![
            MigrationOp::CreatePolicy(policy),
            MigrationOp::AddColumn {
                table: QualifiedName::new("public", "suppliers"),
                column: Column {
                    name: "is_active".to_string(),
                    data_type: PgType::Boolean,
                    nullable: false,
                    default: Some("true".to_string()),
                    comment: None,
                },
            },
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "AddColumn",
            "CreatePolicy",
            |op| matches!(op, MigrationOp::AddColumn { .. }),
            |op| matches!(op, MigrationOp::CreatePolicy(_)),
        );
    }

    #[test]
    fn add_column_before_trigger_referencing_column() {
        let ops = vec![
            MigrationOp::CreateTrigger(make_trigger(
                "check_active",
                "public",
                "suppliers",
                "check_fn",
            )),
            MigrationOp::AddColumn {
                table: QualifiedName::new("public", "suppliers"),
                column: Column {
                    name: "is_active".to_string(),
                    data_type: PgType::Boolean,
                    nullable: false,
                    default: Some("true".to_string()),
                    comment: None,
                },
            },
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "AddColumn",
            "CreateTrigger",
            |op| matches!(op, MigrationOp::AddColumn { .. }),
            |op| matches!(op, MigrationOp::CreateTrigger(_)),
        );
    }

    #[test]
    fn add_column_before_add_fk() {
        let ops = vec![
            MigrationOp::AddForeignKey {
                table: QualifiedName::new("public", "posts"),
                foreign_key: make_fk("users"),
            },
            MigrationOp::AddColumn {
                table: QualifiedName::new("public", "posts"),
                column: Column {
                    name: "user_id".to_string(),
                    data_type: PgType::Integer,
                    nullable: true,
                    default: None,
                    comment: None,
                },
            },
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "AddColumn",
            "AddForeignKey",
            |op| matches!(op, MigrationOp::AddColumn { .. }),
            |op| matches!(op, MigrationOp::AddForeignKey { .. }),
        );
    }

    #[test]
    fn add_column_before_add_check() {
        let ops = vec![
            MigrationOp::AddCheckConstraint {
                table: QualifiedName::new("public", "users"),
                check_constraint: CheckConstraint {
                    name: "email_check".to_string(),
                    expression: "email LIKE '%@%'".to_string(),
                },
            },
            MigrationOp::AddColumn {
                table: QualifiedName::new("public", "users"),
                column: make_column("email"),
            },
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "AddColumn",
            "AddCheckConstraint",
            |op| matches!(op, MigrationOp::AddColumn { .. }),
            |op| matches!(op, MigrationOp::AddCheckConstraint { .. }),
        );
    }

    // --- DROP ordering ---

    #[test]
    fn drop_fk_before_drop_table() {
        let ops = vec![
            MigrationOp::DropTable("public.posts".to_string()),
            MigrationOp::DropForeignKey {
                table: QualifiedName::new("public", "posts"),
                foreign_key_name: "posts_user_fkey".to_string(),
            },
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "DropForeignKey",
            "DropTable",
            |op| matches!(op, MigrationOp::DropForeignKey { .. }),
            |op| matches!(op, MigrationOp::DropTable(_)),
        );
    }

    #[test]
    fn drop_index_before_drop_table() {
        let ops = vec![
            MigrationOp::DropTable("public.users".to_string()),
            MigrationOp::DropIndex {
                table: QualifiedName::new("public", "users"),
                index_name: "users_email_idx".to_string(),
            },
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "DropIndex",
            "DropTable",
            |op| matches!(op, MigrationOp::DropIndex { .. }),
            |op| matches!(op, MigrationOp::DropTable(_)),
        );
    }

    #[test]
    fn drop_policy_before_drop_table() {
        let ops = vec![
            MigrationOp::DropTable("public.users".to_string()),
            MigrationOp::DropPolicy {
                table: QualifiedName::new("public", "users"),
                name: "users_policy".to_string(),
            },
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "DropPolicy",
            "DropTable",
            |op| matches!(op, MigrationOp::DropPolicy { .. }),
            |op| matches!(op, MigrationOp::DropTable(_)),
        );
    }

    #[test]
    fn drop_trigger_before_drop_table() {
        let ops = vec![
            MigrationOp::DropTable("public.users".to_string()),
            MigrationOp::DropTrigger {
                target_schema: "public".to_string(),
                target_name: "users".to_string(),
                name: "audit_trigger".to_string(),
            },
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "DropTrigger",
            "DropTable",
            |op| matches!(op, MigrationOp::DropTrigger { .. }),
            |op| matches!(op, MigrationOp::DropTable(_)),
        );
    }

    #[test]
    fn drop_partition_before_drop_table() {
        let ops = vec![
            MigrationOp::DropTable("public.events".to_string()),
            MigrationOp::DropPartition("public.events_2024".to_string()),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "DropPartition",
            "DropTable",
            |op| matches!(op, MigrationOp::DropPartition(_)),
            |op| matches!(op, MigrationOp::DropTable(_)),
        );
    }

    #[test]
    fn drop_view_before_drop_table() {
        let ops = vec![
            MigrationOp::DropTable("public.users".to_string()),
            MigrationOp::DropView {
                name: "public.active_users".to_string(),
                materialized: false,
            },
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "DropView",
            "DropTable",
            |op| matches!(op, MigrationOp::DropView { .. }),
            |op| matches!(op, MigrationOp::DropTable(_)),
        );
    }

    #[test]
    fn drop_table_before_drop_enum() {
        let ops = vec![
            MigrationOp::DropEnum("public.status".to_string()),
            MigrationOp::DropTable("public.users".to_string()),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "DropTable",
            "DropEnum",
            |op| matches!(op, MigrationOp::DropTable(_)),
            |op| matches!(op, MigrationOp::DropEnum(_)),
        );
    }

    #[test]
    fn drop_table_before_drop_domain() {
        let ops = vec![
            MigrationOp::DropDomain("public.email".to_string()),
            MigrationOp::DropTable("public.users".to_string()),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "DropTable",
            "DropDomain",
            |op| matches!(op, MigrationOp::DropTable(_)),
            |op| matches!(op, MigrationOp::DropDomain(_)),
        );
    }

    #[test]
    fn drop_table_before_drop_schema() {
        let ops = vec![
            MigrationOp::DropSchema("api".to_string()),
            MigrationOp::DropTable("api.users".to_string()),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "DropTable",
            "DropSchema",
            |op| matches!(op, MigrationOp::DropTable(_)),
            |op| matches!(op, MigrationOp::DropSchema(_)),
        );
    }

    // --- ALTER column ordering ---

    #[test]
    fn drop_fk_before_alter_column() {
        let ops = vec![
            MigrationOp::AlterColumn {
                table: QualifiedName::new("public", "posts"),
                column: "user_id".to_string(),
                changes: ColumnChanges {
                    data_type: Some(PgType::Uuid),
                    nullable: None,
                    default: None,
                },
            },
            MigrationOp::DropForeignKey {
                table: QualifiedName::new("public", "posts"),
                foreign_key_name: "posts_user_fkey".to_string(),
            },
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "DropForeignKey",
            "AlterColumn",
            |op| matches!(op, MigrationOp::DropForeignKey { .. }),
            |op| matches!(op, MigrationOp::AlterColumn { .. }),
        );
    }

    #[test]
    fn alter_column_before_add_fk() {
        let ops = vec![
            MigrationOp::AddForeignKey {
                table: QualifiedName::new("public", "posts"),
                foreign_key: make_fk("users"),
            },
            MigrationOp::AlterColumn {
                table: QualifiedName::new("public", "posts"),
                column: "user_id".to_string(),
                changes: ColumnChanges {
                    data_type: Some(PgType::Uuid),
                    nullable: None,
                    default: None,
                },
            },
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "AlterColumn",
            "AddForeignKey",
            |op| matches!(op, MigrationOp::AlterColumn { .. }),
            |op| matches!(op, MigrationOp::AddForeignKey { .. }),
        );
    }

    // --- Modification patterns (drop before recreate) ---

    #[test]
    fn drop_function_before_recreate_function() {
        let func = make_simple_function("my_func", "public");
        let ops = vec![
            MigrationOp::CreateFunction(func),
            MigrationOp::DropFunction {
                name: "public.my_func".to_string(),
                args: "".to_string(),
            },
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "DropFunction",
            "CreateFunction",
            |op| matches!(op, MigrationOp::DropFunction { .. }),
            |op| matches!(op, MigrationOp::CreateFunction(_)),
        );
    }

    #[test]
    fn drop_view_before_create_view() {
        let ops = vec![
            MigrationOp::CreateView(make_view("dashboard", "public", "SELECT 1")),
            MigrationOp::DropView {
                name: "public.dashboard".to_string(),
                materialized: false,
            },
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "DropView",
            "CreateView",
            |op| matches!(op, MigrationOp::DropView { .. }),
            |op| matches!(op, MigrationOp::CreateView(_)),
        );
    }

    // --- Complex multi-object scenarios ---

    #[test]
    fn full_stack_schema_enum_table_view_trigger_policy() {
        let schema = make_schema("api");
        let my_enum = make_enum("status", "api");
        let func = make_simple_function("auth_check", "api");
        let mut table = simple_table_with_fks("users", vec![]);
        table.schema = "api".to_string();
        let view = make_view("user_view", "api", "SELECT * FROM api.users");
        let trigger = make_trigger("audit", "api", "users", "auth_check");
        let policy = make_policy("read_policy", "api", "users");

        // Input in reverse order
        let ops = vec![
            MigrationOp::CreatePolicy(policy),
            MigrationOp::CreateTrigger(trigger),
            MigrationOp::CreateView(view),
            MigrationOp::CreateTable(table),
            MigrationOp::CreateFunction(func),
            MigrationOp::CreateEnum(my_enum),
            MigrationOp::CreateSchema(schema),
        ];

        let planned = plan_migration(ops);

        let find_pos = |name: &str, finder: &dyn Fn(&MigrationOp) -> bool| -> usize {
            planned
                .iter()
                .position(finder)
                .unwrap_or_else(|| panic!("{name} not found"))
        };

        let schema_pos = find_pos("schema", &|op| matches!(op, MigrationOp::CreateSchema(_)));
        let enum_pos = find_pos("enum", &|op| matches!(op, MigrationOp::CreateEnum(_)));
        let func_pos = find_pos("function", &|op| {
            matches!(op, MigrationOp::CreateFunction(_))
        });
        let table_pos = find_pos("table", &|op| matches!(op, MigrationOp::CreateTable(_)));
        let view_pos = find_pos("view", &|op| matches!(op, MigrationOp::CreateView(_)));
        let trigger_pos = find_pos("trigger", &|op| matches!(op, MigrationOp::CreateTrigger(_)));
        let policy_pos = find_pos("policy", &|op| matches!(op, MigrationOp::CreatePolicy(_)));

        assert!(schema_pos < enum_pos, "schema before enum");
        assert!(schema_pos < func_pos, "schema before function");
        assert!(schema_pos < table_pos, "schema before table");
        assert!(enum_pos < func_pos, "enum before function");
        assert!(enum_pos < table_pos, "enum before table");
        assert!(func_pos < table_pos, "function before table");
        assert!(table_pos < view_pos, "table before view");
        assert!(table_pos < trigger_pos, "table before trigger");
        assert!(table_pos < policy_pos, "table before policy");
        assert!(func_pos < trigger_pos, "function before trigger");
        assert!(func_pos < policy_pos, "function before policy");
    }

    #[test]
    fn cross_schema_fk_ordering() {
        let mut users = simple_table_with_fks("users", vec![]);
        users.schema = "auth".to_string();

        let mut fk = make_fk("users");
        fk.name = "posts_author_fkey".to_string();
        fk.columns = vec!["author_id".to_string()];
        fk.referenced_schema = "auth".to_string();

        let mut posts = simple_table_with_fks("posts", vec![fk]);
        posts.schema = "api".to_string();

        let ops = vec![
            MigrationOp::CreateTable(posts),
            MigrationOp::CreateTable(users),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateTable(auth.users)",
            "CreateTable(api.posts)",
            |op| matches!(op, MigrationOp::CreateTable(t) if t.name == "users"),
            |op| matches!(op, MigrationOp::CreateTable(t) if t.name == "posts"),
        );
    }

    #[test]
    fn view_depends_on_another_view() {
        let base_view = make_view("base", "public", "SELECT 1 AS x");
        let derived_view = make_view("derived", "public", "SELECT x FROM public.base");

        let ops = vec![
            MigrationOp::CreateView(derived_view),
            MigrationOp::CreateView(base_view),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateView(base)",
            "CreateView(derived)",
            |op| matches!(op, MigrationOp::CreateView(v) if v.name == "base"),
            |op| matches!(op, MigrationOp::CreateView(v) if v.name == "derived"),
        );
    }

    #[test]
    fn drop_derived_view_before_drop_base_view() {
        // Names chosen so alphabetical order is WRONG (z_base before a_derived),
        // forcing the planner to use content-aware edges, not accidental sort order.
        // Includes CreateView pairs since DropView doesn't carry query info.
        let ops = vec![
            MigrationOp::DropView {
                name: "public.z_base".to_string(),
                materialized: false,
            },
            MigrationOp::DropView {
                name: "public.a_derived".to_string(),
                materialized: false,
            },
            MigrationOp::CreateView(make_view("z_base", "public", "SELECT 1 AS x")),
            MigrationOp::CreateView(make_view(
                "a_derived",
                "public",
                "SELECT x FROM public.z_base",
            )),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "DropView(a_derived)",
            "DropView(z_base)",
            |op| matches!(op, MigrationOp::DropView { name, .. } if name == "public.a_derived"),
            |op| matches!(op, MigrationOp::DropView { name, .. } if name == "public.z_base"),
        );
    }

    #[test]
    fn drop_transitive_view_chain() {
        // Names chosen so alphabetical order is WRONG (z_ first),
        // forcing the planner to use content-aware edges.
        // Includes CreateView pairs since DropView doesn't carry query info.
        let ops = vec![
            MigrationOp::DropView {
                name: "public.z_base".to_string(),
                materialized: false,
            },
            MigrationOp::DropView {
                name: "public.m_middle".to_string(),
                materialized: false,
            },
            MigrationOp::DropView {
                name: "public.a_leaf".to_string(),
                materialized: false,
            },
            MigrationOp::CreateView(make_view("z_base", "public", "SELECT 1 AS x")),
            MigrationOp::CreateView(make_view(
                "m_middle",
                "public",
                "SELECT x FROM public.z_base",
            )),
            MigrationOp::CreateView(make_view(
                "a_leaf",
                "public",
                "SELECT x FROM public.m_middle",
            )),
        ];
        let planned = plan_migration(ops);

        let leaf_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropView { name, .. } if name == "public.a_leaf"))
            .expect("DropView(a_leaf) not found");
        let middle_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropView { name, .. } if name == "public.m_middle"))
            .expect("DropView(m_middle) not found");
        let base_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropView { name, .. } if name == "public.z_base"))
            .expect("DropView(z_base) not found");

        assert!(
            leaf_pos < middle_pos,
            "DropView(a_leaf) at {leaf_pos} must come before DropView(m_middle) at {middle_pos}"
        );
        assert!(
            middle_pos < base_pos,
            "DropView(m_middle) at {middle_pos} must come before DropView(z_base) at {base_pos}"
        );
    }

    #[test]
    fn alter_column_sandwich_drop_fk_alter_add_fk() {
        let ops = vec![
            MigrationOp::AddForeignKey {
                table: QualifiedName::new("public", "posts"),
                foreign_key: make_fk("users"),
            },
            MigrationOp::AlterColumn {
                table: QualifiedName::new("public", "posts"),
                column: "user_id".to_string(),
                changes: ColumnChanges {
                    data_type: Some(PgType::BigInt),
                    nullable: None,
                    default: None,
                },
            },
            MigrationOp::DropForeignKey {
                table: QualifiedName::new("public", "posts"),
                foreign_key_name: "fk_users".to_string(),
            },
        ];
        let planned = plan_migration(ops);

        let drop_fk_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropForeignKey { .. }))
            .unwrap();
        let alter_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::AlterColumn { .. }))
            .unwrap();
        let add_fk_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::AddForeignKey { .. }))
            .unwrap();

        assert!(drop_fk_pos < alter_pos, "DropFK before AlterColumn");
        assert!(alter_pos < add_fk_pos, "AlterColumn before AddFK");
    }

    // --- Extension → domain (missing from original harness) ---

    #[test]
    fn extension_before_domain() {
        let ops = vec![
            MigrationOp::CreateDomain(make_domain("email", "public")),
            MigrationOp::CreateExtension(make_extension("citext")),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateExtension",
            "CreateDomain",
            |op| matches!(op, MigrationOp::CreateExtension(_)),
            |op| matches!(op, MigrationOp::CreateDomain(_)),
        );
    }

    // --- BackfillHint / SetColumnNotNull ordering ---

    #[test]
    fn add_column_before_backfill_hint() {
        let ops = vec![
            MigrationOp::BackfillHint {
                table: QualifiedName::new("public", "users"),
                column: "status".to_string(),
                hint: "UPDATE users SET status = 'active'".to_string(),
            },
            MigrationOp::AddColumn {
                table: QualifiedName::new("public", "users"),
                column: make_column("status"),
            },
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "AddColumn",
            "BackfillHint",
            |op| matches!(op, MigrationOp::AddColumn { .. }),
            |op| matches!(op, MigrationOp::BackfillHint { .. }),
        );
    }

    #[test]
    fn backfill_hint_before_set_column_not_null() {
        let ops = vec![
            MigrationOp::SetColumnNotNull {
                table: QualifiedName::new("public", "users"),
                column: "status".to_string(),
            },
            MigrationOp::BackfillHint {
                table: QualifiedName::new("public", "users"),
                column: "status".to_string(),
                hint: "UPDATE users SET status = 'active'".to_string(),
            },
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "BackfillHint",
            "SetColumnNotNull",
            |op| matches!(op, MigrationOp::BackfillHint { .. }),
            |op| matches!(op, MigrationOp::SetColumnNotNull { .. }),
        );
    }

    #[test]
    fn add_column_before_set_column_not_null() {
        let ops = vec![
            MigrationOp::SetColumnNotNull {
                table: QualifiedName::new("public", "users"),
                column: "status".to_string(),
            },
            MigrationOp::AddColumn {
                table: QualifiedName::new("public", "users"),
                column: make_column("status"),
            },
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "AddColumn",
            "SetColumnNotNull",
            |op| matches!(op, MigrationOp::AddColumn { .. }),
            |op| matches!(op, MigrationOp::SetColumnNotNull { .. }),
        );
    }

    // --- AlterOwner depends on object existing ---

    #[test]
    fn alter_owner_table_after_create_table() {
        let ops = vec![
            MigrationOp::AlterOwner {
                object_kind: OwnerObjectKind::Table,
                schema: "public".to_string(),
                name: "users".to_string(),
                args: None,
                new_owner: "app_admin".to_string(),
            },
            MigrationOp::CreateTable(simple_table_with_fks("users", vec![])),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateTable",
            "AlterOwner",
            |op| matches!(op, MigrationOp::CreateTable(_)),
            |op| matches!(op, MigrationOp::AlterOwner { .. }),
        );
    }

    #[test]
    fn alter_owner_view_after_create_view() {
        let ops = vec![
            MigrationOp::AlterOwner {
                object_kind: OwnerObjectKind::View,
                schema: "public".to_string(),
                name: "dashboard".to_string(),
                args: None,
                new_owner: "app_admin".to_string(),
            },
            MigrationOp::CreateView(make_view("dashboard", "public", "SELECT 1")),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateView",
            "AlterOwner",
            |op| matches!(op, MigrationOp::CreateView(_)),
            |op| matches!(op, MigrationOp::AlterOwner { .. }),
        );
    }

    #[test]
    fn alter_owner_sequence_after_create_sequence() {
        let ops = vec![
            MigrationOp::AlterOwner {
                object_kind: OwnerObjectKind::Sequence,
                schema: "public".to_string(),
                name: "counter_seq".to_string(),
                args: None,
                new_owner: "app_admin".to_string(),
            },
            MigrationOp::CreateSequence(make_sequence("counter_seq", "public")),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateSequence",
            "AlterOwner",
            |op| matches!(op, MigrationOp::CreateSequence(_)),
            |op| matches!(op, MigrationOp::AlterOwner { .. }),
        );
    }

    #[test]
    fn alter_owner_enum_after_create_enum() {
        let ops = vec![
            MigrationOp::AlterOwner {
                object_kind: OwnerObjectKind::Type,
                schema: "public".to_string(),
                name: "status".to_string(),
                args: None,
                new_owner: "app_admin".to_string(),
            },
            MigrationOp::CreateEnum(make_enum("status", "public")),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateEnum",
            "AlterOwner",
            |op| matches!(op, MigrationOp::CreateEnum(_)),
            |op| matches!(op, MigrationOp::AlterOwner { .. }),
        );
    }

    #[test]
    fn alter_owner_domain_after_create_domain() {
        let ops = vec![
            MigrationOp::AlterOwner {
                object_kind: OwnerObjectKind::Domain,
                schema: "public".to_string(),
                name: "email".to_string(),
                args: None,
                new_owner: "app_admin".to_string(),
            },
            MigrationOp::CreateDomain(make_domain("email", "public")),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateDomain",
            "AlterOwner",
            |op| matches!(op, MigrationOp::CreateDomain(_)),
            |op| matches!(op, MigrationOp::AlterOwner { .. }),
        );
    }

    #[test]
    fn alter_owner_partition_after_create_partition() {
        let partition = crate::model::Partition {
            name: "orders_2024".to_string(),
            schema: "public".to_string(),
            parent_name: "orders".to_string(),
            parent_schema: "public".to_string(),
            bound: crate::model::PartitionBound::Default,
            indexes: vec![],
            check_constraints: vec![],
            owner: None,
        };
        let ops = vec![
            MigrationOp::AlterOwner {
                object_kind: OwnerObjectKind::Partition,
                schema: "public".to_string(),
                name: "orders_2024".to_string(),
                args: None,
                new_owner: "app_admin".to_string(),
            },
            MigrationOp::CreatePartition(partition),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreatePartition",
            "AlterOwner",
            |op| matches!(op, MigrationOp::CreatePartition(_)),
            |op| matches!(op, MigrationOp::AlterOwner { .. }),
        );
    }

    #[test]
    fn alter_owner_materialized_view_after_create_view() {
        let ops = vec![
            MigrationOp::AlterOwner {
                object_kind: OwnerObjectKind::MaterializedView,
                schema: "public".to_string(),
                name: "summary".to_string(),
                args: None,
                new_owner: "app_admin".to_string(),
            },
            MigrationOp::CreateView(View {
                materialized: true,
                ..make_view("summary", "public", "SELECT 1")
            }),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "CreateView",
            "AlterOwner",
            |op| matches!(op, MigrationOp::CreateView(_)),
            |op| matches!(op, MigrationOp::AlterOwner { .. }),
        );
    }

    // --- DropIndex/DropCheckConstraint → DropColumn ---

    #[test]
    fn drop_index_before_drop_column() {
        let ops = vec![
            MigrationOp::DropColumn {
                table: QualifiedName::new("public", "users"),
                column: "email".to_string(),
            },
            MigrationOp::DropIndex {
                table: QualifiedName::new("public", "users"),
                index_name: "users_email_idx".to_string(),
            },
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "DropIndex",
            "DropColumn",
            |op| matches!(op, MigrationOp::DropIndex { .. }),
            |op| matches!(op, MigrationOp::DropColumn { .. }),
        );
    }

    #[test]
    fn drop_check_before_drop_column() {
        let ops = vec![
            MigrationOp::DropColumn {
                table: QualifiedName::new("public", "users"),
                column: "email".to_string(),
            },
            MigrationOp::DropCheckConstraint {
                table: QualifiedName::new("public", "users"),
                constraint_name: "email_check".to_string(),
            },
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "DropCheckConstraint",
            "DropColumn",
            |op| matches!(op, MigrationOp::DropCheckConstraint { .. }),
            |op| matches!(op, MigrationOp::DropColumn { .. }),
        );
    }

    #[test]
    fn drop_check_before_drop_table() {
        let ops = vec![
            MigrationOp::DropTable("public.users".to_string()),
            MigrationOp::DropCheckConstraint {
                table: QualifiedName::new("public", "users"),
                constraint_name: "email_check".to_string(),
            },
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "DropCheckConstraint",
            "DropTable",
            |op| matches!(op, MigrationOp::DropCheckConstraint { .. }),
            |op| matches!(op, MigrationOp::DropTable(_)),
        );
    }

    // --- Final creates-before-drops invariant ---

    #[test]
    fn creates_before_final_drops() {
        let ops = vec![
            MigrationOp::DropTable("public.old_table".to_string()),
            MigrationOp::CreateTable(simple_table_with_fks("new_table", vec![])),
            MigrationOp::DropEnum("public.old_status".to_string()),
            MigrationOp::CreateEnum(make_enum("new_status", "public")),
        ];
        let planned = plan_migration(ops);

        let last_create = planned
            .iter()
            .rposition(|op| matches!(op, MigrationOp::CreateTable(_) | MigrationOp::CreateEnum(_)))
            .unwrap();
        let first_drop = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropTable(_) | MigrationOp::DropEnum(_)))
            .unwrap();

        assert!(
            last_create < first_drop,
            "all creates ({last_create}) must come before final drops ({first_drop})"
        );
    }

    #[test]
    fn drop_function_excluded_from_final_drops() {
        // DropFunction is a drop-before-recreate, not a "final drop" like DropTable/DropEnum.
        // It should NOT be pushed to the end of the plan.
        let ops = vec![
            MigrationOp::DropTable("public.old_table".to_string()),
            MigrationOp::DropFunction {
                name: "public.old_fn".to_string(),
                args: "".to_string(),
            },
            MigrationOp::CreateFunction(make_simple_function("old_fn", "public")),
            MigrationOp::CreateTable(simple_table_with_fks("users", vec![])),
        ];
        let planned = plan_migration(ops);
        assert_op_position(
            &planned,
            "DropFunction",
            "CreateFunction",
            |op| matches!(op, MigrationOp::DropFunction { .. }),
            |op| matches!(op, MigrationOp::CreateFunction(_)),
        );

        let create_table_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTable(_)))
            .unwrap();
        let drop_table_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropTable(_)))
            .unwrap();
        assert!(
            create_table_pos < drop_table_pos,
            "CreateTable ({create_table_pos}) should come before DropTable ({drop_table_pos})"
        );
    }

    #[test]
    fn create_policy_using_expr_references_table() {
        let mut policy = make_policy("enterprise_access", "public", "suppliers");
        policy.using_expr = Some(
            "(EXISTS (SELECT 1 FROM enterprise_suppliers es WHERE es.supplier_id = suppliers.id))"
                .to_string(),
        );
        let ops = vec![
            MigrationOp::CreatePolicy(policy),
            MigrationOp::CreateTable(simple_table_with_fks("suppliers", vec![])),
            MigrationOp::CreateTable(simple_table_with_fks("enterprise_suppliers", vec![])),
        ];
        let planned = plan_migration(ops);

        let create_enterprise_pos = planned
            .iter()
            .position(
                |op| matches!(op, MigrationOp::CreateTable(t) if t.name == "enterprise_suppliers"),
            )
            .expect("CreateTable(enterprise_suppliers) not found");
        let policy_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreatePolicy(_)))
            .expect("CreatePolicy not found");

        assert!(
            create_enterprise_pos < policy_pos,
            "CreateTable(enterprise_suppliers) at {create_enterprise_pos} must come before CreatePolicy at {policy_pos}"
        );
    }

    #[test]
    fn create_policy_check_expr_references_table() {
        let mut policy = make_policy("insert_check", "public", "suppliers");
        policy.using_expr = None;
        policy.check_expr = Some(
            "(EXISTS (SELECT 1 FROM enterprise_suppliers es WHERE es.supplier_id = suppliers.id))"
                .to_string(),
        );
        let ops = vec![
            MigrationOp::CreatePolicy(policy),
            MigrationOp::CreateTable(simple_table_with_fks("suppliers", vec![])),
            MigrationOp::CreateTable(simple_table_with_fks("enterprise_suppliers", vec![])),
        ];
        let planned = plan_migration(ops);

        let create_enterprise_pos = planned
            .iter()
            .position(
                |op| matches!(op, MigrationOp::CreateTable(t) if t.name == "enterprise_suppliers"),
            )
            .expect("CreateTable(enterprise_suppliers) not found");
        let policy_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreatePolicy(_)))
            .expect("CreatePolicy not found");

        assert!(
            create_enterprise_pos < policy_pos,
            "CreateTable(enterprise_suppliers) at {create_enterprise_pos} must come before CreatePolicy at {policy_pos}"
        );
    }

    #[test]
    fn alter_policy_using_expr_references_table() {
        let ops = vec![
            MigrationOp::AlterPolicy {
                table: QualifiedName::new("public", "suppliers"),
                name: "enterprise_access".to_string(),
                changes: PolicyChanges {
                    roles: None,
                    using_expr: Some(Some(
                        "(EXISTS (SELECT 1 FROM enterprise_suppliers es WHERE es.supplier_id = suppliers.id))"
                            .to_string(),
                    )),
                    check_expr: None,
                },
            },
            MigrationOp::CreateTable(simple_table_with_fks("enterprise_suppliers", vec![])),
        ];
        let planned = plan_migration(ops);

        let create_table_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTable(_)))
            .expect("CreateTable not found");
        let alter_policy_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::AlterPolicy { .. }))
            .expect("AlterPolicy not found");

        assert!(
            create_table_pos < alter_policy_pos,
            "CreateTable(enterprise_suppliers) at {create_table_pos} must come before AlterPolicy at {alter_policy_pos}"
        );
    }

    #[test]
    fn alter_policy_check_expr_references_table() {
        let ops = vec![
            MigrationOp::AlterPolicy {
                table: QualifiedName::new("public", "suppliers"),
                name: "enterprise_insert".to_string(),
                changes: PolicyChanges {
                    roles: None,
                    using_expr: None,
                    check_expr: Some(Some(
                        "(EXISTS (SELECT 1 FROM enterprise_suppliers es WHERE es.supplier_id = suppliers.id))"
                            .to_string(),
                    )),
                },
            },
            MigrationOp::CreateTable(simple_table_with_fks("enterprise_suppliers", vec![])),
        ];
        let planned = plan_migration(ops);

        let create_table_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTable(_)))
            .expect("CreateTable not found");
        let alter_policy_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::AlterPolicy { .. }))
            .expect("AlterPolicy not found");

        assert!(
            create_table_pos < alter_policy_pos,
            "CreateTable(enterprise_suppliers) at {create_table_pos} must come before AlterPolicy at {alter_policy_pos}"
        );
    }
}
