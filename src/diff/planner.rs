use super::MigrationOp;
use crate::model::qualified_name;
use crate::parser::extract_table_references;
use petgraph::algo::toposort;
use petgraph::graph::{DiGraph, NodeIndex};
use std::collections::{HashMap, HashSet, VecDeque};
use thiserror::Error;

/// Error returned when migration planning fails.
#[derive(Debug, Error)]
pub enum PlanError {
    #[error("Circular dependency detected involving: {0}")]
    CyclicDependency(String),
}

/// Unique key for identifying each MigrationOp in the dependency graph.
/// Used for edge lookup and duplicate detection.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum OpKey {
    CreateSchema(String),
    DropSchema(String),
    CreateExtension(String),
    DropExtension(String),
    CreateEnum(String),
    DropEnum(String),
    AddEnumValue {
        enum_name: String,
        value: String,
    },
    CreateDomain(String),
    DropDomain(String),
    AlterDomain(String),
    CreateTable(String),
    DropTable(String),
    CreatePartition(String),
    DropPartition(String),
    AddColumn {
        table: String,
        column: String,
    },
    DropColumn {
        table: String,
        column: String,
    },
    AlterColumn {
        table: String,
        column: String,
    },
    AddPrimaryKey {
        table: String,
    },
    DropPrimaryKey {
        table: String,
    },
    AddIndex {
        table: String,
        name: String,
    },
    DropIndex {
        table: String,
        name: String,
    },
    AddForeignKey {
        table: String,
        name: String,
    },
    DropForeignKey {
        table: String,
        name: String,
    },
    AddCheckConstraint {
        table: String,
        name: String,
    },
    DropCheckConstraint {
        table: String,
        name: String,
    },
    EnableRls {
        table: String,
    },
    DisableRls {
        table: String,
    },
    CreatePolicy {
        table: String,
        name: String,
    },
    DropPolicy {
        table: String,
        name: String,
    },
    AlterPolicy {
        table: String,
        name: String,
    },
    CreateFunction {
        name: String,
        args: String,
    },
    DropFunction {
        name: String,
        args: String,
    },
    AlterFunction {
        name: String,
        args: String,
    },
    CreateView(String),
    DropView(String),
    AlterView(String),
    CreateTrigger {
        target: String,
        name: String,
    },
    DropTrigger {
        target: String,
        name: String,
    },
    AlterTriggerEnabled {
        target: String,
        name: String,
    },
    CreateSequence(String),
    DropSequence(String),
    AlterSequence(String),
    AlterOwner {
        object_kind: String,
        schema: String,
        name: String,
    },
    BackfillHint {
        table: String,
        column: String,
    },
    SetColumnNotNull {
        table: String,
        column: String,
    },
    GrantPrivileges {
        object_kind: String,
        schema: String,
        name: String,
        grantee: String,
    },
    RevokePrivileges {
        object_kind: String,
        schema: String,
        name: String,
        grantee: String,
    },
    CreateVersionSchema {
        base_schema: String,
        version: String,
    },
    DropVersionSchema {
        base_schema: String,
        version: String,
    },
    CreateVersionView {
        version_schema: String,
        name: String,
    },
    DropVersionView {
        version_schema: String,
        name: String,
    },
}

impl OpKey {
    /// Create an OpKey from a MigrationOp.
    pub fn from_op(op: &MigrationOp) -> Self {
        match op {
            MigrationOp::CreateSchema(s) => OpKey::CreateSchema(s.name.clone()),
            MigrationOp::DropSchema(name) => OpKey::DropSchema(name.clone()),
            MigrationOp::CreateExtension(ext) => OpKey::CreateExtension(ext.name.clone()),
            MigrationOp::DropExtension(name) => OpKey::DropExtension(name.clone()),
            MigrationOp::CreateEnum(e) => OpKey::CreateEnum(qualified_name(&e.schema, &e.name)),
            MigrationOp::DropEnum(name) => OpKey::DropEnum(name.clone()),
            MigrationOp::AddEnumValue {
                enum_name, value, ..
            } => OpKey::AddEnumValue {
                enum_name: enum_name.clone(),
                value: value.clone(),
            },
            MigrationOp::CreateDomain(d) => OpKey::CreateDomain(qualified_name(&d.schema, &d.name)),
            MigrationOp::DropDomain(name) => OpKey::DropDomain(name.clone()),
            MigrationOp::AlterDomain { name, .. } => OpKey::AlterDomain(name.clone()),
            MigrationOp::CreateTable(t) => OpKey::CreateTable(qualified_name(&t.schema, &t.name)),
            MigrationOp::DropTable(name) => OpKey::DropTable(name.clone()),
            MigrationOp::CreatePartition(p) => {
                OpKey::CreatePartition(qualified_name(&p.schema, &p.name))
            }
            MigrationOp::DropPartition(name) => OpKey::DropPartition(name.clone()),
            MigrationOp::AddColumn { table, column } => OpKey::AddColumn {
                table: table.clone(),
                column: column.name.clone(),
            },
            MigrationOp::DropColumn { table, column } => OpKey::DropColumn {
                table: table.clone(),
                column: column.clone(),
            },
            MigrationOp::AlterColumn { table, column, .. } => OpKey::AlterColumn {
                table: table.clone(),
                column: column.clone(),
            },
            MigrationOp::AddPrimaryKey { table, .. } => OpKey::AddPrimaryKey {
                table: table.clone(),
            },
            MigrationOp::DropPrimaryKey { table } => OpKey::DropPrimaryKey {
                table: table.clone(),
            },
            MigrationOp::AddIndex { table, index } => OpKey::AddIndex {
                table: table.clone(),
                name: index.name.clone(),
            },
            MigrationOp::DropIndex { table, index_name } => OpKey::DropIndex {
                table: table.clone(),
                name: index_name.clone(),
            },
            MigrationOp::AddForeignKey { table, foreign_key } => OpKey::AddForeignKey {
                table: table.clone(),
                name: foreign_key.name.clone(),
            },
            MigrationOp::DropForeignKey {
                table,
                foreign_key_name,
            } => OpKey::DropForeignKey {
                table: table.clone(),
                name: foreign_key_name.clone(),
            },
            MigrationOp::AddCheckConstraint {
                table,
                check_constraint,
            } => OpKey::AddCheckConstraint {
                table: table.clone(),
                name: check_constraint.name.clone(),
            },
            MigrationOp::DropCheckConstraint {
                table,
                constraint_name,
            } => OpKey::DropCheckConstraint {
                table: table.clone(),
                name: constraint_name.clone(),
            },
            MigrationOp::EnableRls { table } => OpKey::EnableRls {
                table: table.clone(),
            },
            MigrationOp::DisableRls { table } => OpKey::DisableRls {
                table: table.clone(),
            },
            MigrationOp::CreatePolicy(p) => OpKey::CreatePolicy {
                table: qualified_name(&p.table_schema, &p.table),
                name: p.name.clone(),
            },
            MigrationOp::DropPolicy { table, name } => OpKey::DropPolicy {
                table: table.clone(),
                name: name.clone(),
            },
            MigrationOp::AlterPolicy { table, name, .. } => OpKey::AlterPolicy {
                table: table.clone(),
                name: name.clone(),
            },
            MigrationOp::CreateFunction(f) => OpKey::CreateFunction {
                name: qualified_name(&f.schema, &f.name),
                args: f
                    .arguments
                    .iter()
                    .map(|a| a.data_type.clone())
                    .collect::<Vec<_>>()
                    .join(", "),
            },
            MigrationOp::DropFunction { name, args } => OpKey::DropFunction {
                name: name.clone(),
                args: args.clone(),
            },
            MigrationOp::AlterFunction { name, args, .. } => OpKey::AlterFunction {
                name: name.clone(),
                args: args.clone(),
            },
            MigrationOp::CreateView(v) => OpKey::CreateView(qualified_name(&v.schema, &v.name)),
            MigrationOp::DropView { name, .. } => OpKey::DropView(name.clone()),
            MigrationOp::AlterView { name, .. } => OpKey::AlterView(name.clone()),
            MigrationOp::CreateTrigger(t) => OpKey::CreateTrigger {
                target: qualified_name(&t.target_schema, &t.target_name),
                name: t.name.clone(),
            },
            MigrationOp::DropTrigger {
                target_schema,
                target_name,
                name,
            } => OpKey::DropTrigger {
                target: qualified_name(target_schema, target_name),
                name: name.clone(),
            },
            MigrationOp::AlterTriggerEnabled {
                target_schema,
                target_name,
                name,
                ..
            } => OpKey::AlterTriggerEnabled {
                target: qualified_name(target_schema, target_name),
                name: name.clone(),
            },
            MigrationOp::CreateSequence(s) => {
                OpKey::CreateSequence(qualified_name(&s.schema, &s.name))
            }
            MigrationOp::DropSequence(name) => OpKey::DropSequence(name.clone()),
            MigrationOp::AlterSequence { name, .. } => OpKey::AlterSequence(name.clone()),
            MigrationOp::AlterOwner {
                object_kind,
                schema,
                name,
                ..
            } => OpKey::AlterOwner {
                object_kind: format!("{object_kind:?}"),
                schema: schema.clone(),
                name: name.clone(),
            },
            MigrationOp::BackfillHint { table, column, .. } => OpKey::BackfillHint {
                table: table.clone(),
                column: column.clone(),
            },
            MigrationOp::SetColumnNotNull { table, column } => OpKey::SetColumnNotNull {
                table: table.clone(),
                column: column.clone(),
            },
            MigrationOp::GrantPrivileges {
                object_kind,
                schema,
                name,
                grantee,
                ..
            } => OpKey::GrantPrivileges {
                object_kind: format!("{object_kind:?}"),
                schema: schema.clone(),
                name: name.clone(),
                grantee: grantee.clone(),
            },
            MigrationOp::RevokePrivileges {
                object_kind,
                schema,
                name,
                grantee,
                ..
            } => OpKey::RevokePrivileges {
                object_kind: format!("{object_kind:?}"),
                schema: schema.clone(),
                name: name.clone(),
                grantee: grantee.clone(),
            },
            MigrationOp::CreateVersionSchema {
                base_schema,
                version,
            } => OpKey::CreateVersionSchema {
                base_schema: base_schema.clone(),
                version: version.clone(),
            },
            MigrationOp::DropVersionSchema {
                base_schema,
                version,
            } => OpKey::DropVersionSchema {
                base_schema: base_schema.clone(),
                version: version.clone(),
            },
            MigrationOp::CreateVersionView { view } => OpKey::CreateVersionView {
                version_schema: view.version_schema.clone(),
                name: view.name.clone(),
            },
            MigrationOp::DropVersionView {
                version_schema,
                name,
            } => OpKey::DropVersionView {
                version_schema: version_schema.clone(),
                name: name.clone(),
            },
        }
    }
}

/// Vertex in the dependency graph, wrapping a MigrationOp.
#[derive(Clone)]
struct OpVertex {
    op: MigrationOp,
}

/// Graph-based migration planner using explicit dependency edges.
pub struct MigrationGraph {
    graph: DiGraph<OpVertex, ()>,
    nodes: HashMap<OpKey, NodeIndex>,
}

impl MigrationGraph {
    /// Create a new empty migration graph.
    pub fn new() -> Self {
        Self {
            graph: DiGraph::new(),
            nodes: HashMap::new(),
        }
    }

    /// Add an operation to the graph as a vertex.
    /// Returns the NodeIndex for the new vertex.
    pub fn add_vertex(&mut self, op: MigrationOp) -> NodeIndex {
        let key = OpKey::from_op(&op);
        let node = self.graph.add_node(OpVertex { op });
        self.nodes.insert(key, node);
        node
    }

    /// Add a directed edge from one operation to another (from must run before to).
    /// Returns true if both nodes exist and the edge was added.
    pub fn add_edge(&mut self, from: &OpKey, to: &OpKey) -> bool {
        if let (Some(&from_node), Some(&to_node)) = (self.nodes.get(from), self.nodes.get(to)) {
            self.graph.add_edge(from_node, to_node, ());
            true
        } else {
            false
        }
    }

    /// Get all OpKeys currently in the graph.
    pub fn keys(&self) -> impl Iterator<Item = &OpKey> {
        self.nodes.keys()
    }

    /// Get all NodeIndexes for operations matching a predicate.
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

    /// Add edges from all nodes in `from` to all nodes in `to`.
    fn edges_all_to_all(&mut self, from: &[NodeIndex], to: &[NodeIndex]) {
        for &f in from {
            for &t in to {
                if f != t {
                    self.graph.add_edge(f, t, ());
                }
            }
        }
    }

    /// Add type-level dependency edges (all ops of type A before all ops of type B).
    #[allow(dead_code)]
    pub fn add_type_level_edges(&mut self) {
        // Collect nodes by type using pattern matching
        let schemas = self.nodes_matching(|k| matches!(k, OpKey::CreateSchema(_)));
        let version_schemas =
            self.nodes_matching(|k| matches!(k, OpKey::CreateVersionSchema { .. }));
        let extensions = self.nodes_matching(|k| matches!(k, OpKey::CreateExtension(_)));
        let enums = self.nodes_matching(|k| matches!(k, OpKey::CreateEnum(_)));
        let add_enum_values = self.nodes_matching(|k| matches!(k, OpKey::AddEnumValue { .. }));
        let domains = self.nodes_matching(|k| matches!(k, OpKey::CreateDomain(_)));
        let sequences = self.nodes_matching(|k| matches!(k, OpKey::CreateSequence(_)));
        let functions = self.nodes_matching(|k| matches!(k, OpKey::CreateFunction { .. }));
        let tables = self.nodes_matching(|k| matches!(k, OpKey::CreateTable(_)));
        let partitions = self.nodes_matching(|k| matches!(k, OpKey::CreatePartition(_)));
        let add_columns = self.nodes_matching(|k| matches!(k, OpKey::AddColumn { .. }));
        let add_pks = self.nodes_matching(|k| matches!(k, OpKey::AddPrimaryKey { .. }));
        let add_indexes = self.nodes_matching(|k| matches!(k, OpKey::AddIndex { .. }));
        let add_fks = self.nodes_matching(|k| matches!(k, OpKey::AddForeignKey { .. }));
        let add_checks = self.nodes_matching(|k| matches!(k, OpKey::AddCheckConstraint { .. }));
        let enable_rls = self.nodes_matching(|k| matches!(k, OpKey::EnableRls { .. }));
        let policies = self.nodes_matching(|k| matches!(k, OpKey::CreatePolicy { .. }));
        let triggers = self.nodes_matching(|k| matches!(k, OpKey::CreateTrigger { .. }));
        let views = self.nodes_matching(|k| matches!(k, OpKey::CreateView(_)));
        let version_views = self.nodes_matching(|k| matches!(k, OpKey::CreateVersionView { .. }));
        let alter_sequences = self.nodes_matching(|k| matches!(k, OpKey::AlterSequence(_)));

        let drop_fks = self.nodes_matching(|k| matches!(k, OpKey::DropForeignKey { .. }));
        let drop_indexes = self.nodes_matching(|k| matches!(k, OpKey::DropIndex { .. }));
        let drop_checks = self.nodes_matching(|k| matches!(k, OpKey::DropCheckConstraint { .. }));
        let drop_policies = self.nodes_matching(|k| matches!(k, OpKey::DropPolicy { .. }));
        let drop_triggers = self.nodes_matching(|k| matches!(k, OpKey::DropTrigger { .. }));
        let drop_views = self.nodes_matching(|k| matches!(k, OpKey::DropView(_)));
        let drop_columns = self.nodes_matching(|k| matches!(k, OpKey::DropColumn { .. }));
        let drop_pks = self.nodes_matching(|k| matches!(k, OpKey::DropPrimaryKey { .. }));
        let drop_tables = self.nodes_matching(|k| matches!(k, OpKey::DropTable(_)));
        let drop_partitions = self.nodes_matching(|k| matches!(k, OpKey::DropPartition(_)));
        let drop_sequences = self.nodes_matching(|k| matches!(k, OpKey::DropSequence(_)));
        let drop_domains = self.nodes_matching(|k| matches!(k, OpKey::DropDomain(_)));
        let drop_enums = self.nodes_matching(|k| matches!(k, OpKey::DropEnum(_)));
        let drop_extensions = self.nodes_matching(|k| matches!(k, OpKey::DropExtension(_)));
        let drop_version_schemas =
            self.nodes_matching(|k| matches!(k, OpKey::DropVersionSchema { .. }));
        let drop_schemas = self.nodes_matching(|k| matches!(k, OpKey::DropSchema(_)));
        let drop_version_views =
            self.nodes_matching(|k| matches!(k, OpKey::DropVersionView { .. }));

        let alter_columns = self.nodes_matching(|k| matches!(k, OpKey::AlterColumn { .. }));

        // === CREATE dependencies ===

        // Schema infrastructure first
        self.edges_all_to_all(&schemas, &tables);
        self.edges_all_to_all(&schemas, &enums);
        self.edges_all_to_all(&schemas, &domains);
        self.edges_all_to_all(&schemas, &sequences);
        self.edges_all_to_all(&schemas, &functions);
        self.edges_all_to_all(&schemas, &views);
        self.edges_all_to_all(&version_schemas, &version_views);

        // Extensions before types/tables
        self.edges_all_to_all(&extensions, &enums);
        self.edges_all_to_all(&extensions, &domains);
        self.edges_all_to_all(&extensions, &tables);

        // Types before tables
        self.edges_all_to_all(&enums, &tables);
        self.edges_all_to_all(&enums, &add_columns);
        self.edges_all_to_all(&add_enum_values, &tables);
        self.edges_all_to_all(&add_enum_values, &add_columns);
        self.edges_all_to_all(&domains, &tables);
        self.edges_all_to_all(&domains, &add_columns);
        self.edges_all_to_all(&sequences, &tables);

        // Functions before tables (used in defaults/checks)
        self.edges_all_to_all(&functions, &tables);
        self.edges_all_to_all(&functions, &add_columns);
        self.edges_all_to_all(&functions, &triggers);
        self.edges_all_to_all(&functions, &policies);

        // Tables before partitions
        self.edges_all_to_all(&tables, &partitions);

        // Tables before table-level objects
        self.edges_all_to_all(&tables, &add_columns);
        self.edges_all_to_all(&tables, &add_pks);
        self.edges_all_to_all(&tables, &add_indexes);
        self.edges_all_to_all(&tables, &add_fks);
        self.edges_all_to_all(&tables, &add_checks);
        self.edges_all_to_all(&tables, &enable_rls);
        self.edges_all_to_all(&tables, &policies);
        self.edges_all_to_all(&tables, &triggers);

        // Columns before indexes/constraints on them
        self.edges_all_to_all(&add_columns, &add_indexes);
        self.edges_all_to_all(&add_columns, &add_fks);
        self.edges_all_to_all(&add_columns, &add_checks);

        // Enable RLS before policies
        self.edges_all_to_all(&enable_rls, &policies);

        // Tables before views (views depend on tables)
        self.edges_all_to_all(&tables, &views);

        // Tables before AlterSequence (for OWNED BY)
        self.edges_all_to_all(&tables, &alter_sequences);

        // === DROP dependencies (reverse order) ===

        // Drop constraints before drop tables
        self.edges_all_to_all(&drop_fks, &drop_tables);
        self.edges_all_to_all(&drop_indexes, &drop_tables);
        self.edges_all_to_all(&drop_checks, &drop_tables);
        self.edges_all_to_all(&drop_policies, &drop_tables);
        self.edges_all_to_all(&drop_triggers, &drop_tables);
        self.edges_all_to_all(&drop_pks, &drop_tables);
        self.edges_all_to_all(&drop_columns, &drop_tables);

        // Drop partitions before parent tables
        self.edges_all_to_all(&drop_partitions, &drop_tables);

        // Drop views before drop tables they depend on
        self.edges_all_to_all(&drop_views, &drop_tables);

        // Drop version views before version schemas
        self.edges_all_to_all(&drop_version_views, &drop_version_schemas);

        // Drop tables before schemas/types
        self.edges_all_to_all(&drop_tables, &drop_schemas);
        self.edges_all_to_all(&drop_tables, &drop_enums);
        self.edges_all_to_all(&drop_tables, &drop_domains);
        self.edges_all_to_all(&drop_tables, &drop_sequences);

        // Drop sequences before extensions
        self.edges_all_to_all(&drop_sequences, &drop_extensions);

        // Drop enums/domains before extensions
        self.edges_all_to_all(&drop_enums, &drop_extensions);
        self.edges_all_to_all(&drop_domains, &drop_extensions);

        // Drop extensions before schemas
        self.edges_all_to_all(&drop_extensions, &drop_schemas);

        // === ALTER dependencies ===

        // Drop constraints before alter column type
        self.edges_all_to_all(&drop_fks, &alter_columns);
        self.edges_all_to_all(&drop_indexes, &alter_columns);
        self.edges_all_to_all(&drop_policies, &alter_columns);
        self.edges_all_to_all(&drop_triggers, &alter_columns);
        self.edges_all_to_all(&drop_views, &alter_columns);

        // Re-create constraints after alter column type
        // Pattern: DropX → AlterColumn → CreateX
        self.edges_all_to_all(&alter_columns, &add_fks);
        self.edges_all_to_all(&alter_columns, &add_indexes);
        self.edges_all_to_all(&alter_columns, &policies);
        self.edges_all_to_all(&alter_columns, &triggers);
        self.edges_all_to_all(&alter_columns, &views);

        // === MODIFICATION patterns (drop before create/alter) ===

        let drop_functions = self.nodes_matching(|k| matches!(k, OpKey::DropFunction { .. }));

        // Drop function before create function (for function modifications)
        self.edges_all_to_all(&drop_functions, &functions);

        // === CREATES BEFORE FINAL DROPS ===
        // Final drops (not for modifications) should happen after all creates complete.
        // Exclude drops that need to happen BEFORE creates/alters:
        // - DropFunction (before CreateFunction for modifications)
        // - DropFK, DropIndex, DropPolicy, DropTrigger, DropView (before AlterColumn)

        // Create operations that should complete before final drops
        let all_creates: Vec<NodeIndex> = [
            &schemas,
            &version_schemas,
            &extensions,
            &enums,
            &add_enum_values,
            &domains,
            &sequences,
            &functions,
            &tables,
            &partitions,
            &add_columns,
            &add_pks,
            &add_indexes,
            &add_fks,
            &add_checks,
            &enable_rls,
            &policies,
            &triggers,
            &views,
            &version_views,
            &alter_columns,
            &alter_sequences,
        ]
        .into_iter()
        .flatten()
        .copied()
        .collect();

        // Final drops (not temporary drops for modifications)
        // Note: DropFK, DropIndex, DropPolicy, DropTrigger, DropView, DropFunction
        // are excluded because they may need to happen before alters/creates
        let final_drops: Vec<NodeIndex> = [
            &drop_columns,
            &drop_pks,
            &drop_tables,
            &drop_partitions,
            &drop_sequences,
            &drop_domains,
            &drop_enums,
            &drop_extensions,
            &drop_version_schemas,
            &drop_schemas,
            &drop_version_views,
        ]
        .into_iter()
        .flatten()
        .copied()
        .collect();

        self.edges_all_to_all(&all_creates, &final_drops);
    }

    /// Get the MigrationOp for a given key.
    fn get_op(&self, key: &OpKey) -> Option<&MigrationOp> {
        self.nodes.get(key).map(|&idx| &self.graph[idx].op)
    }

    /// Add content-aware dependency edges (specific op A before specific op B based on content).
    #[allow(dead_code)]
    pub fn add_content_aware_edges(&mut self) {
        // Clone keys to avoid borrow issues during iteration
        let keys: Vec<_> = self.nodes.keys().cloned().collect();

        // Collect edges to add to avoid borrow issues
        let mut edges_to_add: Vec<(OpKey, OpKey)> = Vec::new();

        for key in &keys {
            match key {
                // CreateTable with FKs depends on referenced tables existing
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

                // CreateView depends on tables/views it references in its query
                OpKey::CreateView(view_name) => {
                    if let Some(MigrationOp::CreateView(view)) = self.get_op(key) {
                        let refs = extract_relation_references(&view.query);
                        for ref_name in refs {
                            // Don't add self-edge
                            if ref_name != *view_name {
                                // Try both table and view keys
                                edges_to_add
                                    .push((OpKey::CreateTable(ref_name.clone()), key.clone()));
                                edges_to_add.push((OpKey::CreateView(ref_name), key.clone()));
                            }
                        }
                    }
                }

                // Trigger depends on its target table
                OpKey::CreateTrigger { target, .. } => {
                    edges_to_add.push((OpKey::CreateTable(target.clone()), key.clone()));
                }

                // Policy depends on its table
                OpKey::CreatePolicy { table, .. } => {
                    edges_to_add.push((OpKey::CreateTable(table.clone()), key.clone()));
                }

                // Index depends on its table
                OpKey::AddIndex { table, .. } => {
                    edges_to_add.push((OpKey::CreateTable(table.clone()), key.clone()));
                }

                // AddColumn depends on table
                OpKey::AddColumn { table, .. } => {
                    edges_to_add.push((OpKey::CreateTable(table.clone()), key.clone()));
                }

                // AddCheckConstraint depends on table
                OpKey::AddCheckConstraint { table, .. } => {
                    edges_to_add.push((OpKey::CreateTable(table.clone()), key.clone()));
                }

                // DropColumn must happen after DropFK/DropIndex on that column
                OpKey::DropColumn { table, .. } => {
                    for other in &keys {
                        match other {
                            OpKey::DropForeignKey { table: t, .. } if t == table => {
                                edges_to_add.push((other.clone(), key.clone()));
                            }
                            OpKey::DropIndex { table: t, .. } if t == table => {
                                edges_to_add.push((other.clone(), key.clone()));
                            }
                            OpKey::DropCheckConstraint { table: t, .. } if t == table => {
                                edges_to_add.push((other.clone(), key.clone()));
                            }
                            _ => {}
                        }
                    }
                }

                // DropTable must happen after dropping all table objects
                OpKey::DropTable(table) => {
                    for other in &keys {
                        match other {
                            OpKey::DropForeignKey { table: t, .. } if t == table => {
                                edges_to_add.push((other.clone(), key.clone()));
                            }
                            OpKey::DropIndex { table: t, .. } if t == table => {
                                edges_to_add.push((other.clone(), key.clone()));
                            }
                            OpKey::DropPolicy { table: t, .. } if t == table => {
                                edges_to_add.push((other.clone(), key.clone()));
                            }
                            OpKey::DropTrigger { target: t, .. } if t == table => {
                                edges_to_add.push((other.clone(), key.clone()));
                            }
                            OpKey::DropColumn { table: t, .. } if t == table => {
                                edges_to_add.push((other.clone(), key.clone()));
                            }
                            OpKey::DropCheckConstraint { table: t, .. } if t == table => {
                                edges_to_add.push((other.clone(), key.clone()));
                            }
                            _ => {}
                        }
                    }
                }

                // AlterColumn must happen after dropping dependent objects
                OpKey::AlterColumn { table, .. } => {
                    for other in &keys {
                        match other {
                            OpKey::DropForeignKey { table: t, .. } if t == table => {
                                edges_to_add.push((other.clone(), key.clone()));
                            }
                            OpKey::DropIndex { table: t, .. } if t == table => {
                                edges_to_add.push((other.clone(), key.clone()));
                            }
                            OpKey::DropPolicy { table: t, .. } if t == table => {
                                edges_to_add.push((other.clone(), key.clone()));
                            }
                            OpKey::DropTrigger { target: t, .. } if t == table => {
                                edges_to_add.push((other.clone(), key.clone()));
                            }
                            _ => {}
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

                _ => {}
            }
        }

        // Add all collected edges
        for (from, to) in edges_to_add {
            self.add_edge(&from, &to);
        }
    }

    /// Perform topological sort and return operations in dependency order.
    /// Type-level edges establish priority relationships (schemas before tables, etc.).
    /// Content-aware edges handle specific dependencies (FK ordering, etc.).
    pub fn topological_sort(&self) -> Result<Vec<MigrationOp>, PlanError> {
        // Get topological order - fails if there's a cycle
        let sorted = toposort(&self.graph, None).map_err(|cycle| {
            let node = cycle.node_id();
            let op = &self.graph[node].op;
            PlanError::CyclicDependency(format!("{op:?}"))
        })?;

        // Return operations in topological order
        // Priority is encoded in type-level edges, not post-hoc sorting
        Ok(sorted
            .into_iter()
            .map(|node| self.graph[node].op.clone())
            .collect())
    }
}

impl Default for MigrationGraph {
    fn default() -> Self {
        Self::new()
    }
}

/// Plan migration operations using graph-based dependency ordering.
/// Returns an error if a circular dependency is detected.
pub fn plan_migration_checked(ops: Vec<MigrationOp>) -> Result<Vec<MigrationOp>, PlanError> {
    // Pre-process: Split sequences with owned_by into CreateSequence + AlterSequence
    let processed_ops = preprocess_ops(ops);

    let mut graph = MigrationGraph::new();

    // Add all ops as vertices
    for op in processed_ops {
        graph.add_vertex(op);
    }

    // Add dependency edges
    graph.add_type_level_edges();
    graph.add_content_aware_edges();

    // Sort and return
    graph.topological_sort()
}

/// Pre-process operations to handle special cases.
fn preprocess_ops(ops: Vec<MigrationOp>) -> Vec<MigrationOp> {
    let mut result = Vec::new();

    for op in ops {
        match op {
            // Split CreateSequence with owned_by into CreateSequence + AlterSequence
            MigrationOp::CreateSequence(ref seq) if seq.owned_by.is_some() => {
                let owned_by = seq.owned_by.as_ref().unwrap();
                let mut seq_without_owner = seq.clone();
                seq_without_owner.owned_by = None;
                result.push(MigrationOp::CreateSequence(seq_without_owner));

                let changes = super::SequenceChanges {
                    owned_by: Some(Some(owned_by.clone())),
                    ..Default::default()
                };
                result.push(MigrationOp::AlterSequence {
                    name: qualified_name(&seq.schema, &seq.name),
                    changes,
                });
            }
            _ => result.push(op),
        }
    }

    result
}

/// Plan and order migration operations for safe execution.
/// Uses graph-based dependency ordering for correct operation sequencing.
/// Panics if a circular dependency is detected (use `plan_migration_checked` to handle errors).
pub fn plan_migration(ops: Vec<MigrationOp>) -> Vec<MigrationOp> {
    plan_migration_checked(ops).expect("Circular dependency detected in migration operations")
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

fn extract_relation_references(query: &str) -> HashSet<String> {
    extract_table_references(query, "public")
        .into_iter()
        .map(|r| r.qualified_name())
        .collect()
}

fn order_view_creates(ops: Vec<MigrationOp>) -> Vec<MigrationOp> {
    if ops.is_empty() {
        return ops;
    }

    let view_names: HashSet<String> = ops
        .iter()
        .filter_map(|op| match op {
            MigrationOp::CreateView(view) => Some(qualified_name(&view.schema, &view.name)),
            _ => None,
        })
        .collect();

    let mut view_ops: HashMap<String, MigrationOp> = HashMap::new();
    let mut dependencies: HashMap<String, HashSet<String>> = HashMap::new();

    for op in ops {
        if let MigrationOp::CreateView(ref view) = op {
            let view_name = qualified_name(&view.schema, &view.name);

            let deps: HashSet<String> = extract_relation_references(&view.query)
                .into_iter()
                .filter(|r| view_names.contains(r) && *r != view_name)
                .collect();

            dependencies.insert(view_name.clone(), deps);
            view_ops.insert(view_name, op);
        }
    }

    topological_sort(&view_ops, &dependencies)
}

fn order_table_creates(ops: Vec<MigrationOp>) -> Vec<MigrationOp> {
    if ops.is_empty() {
        return ops;
    }

    let mut table_ops: HashMap<String, MigrationOp> = HashMap::new();
    let mut dependencies: HashMap<String, HashSet<String>> = HashMap::new();

    for op in ops {
        if let MigrationOp::CreateTable(ref table) = op {
            let table_name = qualified_name(&table.schema, &table.name);

            let deps: HashSet<String> = table
                .foreign_keys
                .iter()
                .map(|fk| qualified_name(&fk.referenced_schema, &fk.referenced_table))
                .filter(|r| *r != table_name)
                .collect();

            dependencies.insert(table_name.clone(), deps);
            table_ops.insert(table_name, op);
        }
    }

    topological_sort(&table_ops, &dependencies)
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
    use crate::diff::ColumnChanges;
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
                table: "public.posts".to_string(),
                column: "user_id".to_string(),
                changes: crate::diff::ColumnChanges {
                    data_type: Some(PgType::Uuid),
                    nullable: None,
                    default: None,
                },
            },
            MigrationOp::DropForeignKey {
                table: "public.posts".to_string(),
                foreign_key_name: "posts_user_id_fkey".to_string(),
            },
            MigrationOp::AddForeignKey {
                table: "public.posts".to_string(),
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
                table: "public.users".to_string(),
                column: "id".to_string(),
                changes: crate::diff::ColumnChanges {
                    data_type: Some(PgType::Uuid),
                    nullable: None,
                    default: None,
                },
            },
            MigrationOp::DropPolicy {
                table: "public.users".to_string(),
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
                table: "public.users".to_string(),
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
                table: "public.users".to_string(),
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

    // === Graph planner v2 tests ===

    #[test]
    fn v2_basic_create_table() {
        let users = make_table("users", vec![]);
        let ops = vec![MigrationOp::CreateTable(users)];

        let v2_result = plan_migration_checked(ops.clone()).unwrap();
        let bucket_result = plan_migration(ops);

        assert_eq!(v2_result.len(), bucket_result.len());
    }

    #[test]
    fn v2_fk_dependencies() {
        let posts = make_table("posts", vec![make_fk("users")]);
        let users = make_table("users", vec![]);

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
        let users = make_table("users", vec![]);

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
                table: "public.users".to_string(),
                column: "id".to_string(),
                changes: ColumnChanges {
                    data_type: Some(PgType::Text),
                    nullable: None,
                    default: None,
                },
            },
            MigrationOp::DropForeignKey {
                table: "public.users".to_string(),
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
        let users = make_table("users", vec![]);
        let posts = make_table("posts", vec![make_fk("users")]);

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

        let users = make_table("users", vec![]);
        let posts = make_table("posts", vec![make_fk("users")]);
        let comments = make_table("comments", vec![make_fk("posts"), make_fk("users")]);

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
}
