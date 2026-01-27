use super::MigrationOp;
use crate::model::qualified_name;
use crate::parser::{extract_function_references, extract_table_references};
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

/// Vertex in the dependency graph, wrapping a MigrationOp with its priority.
#[derive(Clone)]
struct OpVertex {
    op: MigrationOp,
    priority: i32,
}

/// Operation type for grouping operations in type-level dependency rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum OpType {
    CreateSchema,
    DropSchema,
    CreateExtension,
    DropExtension,
    CreateEnum,
    DropEnum,
    AddEnumValue,
    CreateDomain,
    DropDomain,
    AlterDomain,
    CreateSequence,
    DropSequence,
    AlterSequence,
    CreateFunction,
    DropFunction,
    AlterFunction,
    CreateTable,
    DropTable,
    CreatePartition,
    DropPartition,
    AddColumn,
    DropColumn,
    AlterColumn,
    AddPrimaryKey,
    DropPrimaryKey,
    AddIndex,
    DropIndex,
    AddForeignKey,
    DropForeignKey,
    AddCheckConstraint,
    DropCheckConstraint,
    EnableRls,
    DisableRls,
    CreatePolicy,
    DropPolicy,
    AlterPolicy,
    CreateView,
    DropView,
    AlterView,
    CreateTrigger,
    DropTrigger,
    AlterTriggerEnabled,
    AlterOwner,
    BackfillHint,
    SetColumnNotNull,
    GrantPrivileges,
    RevokePrivileges,
    CreateVersionSchema,
    DropVersionSchema,
    CreateVersionView,
    DropVersionView,
}

impl OpKey {
    /// Get the operation type for this key.
    fn op_type(&self) -> OpType {
        match self {
            OpKey::CreateSchema(_) => OpType::CreateSchema,
            OpKey::DropSchema(_) => OpType::DropSchema,
            OpKey::CreateExtension(_) => OpType::CreateExtension,
            OpKey::DropExtension(_) => OpType::DropExtension,
            OpKey::CreateEnum(_) => OpType::CreateEnum,
            OpKey::DropEnum(_) => OpType::DropEnum,
            OpKey::AddEnumValue { .. } => OpType::AddEnumValue,
            OpKey::CreateDomain(_) => OpType::CreateDomain,
            OpKey::DropDomain(_) => OpType::DropDomain,
            OpKey::AlterDomain(_) => OpType::AlterDomain,
            OpKey::CreateTable(_) => OpType::CreateTable,
            OpKey::DropTable(_) => OpType::DropTable,
            OpKey::CreatePartition(_) => OpType::CreatePartition,
            OpKey::DropPartition(_) => OpType::DropPartition,
            OpKey::AddColumn { .. } => OpType::AddColumn,
            OpKey::DropColumn { .. } => OpType::DropColumn,
            OpKey::AlterColumn { .. } => OpType::AlterColumn,
            OpKey::AddPrimaryKey { .. } => OpType::AddPrimaryKey,
            OpKey::DropPrimaryKey { .. } => OpType::DropPrimaryKey,
            OpKey::AddIndex { .. } => OpType::AddIndex,
            OpKey::DropIndex { .. } => OpType::DropIndex,
            OpKey::AddForeignKey { .. } => OpType::AddForeignKey,
            OpKey::DropForeignKey { .. } => OpType::DropForeignKey,
            OpKey::AddCheckConstraint { .. } => OpType::AddCheckConstraint,
            OpKey::DropCheckConstraint { .. } => OpType::DropCheckConstraint,
            OpKey::EnableRls { .. } => OpType::EnableRls,
            OpKey::DisableRls { .. } => OpType::DisableRls,
            OpKey::CreatePolicy { .. } => OpType::CreatePolicy,
            OpKey::DropPolicy { .. } => OpType::DropPolicy,
            OpKey::AlterPolicy { .. } => OpType::AlterPolicy,
            OpKey::CreateFunction { .. } => OpType::CreateFunction,
            OpKey::DropFunction { .. } => OpType::DropFunction,
            OpKey::AlterFunction { .. } => OpType::AlterFunction,
            OpKey::CreateView(_) => OpType::CreateView,
            OpKey::DropView(_) => OpType::DropView,
            OpKey::AlterView(_) => OpType::AlterView,
            OpKey::CreateTrigger { .. } => OpType::CreateTrigger,
            OpKey::DropTrigger { .. } => OpType::DropTrigger,
            OpKey::AlterTriggerEnabled { .. } => OpType::AlterTriggerEnabled,
            OpKey::CreateSequence(_) => OpType::CreateSequence,
            OpKey::DropSequence(_) => OpType::DropSequence,
            OpKey::AlterSequence(_) => OpType::AlterSequence,
            OpKey::AlterOwner { .. } => OpType::AlterOwner,
            OpKey::BackfillHint { .. } => OpType::BackfillHint,
            OpKey::SetColumnNotNull { .. } => OpType::SetColumnNotNull,
            OpKey::GrantPrivileges { .. } => OpType::GrantPrivileges,
            OpKey::RevokePrivileges { .. } => OpType::RevokePrivileges,
            OpKey::CreateVersionSchema { .. } => OpType::CreateVersionSchema,
            OpKey::DropVersionSchema { .. } => OpType::DropVersionSchema,
            OpKey::CreateVersionView { .. } => OpType::CreateVersionView,
            OpKey::DropVersionView { .. } => OpType::DropVersionView,
        }
    }
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
    pub fn add_vertex(&mut self, op: MigrationOp, priority: i32) -> NodeIndex {
        let key = OpKey::from_op(&op);
        let node = self.graph.add_node(OpVertex { op, priority });
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

    /// Get the NodeIndex for an operation by its key.
    pub fn get_node(&self, key: &OpKey) -> Option<NodeIndex> {
        self.nodes.get(key).copied()
    }

    /// Get all OpKeys currently in the graph.
    pub fn keys(&self) -> impl Iterator<Item = &OpKey> {
        self.nodes.keys()
    }

    /// Get the number of operations in the graph.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.graph.node_count()
    }

    /// Check if the graph is empty.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.graph.node_count() == 0
    }

    /// Get all NodeIndexes for operations of a specific type.
    fn nodes_of_type(&self, op_type: OpType) -> Vec<NodeIndex> {
        self.nodes
            .iter()
            .filter(|(key, _)| key.op_type() == op_type)
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
        // Collect nodes by type
        let schemas = self.nodes_of_type(OpType::CreateSchema);
        let version_schemas = self.nodes_of_type(OpType::CreateVersionSchema);
        let extensions = self.nodes_of_type(OpType::CreateExtension);
        let enums = self.nodes_of_type(OpType::CreateEnum);
        let add_enum_values = self.nodes_of_type(OpType::AddEnumValue);
        let domains = self.nodes_of_type(OpType::CreateDomain);
        let sequences = self.nodes_of_type(OpType::CreateSequence);
        let functions = self.nodes_of_type(OpType::CreateFunction);
        let tables = self.nodes_of_type(OpType::CreateTable);
        let partitions = self.nodes_of_type(OpType::CreatePartition);
        let add_columns = self.nodes_of_type(OpType::AddColumn);
        let add_pks = self.nodes_of_type(OpType::AddPrimaryKey);
        let add_indexes = self.nodes_of_type(OpType::AddIndex);
        let add_fks = self.nodes_of_type(OpType::AddForeignKey);
        let add_checks = self.nodes_of_type(OpType::AddCheckConstraint);
        let enable_rls = self.nodes_of_type(OpType::EnableRls);
        let policies = self.nodes_of_type(OpType::CreatePolicy);
        let triggers = self.nodes_of_type(OpType::CreateTrigger);
        let views = self.nodes_of_type(OpType::CreateView);
        let version_views = self.nodes_of_type(OpType::CreateVersionView);
        let alter_sequences = self.nodes_of_type(OpType::AlterSequence);

        let drop_fks = self.nodes_of_type(OpType::DropForeignKey);
        let drop_indexes = self.nodes_of_type(OpType::DropIndex);
        let drop_checks = self.nodes_of_type(OpType::DropCheckConstraint);
        let drop_policies = self.nodes_of_type(OpType::DropPolicy);
        let drop_triggers = self.nodes_of_type(OpType::DropTrigger);
        let drop_views = self.nodes_of_type(OpType::DropView);
        let drop_columns = self.nodes_of_type(OpType::DropColumn);
        let drop_pks = self.nodes_of_type(OpType::DropPrimaryKey);
        let drop_tables = self.nodes_of_type(OpType::DropTable);
        let drop_partitions = self.nodes_of_type(OpType::DropPartition);
        let drop_sequences = self.nodes_of_type(OpType::DropSequence);
        let drop_domains = self.nodes_of_type(OpType::DropDomain);
        let drop_enums = self.nodes_of_type(OpType::DropEnum);
        let drop_extensions = self.nodes_of_type(OpType::DropExtension);
        let drop_version_schemas = self.nodes_of_type(OpType::DropVersionSchema);
        let drop_schemas = self.nodes_of_type(OpType::DropSchema);
        let drop_version_views = self.nodes_of_type(OpType::DropVersionView);

        let alter_columns = self.nodes_of_type(OpType::AlterColumn);

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
                            // Don't add self-edge
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

                _ => {}
            }
        }

        // Add all collected edges
        for (from, to) in edges_to_add {
            self.add_edge(&from, &to);
        }
    }

    /// Perform topological sort and return operations in dependency order.
    /// Uses priority as a tiebreaker for deterministic output.
    #[allow(dead_code)]
    pub fn topological_sort(&self) -> Result<Vec<MigrationOp>, PlanError> {
        // Get topological order - fails if there's a cycle
        let sorted = toposort(&self.graph, None).map_err(|cycle| {
            let node = cycle.node_id();
            let op = &self.graph[node].op;
            PlanError::CyclicDependency(format!("{op:?}"))
        })?;

        // Collect vertices with their original index for stable sorting
        let mut vertices: Vec<(usize, &OpVertex)> = sorted
            .into_iter()
            .enumerate()
            .map(|(idx, node)| (idx, &self.graph[node]))
            .collect();

        // Sort by priority, using original index as tiebreaker for same priority
        // This ensures deterministic output while respecting topological constraints
        vertices
            .sort_by(|(idx_a, a), (idx_b, b)| a.priority.cmp(&b.priority).then(idx_a.cmp(idx_b)));

        Ok(vertices.into_iter().map(|(_, v)| v.op.clone()).collect())
    }
}

impl Default for MigrationGraph {
    fn default() -> Self {
        Self::new()
    }
}

/// Assign a priority value to each operation type for tiebreaking in topological sort.
/// Lower values run earlier. This mirrors the implicit ordering from the bucket-based approach.
fn priority_for(op: &MigrationOp) -> i32 {
    match op {
        // Schema infrastructure (earliest)
        MigrationOp::CreateSchema(_) => 0,
        MigrationOp::CreateVersionSchema { .. } => 5,
        MigrationOp::CreateExtension(_) => 10,

        // Types before tables
        MigrationOp::CreateEnum(_) => 20,
        MigrationOp::AddEnumValue { .. } => 25,
        MigrationOp::CreateDomain(_) => 30,
        MigrationOp::CreateSequence(_) => 40,

        // Functions before tables (may be used in defaults/checks)
        MigrationOp::DropFunction { .. } => 45,
        MigrationOp::CreateFunction(_) => 50,

        // Tables
        MigrationOp::CreateTable(_) => 60,
        MigrationOp::CreatePartition(_) => 65,
        MigrationOp::AddColumn { .. } => 70,
        MigrationOp::AddPrimaryKey { .. } => 80,
        MigrationOp::DropIndex { .. } => 85,
        MigrationOp::AddIndex { .. } => 90,

        // Constraints and policies - drops before alters
        MigrationOp::DropForeignKey { .. } => 95,
        MigrationOp::DropPolicy { .. } => 96,
        MigrationOp::DropTrigger { .. } => 97,
        MigrationOp::DropView { .. } => 98,
        MigrationOp::AlterColumn { .. } => 100,
        MigrationOp::SetColumnNotNull { .. } => 105,
        MigrationOp::DropCheckConstraint { .. } => 108,
        MigrationOp::AddForeignKey { .. } => 110,
        MigrationOp::AddCheckConstraint { .. } => 115,

        // RLS and policies
        MigrationOp::EnableRls { .. } => 120,
        MigrationOp::CreatePolicy(_) => 125,
        MigrationOp::AlterPolicy { .. } => 130,

        // Sequences, domains, functions (alters)
        MigrationOp::AlterSequence { .. } => 140,
        MigrationOp::AlterDomain { .. } => 145,
        MigrationOp::AlterFunction { .. } => 150,

        // Views and triggers
        MigrationOp::CreateView(_) => 160,
        MigrationOp::AlterView { .. } => 165,
        MigrationOp::CreateVersionView { .. } => 170,
        MigrationOp::CreateTrigger(_) => 175,
        MigrationOp::AlterTriggerEnabled { .. } => 180,

        // Ownership and grants
        MigrationOp::AlterOwner { .. } => 190,
        MigrationOp::GrantPrivileges { .. } => 195,
        MigrationOp::BackfillHint { .. } => 198,

        // Drops (later)
        MigrationOp::RevokePrivileges { .. } => 500,
        MigrationOp::DropVersionView { .. } => 510,
        MigrationOp::DisableRls { .. } => 520,
        MigrationOp::DropPrimaryKey { .. } => 540,
        MigrationOp::DropColumn { .. } => 550,
        MigrationOp::DropPartition(_) => 555,
        MigrationOp::DropTable(_) => 560,
        MigrationOp::DropSequence(_) => 570,
        MigrationOp::DropDomain(_) => 575,
        MigrationOp::DropEnum(_) => 580,
        MigrationOp::DropExtension(_) => 585,
        MigrationOp::DropVersionSchema { .. } => 590,
        MigrationOp::DropSchema(_) => 595,
    }
}

/// Plan migration operations using graph-based dependency ordering.
/// Returns an error if a circular dependency is detected.
pub fn plan_migration_checked(ops: Vec<MigrationOp>) -> Result<Vec<MigrationOp>, PlanError> {
    // Pre-process: Split sequences with owned_by into CreateSequence + AlterSequence
    let processed_ops = preprocess_ops(ops);

    let mut graph = MigrationGraph::new();

    // Add all ops as vertices with their priorities
    for op in processed_ops {
        let priority = priority_for(&op);
        graph.add_vertex(op, priority);
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

/// Plan and order migration operations for safe execution using bucket-based approach.
/// This is the legacy implementation - use `plan_migration` for the new graph-based approach.
#[allow(dead_code)]
fn plan_migration_bucket(ops: Vec<MigrationOp>) -> Vec<MigrationOp> {
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
    let create_functions = order_function_creates(create_functions);
    let create_views = order_view_creates(create_views);

    // Split FK drops into two groups:
    // 1. Temporary drops (matching add exists) - needed before ALTER COLUMN TYPE
    // 2. Permanent drops (no matching add) - stay at original position
    let fk_add_keys: HashSet<(String, String)> = add_foreign_keys
        .iter()
        .filter_map(|op| {
            if let MigrationOp::AddForeignKey { table, foreign_key } = op {
                Some((table.clone(), foreign_key.name.clone()))
            } else {
                None
            }
        })
        .collect();

    let (drop_fks_for_type_change, drop_fks_permanent): (Vec<_>, Vec<_>) =
        drop_foreign_keys.into_iter().partition(|op| {
            if let MigrationOp::DropForeignKey {
                table,
                foreign_key_name,
            } = op
            {
                fk_add_keys.contains(&(table.clone(), foreign_key_name.clone()))
            } else {
                false
            }
        });

    // Split policy drops into two groups:
    // 1. Temporary drops (matching create exists) - needed before ALTER COLUMN TYPE
    // 2. Permanent drops (no matching create) - stay at original position
    let policy_create_keys: HashSet<(String, String)> = create_policies
        .iter()
        .filter_map(|op| {
            if let MigrationOp::CreatePolicy(policy) = op {
                Some((
                    qualified_name(&policy.table_schema, &policy.table),
                    policy.name.clone(),
                ))
            } else {
                None
            }
        })
        .collect();

    let (drop_policies_for_type_change, drop_policies_permanent): (Vec<_>, Vec<_>) =
        drop_policies.into_iter().partition(|op| {
            if let MigrationOp::DropPolicy { table, name } = op {
                policy_create_keys.contains(&(table.clone(), name.clone()))
            } else {
                false
            }
        });

    // Split trigger drops into two groups:
    // 1. Temporary drops (matching create exists) - needed before ALTER COLUMN TYPE
    // 2. Permanent drops (no matching create) - stay at original position
    let trigger_create_keys: HashSet<(String, String, String)> = create_triggers
        .iter()
        .filter_map(|op| {
            if let MigrationOp::CreateTrigger(trigger) = op {
                Some((
                    trigger.target_schema.clone(),
                    trigger.target_name.clone(),
                    trigger.name.clone(),
                ))
            } else {
                None
            }
        })
        .collect();

    let (drop_triggers_for_type_change, drop_triggers_permanent): (Vec<_>, Vec<_>) =
        drop_triggers.into_iter().partition(|op| {
            if let MigrationOp::DropTrigger {
                target_schema,
                target_name,
                name,
            } = op
            {
                trigger_create_keys.contains(&(
                    target_schema.clone(),
                    target_name.clone(),
                    name.clone(),
                ))
            } else {
                false
            }
        });

    // Split view drops into two groups:
    // 1. Temporary drops (matching create exists) - needed before ALTER COLUMN TYPE
    // 2. Permanent drops (no matching create) - stay at original position
    let view_create_keys: HashSet<String> = create_views
        .iter()
        .filter_map(|op| {
            if let MigrationOp::CreateView(view) = op {
                Some(qualified_name(&view.schema, &view.name))
            } else {
                None
            }
        })
        .collect();

    let (drop_views_for_type_change, drop_views_permanent): (Vec<_>, Vec<_>) =
        drop_views.into_iter().partition(|op| {
            if let MigrationOp::DropView { name, .. } = op {
                view_create_keys.contains(name)
            } else {
                false
            }
        });

    let mut result = Vec::new();

    result.extend(create_schemas);
    // Create version schemas early (right after base schemas) since they're empty containers
    // that version views will be created in later
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
    // Drop FKs that have matching adds (temporary drops for ALTER COLUMN TYPE)
    // PostgreSQL requires FK to be dropped before altering referenced column types
    result.extend(drop_fks_for_type_change);
    // Drop policies that have matching creates (temporary drops for ALTER COLUMN TYPE)
    // PostgreSQL requires policies to be dropped before altering column types they reference
    result.extend(drop_policies_for_type_change);
    // Drop triggers that have matching creates (temporary drops for ALTER COLUMN TYPE)
    // PostgreSQL requires triggers to be dropped before altering column types they reference
    result.extend(drop_triggers_for_type_change);
    // Drop views that have matching creates (temporary drops for ALTER COLUMN TYPE)
    // PostgreSQL requires views to be dropped before altering column types they reference
    result.extend(drop_views_for_type_change);
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
    // Create version views AFTER base tables and views exist (version views reference them)
    result.extend(create_version_views);
    result.extend(create_triggers);
    result.extend(alter_triggers);
    result.extend(alter_owners);
    result.extend(grant_privileges);

    result.extend(revoke_privileges);
    // Note: drop_triggers_for_type_change is handled earlier (before alter_columns)
    // to support ALTER COLUMN TYPE on columns referenced by triggers
    result.extend(drop_triggers_permanent);
    // Drop version views BEFORE dropping base views (version views depend on base tables)
    result.extend(drop_version_views);
    // Note: drop_views_for_type_change is handled earlier (before alter_columns)
    // to support ALTER COLUMN TYPE on columns referenced by views
    result.extend(drop_views_permanent);
    // Note: drop_policies_for_type_change is handled earlier (before alter_columns)
    // to support ALTER COLUMN TYPE on columns referenced by policies
    result.extend(drop_policies_permanent);
    result.extend(disable_rls);
    // Note: drop_check_constraints is handled earlier (before add_check_constraints)
    // to support constraint modifications
    // Note: drop_fks_for_type_change is handled earlier (before alter_columns)
    // to support ALTER COLUMN TYPE on FK-involved columns
    result.extend(drop_fks_permanent);
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
    // Drop version schemas at the end via CASCADE (drops all version views automatically)
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

fn extract_relation_references(query: &str) -> HashSet<String> {
    extract_table_references(query, "public")
        .into_iter()
        .map(|r| r.qualified_name())
        .collect()
}

fn order_function_creates(ops: Vec<MigrationOp>) -> Vec<MigrationOp> {
    if ops.is_empty() {
        return ops;
    }

    fn func_signature(func: &crate::model::Function) -> String {
        let args: Vec<&str> = func
            .arguments
            .iter()
            .map(|a| a.data_type.as_str())
            .collect();
        qualified_name(&func.schema, &format!("{}({})", func.name, args.join(", ")))
    }

    let function_sigs: HashSet<String> = ops
        .iter()
        .filter_map(|op| match op {
            MigrationOp::CreateFunction(func) => Some(func_signature(func)),
            _ => None,
        })
        .collect();

    let mut function_ops: HashMap<String, MigrationOp> = HashMap::new();
    let mut dependencies: HashMap<String, HashSet<String>> = HashMap::new();

    for op in ops {
        if let MigrationOp::CreateFunction(ref func) = op {
            let func_sig = func_signature(func);
            let func_sig_for_filter = func_sig.clone();

            let deps: HashSet<String> = extract_function_references(&func.body, &func.schema)
                .into_iter()
                .flat_map(|func_ref| {
                    let ref_prefix = format!("{}.{}(", func_ref.schema, func_ref.name);
                    let exclude_sig = func_sig_for_filter.clone();
                    function_sigs
                        .iter()
                        .filter(move |sig| sig.starts_with(&ref_prefix) && **sig != exclude_sig)
                        .cloned()
                })
                .collect();

            dependencies.insert(func_sig.clone(), deps);
            function_ops.insert(func_sig, op);
        }
    }

    topological_sort(&function_ops, &dependencies)
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

fn order_table_drops(ops: Vec<MigrationOp>) -> Vec<MigrationOp> {
    if ops.is_empty() {
        return ops;
    }

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
}
