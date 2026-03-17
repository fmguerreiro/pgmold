use super::{GrantObjectKind, MigrationOp, OwnerObjectKind};
use crate::model::{qualified_name, QualifiedName};

/// Extracts the type reference from a SETOF return type, if present.
/// Returns the raw type string after "SETOF " (trimmed but preserving quotes).
pub(crate) fn extract_setof_type_ref(return_type: &str) -> Option<&str> {
    let rt = return_type.trim();
    if rt.to_lowercase().starts_with("setof ") {
        Some(rt["setof ".len()..].trim())
    } else {
        None
    }
}

/// Parses a possibly-qualified type reference into (schema, name).
/// Falls back to `default_schema` when no dot is present.
pub(crate) fn parse_type_ref(type_ref: &str, default_schema: &str) -> (String, String) {
    if let Some(dot_pos) = type_ref.find('.') {
        let schema = type_ref[..dot_pos].trim().trim_matches('"');
        let name = type_ref[dot_pos + 1..].trim().trim_matches('"');
        (schema.to_string(), name.to_string())
    } else {
        (
            default_schema.to_string(),
            type_ref.trim_matches('"').to_string(),
        )
    }
}

/// Unique key for identifying each MigrationOp in the dependency graph.
/// Used for edge lookup and duplicate detection.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub(crate) enum OpKey {
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
        table: QualifiedName,
        column: String,
    },
    DropColumn {
        table: QualifiedName,
        column: String,
    },
    AlterColumn {
        table: QualifiedName,
        column: String,
    },
    AddPrimaryKey {
        table: QualifiedName,
    },
    DropPrimaryKey {
        table: QualifiedName,
    },
    AddIndex {
        table: QualifiedName,
        name: String,
    },
    DropIndex {
        table: QualifiedName,
        name: String,
    },
    AddForeignKey {
        table: QualifiedName,
        name: String,
    },
    DropForeignKey {
        table: QualifiedName,
        name: String,
    },
    AddCheckConstraint {
        table: QualifiedName,
        name: String,
    },
    DropCheckConstraint {
        table: QualifiedName,
        name: String,
    },
    EnableRls {
        table: QualifiedName,
    },
    DisableRls {
        table: QualifiedName,
    },
    CreatePolicy {
        table: QualifiedName,
        name: String,
    },
    DropPolicy {
        table: QualifiedName,
        name: String,
    },
    AlterPolicy {
        table: QualifiedName,
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
        target: QualifiedName,
        name: String,
    },
    DropTrigger {
        target: QualifiedName,
        name: String,
    },
    AlterTriggerEnabled {
        target: QualifiedName,
        name: String,
    },
    CreateSequence(String),
    DropSequence(String),
    AlterSequence(String),
    AlterOwner {
        object_kind: OwnerObjectKind,
        schema: String,
        name: String,
    },
    BackfillHint {
        table: QualifiedName,
        column: String,
    },
    SetColumnNotNull {
        table: QualifiedName,
        column: String,
    },
    GrantPrivileges {
        object_kind: GrantObjectKind,
        schema: String,
        name: String,
        grantee: String,
    },
    RevokePrivileges {
        object_kind: GrantObjectKind,
        schema: String,
        name: String,
        grantee: String,
    },
    AlterDefaultPrivileges {
        target_role: String,
        schema: Option<String>,
        object_type: String,
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
    pub(crate) fn from_op(op: &MigrationOp) -> Self {
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
            // DropUniqueConstraint maps to OpKey::DropIndex intentionally:
            // both need identical ordering (run before DropTable/DropColumn,
            // after AddIndex in replace-in-place scenarios).
            MigrationOp::DropUniqueConstraint {
                table,
                constraint_name,
            } => OpKey::DropIndex {
                table: table.clone(),
                name: constraint_name.clone(),
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
                table: QualifiedName::new(&p.table_schema, &p.table),
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
                target: QualifiedName::new(&t.target_schema, &t.target_name),
                name: t.name.clone(),
            },
            MigrationOp::DropTrigger {
                target_schema,
                target_name,
                name,
            } => OpKey::DropTrigger {
                target: QualifiedName::new(target_schema, target_name),
                name: name.clone(),
            },
            MigrationOp::AlterTriggerEnabled {
                target_schema,
                target_name,
                name,
                ..
            } => OpKey::AlterTriggerEnabled {
                target: QualifiedName::new(target_schema, target_name),
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
                object_kind: *object_kind,
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
                object_kind: *object_kind,
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
                object_kind: *object_kind,
                schema: schema.clone(),
                name: name.clone(),
                grantee: grantee.clone(),
            },
            MigrationOp::AlterDefaultPrivileges {
                target_role,
                schema,
                object_type,
                grantee,
                ..
            } => OpKey::AlterDefaultPrivileges {
                target_role: target_role.clone(),
                schema: schema.clone(),
                object_type: object_type.as_sql_str().to_string(),
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

/// Helper to add dependency edge from a Create op to a Grant/Revoke op.
/// Used for both GrantPrivileges and RevokePrivileges to ensure objects exist before granting.
pub(crate) fn add_privilege_dependency_edge(
    edges: &mut Vec<(OpKey, OpKey)>,
    object_kind: &GrantObjectKind,
    schema: &str,
    name: &str,
    args: Option<&String>,
    key: &OpKey,
) {
    let qualified = qualified_name(schema, name);
    match object_kind {
        GrantObjectKind::Table => edges.push((OpKey::CreateTable(qualified), key.clone())),
        GrantObjectKind::View => edges.push((OpKey::CreateView(qualified), key.clone())),
        GrantObjectKind::Sequence => edges.push((OpKey::CreateSequence(qualified), key.clone())),
        GrantObjectKind::Type => edges.push((OpKey::CreateEnum(qualified), key.clone())),
        GrantObjectKind::Domain => edges.push((OpKey::CreateDomain(qualified), key.clone())),
        GrantObjectKind::Schema => edges.push((OpKey::CreateSchema(name.to_string()), key.clone())),
        GrantObjectKind::Function => {
            if let Some(args) = args {
                edges.push((
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
