use std::collections::HashSet;

use crate::model::{
    CheckConstraint, Column, Domain, EnumType, ExclusionConstraint, Extension, ForeignKey,
    Function, Index, Partition, PgSchema, PgType, Policy, PrimaryKey, Privilege, QualifiedName,
    Sequence, SequenceDataType, SequenceOwner, Table, Trigger, TriggerEnabled, VersionView, View,
};

pub struct DiffOptions<'a> {
    pub manage_ownership: bool,
    pub manage_grants: bool,
    pub excluded_grant_roles: &'a HashSet<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CommentObjectType {
    Table,
    Column,
    View,
    MaterializedView,
    Function,
    Type,
    Domain,
    Schema,
    Sequence,
    Trigger,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OwnerObjectKind {
    Table,
    Partition,
    View,
    MaterializedView,
    Sequence,
    Function,
    Type,
    Domain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GrantObjectKind {
    Table,
    View,
    Sequence,
    Function,
    Schema,
    Type,
    Domain,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationOp {
    CreateSchema(PgSchema),
    DropSchema(String),
    CreateExtension(Extension),
    DropExtension(String),
    CreateEnum(EnumType),
    DropEnum(String),
    AddEnumValue {
        enum_name: String,
        value: String,
        position: Option<EnumValuePosition>,
    },
    CreateDomain(Domain),
    DropDomain(String),
    AlterDomain {
        name: String,
        changes: DomainChanges,
    },
    CreateTable(Table),
    DropTable(String),
    CreatePartition(Partition),
    DropPartition(String),
    AddColumn {
        table: QualifiedName,
        column: Column,
    },
    DropColumn {
        table: QualifiedName,
        column: String,
    },
    AlterColumn {
        table: QualifiedName,
        column: String,
        changes: ColumnChanges,
    },
    AddPrimaryKey {
        table: QualifiedName,
        primary_key: PrimaryKey,
    },
    DropPrimaryKey {
        table: QualifiedName,
    },
    AddIndex {
        table: QualifiedName,
        index: Index,
    },
    DropIndex {
        table: QualifiedName,
        index_name: String,
    },
    DropUniqueConstraint {
        table: QualifiedName,
        constraint_name: String,
    },
    AddForeignKey {
        table: QualifiedName,
        foreign_key: ForeignKey,
    },
    DropForeignKey {
        table: QualifiedName,
        foreign_key_name: String,
    },
    AddCheckConstraint {
        table: QualifiedName,
        check_constraint: CheckConstraint,
    },
    DropCheckConstraint {
        table: QualifiedName,
        constraint_name: String,
    },
    AddExclusionConstraint {
        table: QualifiedName,
        exclusion_constraint: ExclusionConstraint,
    },
    DropExclusionConstraint {
        table: QualifiedName,
        constraint_name: String,
    },
    EnableRls {
        table: QualifiedName,
    },
    DisableRls {
        table: QualifiedName,
    },
    ForceRls {
        table: QualifiedName,
    },
    NoForceRls {
        table: QualifiedName,
    },
    CreatePolicy(Policy),
    DropPolicy {
        table: QualifiedName,
        name: String,
    },
    AlterPolicy {
        table: QualifiedName,
        name: String,
        changes: PolicyChanges,
    },
    CreateFunction(Function),
    DropFunction {
        name: String,
        args: String,
    },
    AlterFunction {
        name: String,
        args: String,
        new_function: Function,
    },
    CreateView(View),
    DropView {
        name: String,
        materialized: bool,
    },
    AlterView {
        name: String,
        new_view: View,
    },
    CreateTrigger(Trigger),
    DropTrigger {
        target_schema: String,
        target_name: String,
        name: String,
    },
    AlterTriggerEnabled {
        target_schema: String,
        target_name: String,
        name: String,
        enabled: TriggerEnabled,
    },
    CreateSequence(Sequence),
    DropSequence(String),
    AlterSequence {
        name: String,
        changes: SequenceChanges,
    },
    AlterOwner {
        object_kind: OwnerObjectKind,
        schema: String,
        name: String,
        args: Option<String>,
        new_owner: String,
    },
    BackfillHint {
        table: QualifiedName,
        column: String,
        hint: String,
    },
    SetColumnNotNull {
        table: QualifiedName,
        column: String,
    },
    GrantPrivileges {
        object_kind: GrantObjectKind,
        schema: String,
        name: String,
        args: Option<String>,
        grantee: String,
        privileges: Vec<Privilege>,
        with_grant_option: bool,
    },
    RevokePrivileges {
        object_kind: GrantObjectKind,
        schema: String,
        name: String,
        args: Option<String>,
        grantee: String,
        privileges: Vec<Privilege>,
        revoke_grant_option: bool,
    },

    AlterDefaultPrivileges {
        target_role: String,
        schema: Option<String>,
        object_type: crate::model::DefaultPrivilegeObjectType,
        grantee: String,
        privileges: Vec<Privilege>,
        with_grant_option: bool,
        revoke: bool,
    },

    SetComment {
        object_type: CommentObjectType,
        schema: String,
        name: String,
        arguments: Option<String>,
        column: Option<String>,
        target: Option<String>,
        comment: Option<String>,
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
        view: VersionView,
    },
    DropVersionView {
        version_schema: String,
        name: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyChanges {
    pub roles: Option<Vec<String>>,
    pub using_expr: Option<Option<String>>,
    pub check_expr: Option<Option<String>>,
}

impl PolicyChanges {
    pub fn has_changes(&self) -> bool {
        self.roles.is_some() || self.using_expr.is_some() || self.check_expr.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnChanges {
    pub data_type: Option<PgType>,
    pub nullable: Option<bool>,
    pub default: Option<Option<String>>,
}

impl ColumnChanges {
    pub fn has_changes(&self) -> bool {
        self.data_type.is_some() || self.nullable.is_some() || self.default.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DomainChanges {
    pub default: Option<Option<String>>,
    pub not_null: Option<bool>,
}

impl DomainChanges {
    pub fn has_changes(&self) -> bool {
        self.default.is_some() || self.not_null.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SequenceChanges {
    pub data_type: Option<SequenceDataType>,
    pub increment: Option<i64>,
    pub min_value: Option<Option<i64>>,
    pub max_value: Option<Option<i64>>,
    pub restart: Option<i64>,
    pub cache: Option<i64>,
    pub cycle: Option<bool>,
    pub owned_by: Option<Option<SequenceOwner>>,
}

impl SequenceChanges {
    pub fn has_changes(&self) -> bool {
        self.data_type.is_some()
            || self.increment.is_some()
            || self.min_value.is_some()
            || self.max_value.is_some()
            || self.restart.is_some()
            || self.cache.is_some()
            || self.cycle.is_some()
            || self.owned_by.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnumValuePosition {
    Before(String),
    After(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{versioned_schema_name, ColumnMapping, VersionView};

    #[test]
    fn migration_op_alter_default_privileges_exists() {
        use crate::model::{DefaultPrivilegeObjectType, Privilege};

        let _op = MigrationOp::AlterDefaultPrivileges {
            target_role: "admin".to_string(),
            schema: Some("public".to_string()),
            object_type: DefaultPrivilegeObjectType::Tables,
            grantee: "app_user".to_string(),
            privileges: vec![Privilege::Select],
            with_grant_option: false,
            revoke: false,
        };
    }

    #[test]
    fn create_version_schema_op_pattern_matching() {
        let op = MigrationOp::CreateVersionSchema {
            base_schema: "public".to_string(),
            version: "v0001".to_string(),
        };
        match op {
            MigrationOp::CreateVersionSchema {
                base_schema,
                version,
            } => {
                assert_eq!(base_schema, "public");
                assert_eq!(version, "v0001");
                assert_eq!(
                    versioned_schema_name(&base_schema, &version),
                    "public_v0001"
                );
            }
            _ => panic!("Expected CreateVersionSchema"),
        }
    }

    #[test]
    fn drop_version_schema_op_pattern_matching() {
        let op = MigrationOp::DropVersionSchema {
            base_schema: "auth".to_string(),
            version: "v0002".to_string(),
        };
        match op {
            MigrationOp::DropVersionSchema {
                base_schema,
                version,
            } => {
                assert_eq!(base_schema, "auth");
                assert_eq!(version, "v0002");
            }
            _ => panic!("Expected DropVersionSchema"),
        }
    }

    #[test]
    fn create_version_view_op_pattern_matching() {
        let view = VersionView {
            name: "users".to_string(),
            base_schema: "public".to_string(),
            version_schema: "public_v0001".to_string(),
            base_table: "users".to_string(),
            column_mappings: vec![ColumnMapping {
                virtual_name: "id".to_string(),
                physical_name: "id".to_string(),
            }],
            security_invoker: true,
            owner: None,
        };
        let op = MigrationOp::CreateVersionView { view: view.clone() };
        match op {
            MigrationOp::CreateVersionView { view: v } => {
                assert_eq!(v.name, "users");
                assert_eq!(v.version_schema, "public_v0001");
            }
            _ => panic!("Expected CreateVersionView"),
        }
    }

    #[test]
    fn drop_version_view_op_pattern_matching() {
        let op = MigrationOp::DropVersionView {
            version_schema: "public_v0001".to_string(),
            name: "users".to_string(),
        };
        match op {
            MigrationOp::DropVersionView {
                version_schema,
                name,
            } => {
                assert_eq!(version_schema, "public_v0001");
                assert_eq!(name, "users");
            }
            _ => panic!("Expected DropVersionView"),
        }
    }
}
