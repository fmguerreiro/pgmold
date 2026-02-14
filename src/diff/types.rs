use crate::model::{
    CheckConstraint, Column, Domain, EnumType, Extension, ForeignKey, Function, Index, Partition,
    PgSchema, PgType, Policy, PrimaryKey, Privilege, Sequence, SequenceDataType, SequenceOwner,
    Table, Trigger, TriggerEnabled, VersionView, View,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnerObjectKind {
    Table,
    View,
    Sequence,
    Function,
    Type,
    Domain,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
        table: String,
        column: Column,
    },
    DropColumn {
        table: String,
        column: String,
    },
    AlterColumn {
        table: String,
        column: String,
        changes: ColumnChanges,
    },
    AddPrimaryKey {
        table: String,
        primary_key: PrimaryKey,
    },
    DropPrimaryKey {
        table: String,
    },
    AddIndex {
        table: String,
        index: Index,
    },
    DropIndex {
        table: String,
        index_name: String,
    },
    AddForeignKey {
        table: String,
        foreign_key: ForeignKey,
    },
    DropForeignKey {
        table: String,
        foreign_key_name: String,
    },
    AddCheckConstraint {
        table: String,
        check_constraint: CheckConstraint,
    },
    DropCheckConstraint {
        table: String,
        constraint_name: String,
    },
    EnableRls {
        table: String,
    },
    DisableRls {
        table: String,
    },
    CreatePolicy(Policy),
    DropPolicy {
        table: String,
        name: String,
    },
    AlterPolicy {
        table: String,
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
        table: String,
        column: String,
        hint: String,
    },
    SetColumnNotNull {
        table: String,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnChanges {
    pub data_type: Option<PgType>,
    pub nullable: Option<bool>,
    pub default: Option<Option<String>>,
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

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DomainChanges {
    pub default: Option<Option<String>>,
    pub not_null: Option<bool>,
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
