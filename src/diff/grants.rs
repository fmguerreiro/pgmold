use std::collections::{BTreeMap, HashSet};

use crate::model::{DefaultPrivilege, Grant, Privilege, Schema};

use super::{GrantObjectKind, MigrationOp};

pub(super) fn diff_grants_for_object(
    from_grants: &[Grant],
    to_grants: &[Grant],
    object_kind: GrantObjectKind,
    schema: &str,
    name: &str,
    args: Option<String>,
    excluded_grant_roles: &HashSet<String>,
) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    let from_by_grantee: BTreeMap<&str, &Grant> = from_grants
        .iter()
        .filter(|g| !excluded_grant_roles.contains(&g.grantee.to_lowercase()))
        .map(|g| (g.grantee.as_str(), g))
        .collect();
    let to_by_grantee: BTreeMap<&str, &Grant> = to_grants
        .iter()
        .filter(|g| !excluded_grant_roles.contains(&g.grantee.to_lowercase()))
        .map(|g| (g.grantee.as_str(), g))
        .collect();

    for (grantee, from_grant) in &from_by_grantee {
        match to_by_grantee.get(grantee) {
            Some(to_grant) => {
                let privs_to_revoke: Vec<Privilege> = from_grant
                    .privileges
                    .difference(&to_grant.privileges)
                    .cloned()
                    .collect();
                if !privs_to_revoke.is_empty() {
                    ops.push(MigrationOp::RevokePrivileges {
                        object_kind: object_kind.clone(),
                        schema: schema.to_string(),
                        name: name.to_string(),
                        args: args.clone(),
                        grantee: grantee.to_string(),
                        privileges: privs_to_revoke,
                        revoke_grant_option: false,
                    });
                }

                let privs_to_grant: Vec<Privilege> = to_grant
                    .privileges
                    .difference(&from_grant.privileges)
                    .cloned()
                    .collect();
                if !privs_to_grant.is_empty() {
                    ops.push(MigrationOp::GrantPrivileges {
                        object_kind: object_kind.clone(),
                        schema: schema.to_string(),
                        name: name.to_string(),
                        args: args.clone(),
                        grantee: grantee.to_string(),
                        privileges: privs_to_grant,
                        with_grant_option: to_grant.with_grant_option,
                    });
                }

                if from_grant.with_grant_option && !to_grant.with_grant_option {
                    let common_privs: Vec<Privilege> = from_grant
                        .privileges
                        .intersection(&to_grant.privileges)
                        .cloned()
                        .collect();
                    if !common_privs.is_empty() {
                        ops.push(MigrationOp::RevokePrivileges {
                            object_kind: object_kind.clone(),
                            schema: schema.to_string(),
                            name: name.to_string(),
                            args: args.clone(),
                            grantee: grantee.to_string(),
                            privileges: common_privs,
                            revoke_grant_option: true,
                        });
                    }
                } else if !from_grant.with_grant_option && to_grant.with_grant_option {
                    let common_privs: Vec<Privilege> = from_grant
                        .privileges
                        .intersection(&to_grant.privileges)
                        .cloned()
                        .collect();
                    if !common_privs.is_empty() {
                        ops.push(MigrationOp::GrantPrivileges {
                            object_kind: object_kind.clone(),
                            schema: schema.to_string(),
                            name: name.to_string(),
                            args: args.clone(),
                            grantee: grantee.to_string(),
                            privileges: common_privs,
                            with_grant_option: true,
                        });
                    }
                }
            }
            None => {
                let privs: Vec<Privilege> = from_grant.privileges.iter().cloned().collect();
                if !privs.is_empty() {
                    ops.push(MigrationOp::RevokePrivileges {
                        object_kind: object_kind.clone(),
                        schema: schema.to_string(),
                        name: name.to_string(),
                        args: args.clone(),
                        grantee: grantee.to_string(),
                        privileges: privs,
                        revoke_grant_option: false,
                    });
                }
            }
        }
    }

    for (grantee, to_grant) in &to_by_grantee {
        if !from_by_grantee.contains_key(grantee) {
            let privs: Vec<Privilege> = to_grant.privileges.iter().cloned().collect();
            if !privs.is_empty() {
                ops.push(MigrationOp::GrantPrivileges {
                    object_kind: object_kind.clone(),
                    schema: schema.to_string(),
                    name: name.to_string(),
                    args: args.clone(),
                    grantee: grantee.to_string(),
                    privileges: privs,
                    with_grant_option: to_grant.with_grant_option,
                });
            }
        }
    }

    ops
}

pub(super) fn create_grants_for_new_object(
    grants: &[Grant],
    object_kind: GrantObjectKind,
    schema: &str,
    name: &str,
    args: Option<String>,
    excluded_grant_roles: &HashSet<String>,
) -> Vec<MigrationOp> {
    grants
        .iter()
        .filter(|grant| !excluded_grant_roles.contains(&grant.grantee.to_lowercase()))
        .filter_map(|grant| {
            let privs: Vec<Privilege> = grant.privileges.iter().cloned().collect();
            if privs.is_empty() {
                return None;
            }
            Some(MigrationOp::GrantPrivileges {
                object_kind: object_kind.clone(),
                schema: schema.to_string(),
                name: name.to_string(),
                args: args.clone(),
                grantee: grant.grantee.clone(),
                privileges: privs,
                with_grant_option: grant.with_grant_option,
            })
        })
        .collect()
}

pub(super) fn diff_default_privileges(from: &Schema, to: &Schema) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    type DpKey = (String, Option<String>, String, String);

    fn dp_key(dp: &DefaultPrivilege) -> DpKey {
        (
            dp.target_role.clone(),
            dp.schema.clone(),
            dp.object_type.as_sql_str().to_string(),
            dp.grantee.clone(),
        )
    }

    let from_map: BTreeMap<DpKey, &DefaultPrivilege> = from
        .default_privileges
        .iter()
        .map(|dp| (dp_key(dp), dp))
        .collect();
    let to_map: BTreeMap<DpKey, &DefaultPrivilege> = to
        .default_privileges
        .iter()
        .map(|dp| (dp_key(dp), dp))
        .collect();

    for (key, from_dp) in &from_map {
        if !to_map.contains_key(key) {
            let privs: Vec<Privilege> = from_dp.privileges.iter().cloned().collect();
            if !privs.is_empty() {
                ops.push(MigrationOp::AlterDefaultPrivileges {
                    target_role: from_dp.target_role.clone(),
                    schema: from_dp.schema.clone(),
                    object_type: from_dp.object_type.clone(),
                    grantee: from_dp.grantee.clone(),
                    privileges: privs,
                    with_grant_option: from_dp.with_grant_option,
                    revoke: true,
                });
            }
        }
    }

    for (key, to_dp) in &to_map {
        if !from_map.contains_key(key) {
            let privs: Vec<Privilege> = to_dp.privileges.iter().cloned().collect();
            if !privs.is_empty() {
                ops.push(MigrationOp::AlterDefaultPrivileges {
                    target_role: to_dp.target_role.clone(),
                    schema: to_dp.schema.clone(),
                    object_type: to_dp.object_type.clone(),
                    grantee: to_dp.grantee.clone(),
                    privileges: privs,
                    with_grant_option: to_dp.with_grant_option,
                    revoke: false,
                });
            }
        }
    }

    for (key, to_dp) in &to_map {
        if let Some(from_dp) = from_map.get(key) {
            let privs_to_revoke: Vec<Privilege> = from_dp
                .privileges
                .difference(&to_dp.privileges)
                .cloned()
                .collect();
            let privs_to_grant: Vec<Privilege> = to_dp
                .privileges
                .difference(&from_dp.privileges)
                .cloned()
                .collect();

            if !privs_to_revoke.is_empty() {
                ops.push(MigrationOp::AlterDefaultPrivileges {
                    target_role: from_dp.target_role.clone(),
                    schema: from_dp.schema.clone(),
                    object_type: from_dp.object_type.clone(),
                    grantee: from_dp.grantee.clone(),
                    privileges: privs_to_revoke,
                    with_grant_option: from_dp.with_grant_option,
                    revoke: true,
                });
            }

            if !privs_to_grant.is_empty() {
                ops.push(MigrationOp::AlterDefaultPrivileges {
                    target_role: to_dp.target_role.clone(),
                    schema: to_dp.schema.clone(),
                    object_type: to_dp.object_type.clone(),
                    grantee: to_dp.grantee.clone(),
                    privileges: privs_to_grant,
                    with_grant_option: to_dp.with_grant_option,
                    revoke: false,
                });
            }

            if from_dp.with_grant_option != to_dp.with_grant_option {
                let common_privs: Vec<Privilege> = from_dp
                    .privileges
                    .intersection(&to_dp.privileges)
                    .cloned()
                    .collect();
                if !common_privs.is_empty() {
                    ops.push(MigrationOp::AlterDefaultPrivileges {
                        target_role: from_dp.target_role.clone(),
                        schema: from_dp.schema.clone(),
                        object_type: from_dp.object_type.clone(),
                        grantee: from_dp.grantee.clone(),
                        privileges: common_privs.clone(),
                        with_grant_option: from_dp.with_grant_option,
                        revoke: true,
                    });
                    ops.push(MigrationOp::AlterDefaultPrivileges {
                        target_role: to_dp.target_role.clone(),
                        schema: to_dp.schema.clone(),
                        object_type: to_dp.object_type.clone(),
                        grantee: to_dp.grantee.clone(),
                        privileges: common_privs,
                        with_grant_option: to_dp.with_grant_option,
                        revoke: false,
                    });
                }
            }
        }
    }

    ops
}
