use std::collections::{BTreeMap, BTreeSet, HashSet};

use crate::model::{DefaultPrivilege, Grant, Privilege, Schema};

use super::{GrantObjectKind, MigrationOp};

fn nonempty_privileges(set: &BTreeSet<Privilege>) -> Option<Vec<Privilege>> {
    if set.is_empty() {
        None
    } else {
        Some(set.iter().cloned().collect())
    }
}

struct GrantOptionChange {
    revoke_grant_option: Option<Vec<Privilege>>,
    regrant_with_option: Option<Vec<Privilege>>,
}

fn compute_grant_option_changes(
    from_privileges: &BTreeSet<Privilege>,
    to_privileges: &BTreeSet<Privilege>,
    from_with_grant_option: bool,
    to_with_grant_option: bool,
) -> GrantOptionChange {
    if from_with_grant_option == to_with_grant_option {
        return GrantOptionChange {
            revoke_grant_option: None,
            regrant_with_option: None,
        };
    }
    let common_privs: Vec<Privilege> = from_privileges
        .intersection(to_privileges)
        .cloned()
        .collect();
    if common_privs.is_empty() {
        return GrantOptionChange {
            revoke_grant_option: None,
            regrant_with_option: None,
        };
    }
    if from_with_grant_option && !to_with_grant_option {
        GrantOptionChange {
            revoke_grant_option: Some(common_privs),
            regrant_with_option: None,
        }
    } else {
        GrantOptionChange {
            revoke_grant_option: None,
            regrant_with_option: Some(common_privs),
        }
    }
}

pub(super) fn diff_grants_for_object(
    from_grants: &[Grant],
    to_grants: &[Grant],
    object_kind: GrantObjectKind,
    schema: &str,
    name: &str,
    args: Option<&str>,
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

    let args_owned = args.map(str::to_string);
    let revoke = |grantee: &str, privileges: Vec<Privilege>, revoke_grant_option: bool| {
        MigrationOp::RevokePrivileges {
            object_kind,
            schema: schema.to_string(),
            name: name.to_string(),
            args: args_owned.clone(),
            grantee: grantee.to_string(),
            privileges,
            revoke_grant_option,
        }
    };
    let grant = |grantee: &str, privileges: Vec<Privilege>, with_grant_option: bool| {
        MigrationOp::GrantPrivileges {
            object_kind,
            schema: schema.to_string(),
            name: name.to_string(),
            args: args_owned.clone(),
            grantee: grantee.to_string(),
            privileges,
            with_grant_option,
        }
    };

    for (grantee, from_grant) in &from_by_grantee {
        match to_by_grantee.get(grantee) {
            Some(to_grant) => {
                let privs_to_revoke: Vec<Privilege> = from_grant
                    .privileges
                    .difference(&to_grant.privileges)
                    .cloned()
                    .collect();
                if !privs_to_revoke.is_empty() {
                    ops.push(revoke(grantee, privs_to_revoke, false));
                }

                let privs_to_grant: Vec<Privilege> = to_grant
                    .privileges
                    .difference(&from_grant.privileges)
                    .cloned()
                    .collect();
                if !privs_to_grant.is_empty() {
                    ops.push(grant(grantee, privs_to_grant, to_grant.with_grant_option));
                }

                let grant_option_change = compute_grant_option_changes(
                    &from_grant.privileges,
                    &to_grant.privileges,
                    from_grant.with_grant_option,
                    to_grant.with_grant_option,
                );
                if let Some(privs) = grant_option_change.revoke_grant_option {
                    ops.push(revoke(grantee, privs, true));
                }
                if let Some(privs) = grant_option_change.regrant_with_option {
                    ops.push(grant(grantee, privs, true));
                }
            }
            None => {
                if let Some(privs) = nonempty_privileges(&from_grant.privileges) {
                    ops.push(revoke(grantee, privs, false));
                }
            }
        }
    }

    for (grantee, to_grant) in &to_by_grantee {
        if !from_by_grantee.contains_key(grantee) {
            if let Some(privs) = nonempty_privileges(&to_grant.privileges) {
                ops.push(grant(grantee, privs, to_grant.with_grant_option));
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
    args: Option<&str>,
    excluded_grant_roles: &HashSet<String>,
) -> Vec<MigrationOp> {
    let args_owned = args.map(str::to_string);
    grants
        .iter()
        .filter(|grant| !excluded_grant_roles.contains(&grant.grantee.to_lowercase()))
        .filter_map(|grant| {
            let privs = nonempty_privileges(&grant.privileges)?;
            Some(MigrationOp::GrantPrivileges {
                object_kind,
                schema: schema.to_string(),
                name: name.to_string(),
                args: args_owned.clone(),
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

    let emit_dp = |dp: &DefaultPrivilege, privileges: Vec<Privilege>, revoke: bool| {
        MigrationOp::AlterDefaultPrivileges {
            target_role: dp.target_role.clone(),
            schema: dp.schema.clone(),
            object_type: dp.object_type.clone(),
            grantee: dp.grantee.clone(),
            privileges,
            with_grant_option: dp.with_grant_option,
            revoke,
        }
    };

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
            if let Some(privs) = nonempty_privileges(&from_dp.privileges) {
                ops.push(emit_dp(from_dp, privs, true));
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
                ops.push(emit_dp(from_dp, privs_to_revoke, true));
            }

            if !privs_to_grant.is_empty() {
                ops.push(emit_dp(to_dp, privs_to_grant, false));
            }

            if from_dp.with_grant_option != to_dp.with_grant_option {
                let common_privs: Vec<Privilege> = from_dp
                    .privileges
                    .intersection(&to_dp.privileges)
                    .cloned()
                    .collect();
                if !common_privs.is_empty() {
                    ops.push(emit_dp(from_dp, common_privs.clone(), true));
                    ops.push(emit_dp(to_dp, common_privs, false));
                }
            }
        } else if let Some(privs) = nonempty_privileges(&to_dp.privileges) {
            ops.push(emit_dp(to_dp, privs, false));
        }
    }

    ops
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashSet};

    use crate::diff::GrantObjectKind;
    use crate::model::{Grant, Privilege};

    use super::diff_grants_for_object;

    #[test]
    fn owner_grants_in_db_cause_spurious_revoke() {
        let owner_grant = Grant {
            grantee: "postgres".to_string(),
            privileges: BTreeSet::from([
                Privilege::Select,
                Privilege::Insert,
                Privilege::Update,
                Privilege::Delete,
            ]),
            with_grant_option: false,
        };
        let app_grant = Grant {
            grantee: "app_user".to_string(),
            privileges: BTreeSet::from([Privilege::Select]),
            with_grant_option: false,
        };

        let from_grants = vec![owner_grant, app_grant.clone()];
        let to_grants = vec![app_grant];

        let ops = diff_grants_for_object(
            &from_grants,
            &to_grants,
            GrantObjectKind::Table,
            "public",
            "users",
            None,
            &HashSet::new(),
        );

        assert!(
            !ops.is_empty(),
            "Without owner filtering, spurious REVOKE ops should be generated"
        );
    }

    #[test]
    fn excluded_owner_role_prevents_spurious_revoke() {
        let owner_grant = Grant {
            grantee: "postgres".to_string(),
            privileges: BTreeSet::from([
                Privilege::Select,
                Privilege::Insert,
                Privilege::Update,
                Privilege::Delete,
            ]),
            with_grant_option: false,
        };
        let app_grant = Grant {
            grantee: "app_user".to_string(),
            privileges: BTreeSet::from([Privilege::Select]),
            with_grant_option: false,
        };

        let from_grants = vec![owner_grant, app_grant.clone()];
        let to_grants = vec![app_grant];

        let excluded = HashSet::from(["postgres".to_string()]);
        let ops = diff_grants_for_object(
            &from_grants,
            &to_grants,
            GrantObjectKind::Table,
            "public",
            "users",
            None,
            &excluded,
        );

        assert!(
            ops.is_empty(),
            "Owner role should be filtered out, leaving no diff"
        );
    }
}
