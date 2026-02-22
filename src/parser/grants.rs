use crate::model::*;
use crate::util::Result;
use regex::Regex;
use std::collections::BTreeSet;

fn parse_privileges(privileges_str: &str, object_type: Option<&str>) -> BTreeSet<Privilege> {
    let mut privileges = BTreeSet::new();
    for priv_str in privileges_str.split(',') {
        let priv_trimmed = priv_str.trim().to_uppercase();
        match priv_trimmed.as_str() {
            "SELECT" => {
                privileges.insert(Privilege::Select);
            }
            "INSERT" => {
                privileges.insert(Privilege::Insert);
            }
            "UPDATE" => {
                privileges.insert(Privilege::Update);
            }
            "DELETE" => {
                privileges.insert(Privilege::Delete);
            }
            "TRUNCATE" => {
                privileges.insert(Privilege::Truncate);
            }
            "REFERENCES" => {
                privileges.insert(Privilege::References);
            }
            "TRIGGER" => {
                privileges.insert(Privilege::Trigger);
            }
            "USAGE" => {
                privileges.insert(Privilege::Usage);
            }
            "EXECUTE" => {
                privileges.insert(Privilege::Execute);
            }
            "CREATE" => {
                privileges.insert(Privilege::Create);
            }
            "ALL" | "ALL PRIVILEGES" => match object_type {
                Some("SCHEMA") => {
                    privileges.insert(Privilege::Usage);
                    privileges.insert(Privilege::Create);
                }
                Some("SEQUENCE") => {
                    privileges.insert(Privilege::Usage);
                    privileges.insert(Privilege::Select);
                    privileges.insert(Privilege::Update);
                }
                Some("FUNCTION") => {
                    privileges.insert(Privilege::Execute);
                }
                Some("TYPE") => {
                    privileges.insert(Privilege::Usage);
                }
                _ => {
                    privileges.insert(Privilege::Select);
                    privileges.insert(Privilege::Insert);
                    privileges.insert(Privilege::Update);
                    privileges.insert(Privilege::Delete);
                    privileges.insert(Privilege::Truncate);
                    privileges.insert(Privilege::References);
                    privileges.insert(Privilege::Trigger);
                }
            },
            _ => continue,
        }
    }
    privileges
}

fn parse_grant_all_in_schema(sql: &str, schema: &mut Schema) {
    let re = Regex::new(
        r#"(?i)GRANT\s+(.+?)\s+ON\s+ALL\s+(TABLES|SEQUENCES|FUNCTIONS)\s+IN\s+SCHEMA\s+("[^"]+"|\w+)\s+TO\s+("[^"]+"|\w+|PUBLIC)\s*(WITH\s+GRANT\s+OPTION)?\s*;"#
    ).unwrap();

    for cap in re.captures_iter(sql) {
        let privileges_str = cap.get(1).unwrap().as_str();
        let object_kind = cap.get(2).unwrap().as_str().to_uppercase();
        let schema_name = cap.get(3).unwrap().as_str().trim_matches('"');
        let grantee = cap.get(4).unwrap().as_str().trim_matches('"');
        let with_grant_option = cap.get(5).is_some();

        let inferred_type = match object_kind.as_str() {
            "TABLES" => "TABLE",
            "SEQUENCES" => "SEQUENCE",
            "FUNCTIONS" => "FUNCTION",
            _ => continue,
        };

        let privileges = parse_privileges(privileges_str, Some(inferred_type));
        if privileges.is_empty() {
            continue;
        }

        let grant = Grant {
            grantee: grantee.to_string(),
            privileges,
            with_grant_option,
        };

        match object_kind.as_str() {
            "TABLES" => {
                let keys: Vec<String> = schema
                    .tables
                    .iter()
                    .filter(|(_, t)| t.schema == schema_name)
                    .map(|(k, _)| k.clone())
                    .collect();
                for key in keys {
                    if let Some(table) = schema.tables.get_mut(&key) {
                        table.grants.push(grant.clone());
                    }
                }
                let view_keys: Vec<String> = schema
                    .views
                    .iter()
                    .filter(|(_, v)| v.schema == schema_name)
                    .map(|(k, _)| k.clone())
                    .collect();
                for key in view_keys {
                    if let Some(view) = schema.views.get_mut(&key) {
                        view.grants.push(grant.clone());
                    }
                }
            }
            "SEQUENCES" => {
                let keys: Vec<String> = schema
                    .sequences
                    .iter()
                    .filter(|(_, s)| s.schema == schema_name)
                    .map(|(k, _)| k.clone())
                    .collect();
                for key in keys {
                    if let Some(seq) = schema.sequences.get_mut(&key) {
                        seq.grants.push(grant.clone());
                    }
                }
            }
            "FUNCTIONS" => {
                let keys: Vec<String> = schema
                    .functions
                    .iter()
                    .filter(|(_, f)| f.schema == schema_name)
                    .map(|(k, _)| k.clone())
                    .collect();
                for key in keys {
                    if let Some(func) = schema.functions.get_mut(&key) {
                        func.grants.push(grant.clone());
                    }
                }
            }
            _ => {}
        }
    }
}

pub(super) fn parse_grant_statements(sql: &str, schema: &mut Schema) -> Result<()> {
    parse_grant_all_in_schema(sql, schema);

    let grant_re = Regex::new(
        r#"(?i)GRANT\s+(.+?)\s+ON\s+(?:(TABLE|VIEW|SEQUENCE|FUNCTION|SCHEMA|TYPE)\s+)?(.+?)\s+TO\s+("[^"]+"|\w+|PUBLIC)\s*(WITH\s+GRANT\s+OPTION)?"#
    ).unwrap();

    for cap in grant_re.captures_iter(sql) {
        let privileges_str = cap.get(1).unwrap().as_str();
        let object_type = cap.get(2).map(|m| m.as_str().to_uppercase());
        let object_name_raw = cap.get(3).unwrap().as_str();
        let grantee = cap.get(4).unwrap().as_str().trim_matches('"');
        let with_grant_option = cap.get(5).is_some();

        if object_name_raw.to_uppercase().starts_with("ALL ") {
            continue;
        }

        let inferred_type = object_type.as_deref().unwrap_or("TABLE");
        let privileges = parse_privileges(privileges_str, Some(inferred_type));
        if privileges.is_empty() {
            continue;
        }

        let grant = Grant {
            grantee: grantee.to_string(),
            privileges,
            with_grant_option,
        };
        match inferred_type {
            "TABLE" | "VIEW" => {
                let (obj_schema, obj_name) = parse_object_name(object_name_raw);
                let key = qualified_name(&obj_schema, &obj_name);

                if let Some(table) = schema.tables.get_mut(&key) {
                    table.grants.push(grant);
                } else if let Some(view) = schema.views.get_mut(&key) {
                    view.grants.push(grant);
                } else {
                    let pending_type = if inferred_type == "VIEW" {
                        PendingGrantObjectType::View
                    } else {
                        PendingGrantObjectType::Table
                    };
                    schema.pending_grants.push(PendingGrant {
                        object_type: pending_type,
                        object_key: key,
                        grant,
                    });
                }
            }
            "SEQUENCE" => {
                let (obj_schema, obj_name) = parse_object_name(object_name_raw);
                let key = qualified_name(&obj_schema, &obj_name);
                if let Some(sequence) = schema.sequences.get_mut(&key) {
                    sequence.grants.push(grant);
                } else {
                    schema.pending_grants.push(PendingGrant {
                        object_type: PendingGrantObjectType::Sequence,
                        object_key: key,
                        grant,
                    });
                }
            }
            "FUNCTION" => {
                let function_key = parse_function_signature(object_name_raw);
                if let Some(func) = schema.functions.get_mut(&function_key) {
                    func.grants.push(grant);
                } else {
                    schema.pending_grants.push(PendingGrant {
                        object_type: PendingGrantObjectType::Function,
                        object_key: function_key,
                        grant,
                    });
                }
            }
            "SCHEMA" => {
                let schema_name = object_name_raw.trim().trim_matches('"');
                if let Some(pg_schema) = schema.schemas.get_mut(schema_name) {
                    pg_schema.grants.push(grant);
                } else {
                    schema.pending_grants.push(PendingGrant {
                        object_type: PendingGrantObjectType::Schema,
                        object_key: schema_name.to_string(),
                        grant,
                    });
                }
            }
            "TYPE" => {
                let (obj_schema, obj_name) = parse_object_name(object_name_raw);
                let key = qualified_name(&obj_schema, &obj_name);

                if let Some(enum_type) = schema.enums.get_mut(&key) {
                    enum_type.grants.push(grant);
                } else if let Some(domain) = schema.domains.get_mut(&key) {
                    domain.grants.push(grant);
                } else {
                    schema.pending_grants.push(PendingGrant {
                        object_type: PendingGrantObjectType::Enum,
                        object_key: key,
                        grant,
                    });
                }
            }
            _ => {}
        }
    }

    Ok(())
}

pub(super) fn parse_revoke_statements(sql: &str, schema: &mut Schema) -> Result<()> {
    let revoke_re = Regex::new(
        r#"(?i)REVOKE\s+(GRANT\s+OPTION\s+FOR\s+)?(.+?)\s+ON\s+(?:(TABLE|VIEW|SEQUENCE|FUNCTION|SCHEMA|TYPE)\s+)?(.+?)\s+FROM\s+("[^"]+"|\w+|PUBLIC)\s*;"#
    ).unwrap();

    for cap in revoke_re.captures_iter(sql) {
        let grant_option_for = cap.get(1).is_some();
        let privileges_str = cap.get(2).unwrap().as_str();
        let object_type = cap.get(3).map(|m| m.as_str().to_uppercase());
        let object_name_raw = cap.get(4).unwrap().as_str();
        let grantee = cap.get(5).unwrap().as_str().trim_matches('"');

        if object_name_raw.to_uppercase().starts_with("ALL ") {
            continue;
        }

        let inferred_type = object_type.as_deref().unwrap_or("TABLE");
        let privileges = parse_privileges(privileges_str, Some(inferred_type));
        if privileges.is_empty() {
            continue;
        }
        match inferred_type {
            "TABLE" | "VIEW" => {
                let (obj_schema, obj_name) = parse_object_name(object_name_raw);
                let key = qualified_name(&obj_schema, &obj_name);

                if let Some(table) = schema.tables.get_mut(&key) {
                    revoke_from_grants(&mut table.grants, grantee, &privileges, grant_option_for);
                } else if let Some(view) = schema.views.get_mut(&key) {
                    revoke_from_grants(&mut view.grants, grantee, &privileges, grant_option_for);
                } else {
                    let pending_type = if inferred_type == "VIEW" {
                        PendingGrantObjectType::View
                    } else {
                        PendingGrantObjectType::Table
                    };
                    schema.pending_revokes.push(PendingRevoke {
                        object_type: pending_type,
                        object_key: key,
                        grantee: grantee.to_string(),
                        privileges,
                        grant_option_for,
                    });
                }
            }
            "SEQUENCE" => {
                let (obj_schema, obj_name) = parse_object_name(object_name_raw);
                let key = qualified_name(&obj_schema, &obj_name);
                if let Some(sequence) = schema.sequences.get_mut(&key) {
                    revoke_from_grants(
                        &mut sequence.grants,
                        grantee,
                        &privileges,
                        grant_option_for,
                    );
                } else {
                    schema.pending_revokes.push(PendingRevoke {
                        object_type: PendingGrantObjectType::Sequence,
                        object_key: key,
                        grantee: grantee.to_string(),
                        privileges,
                        grant_option_for,
                    });
                }
            }
            "FUNCTION" => {
                let function_key = parse_function_signature(object_name_raw);
                if let Some(func) = schema.functions.get_mut(&function_key) {
                    revoke_from_grants(&mut func.grants, grantee, &privileges, grant_option_for);
                } else {
                    schema.pending_revokes.push(PendingRevoke {
                        object_type: PendingGrantObjectType::Function,
                        object_key: function_key,
                        grantee: grantee.to_string(),
                        privileges,
                        grant_option_for,
                    });
                }
            }
            "SCHEMA" => {
                let schema_name = object_name_raw.trim().trim_matches('"');
                if let Some(pg_schema) = schema.schemas.get_mut(schema_name) {
                    revoke_from_grants(
                        &mut pg_schema.grants,
                        grantee,
                        &privileges,
                        grant_option_for,
                    );
                } else {
                    schema.pending_revokes.push(PendingRevoke {
                        object_type: PendingGrantObjectType::Schema,
                        object_key: schema_name.to_string(),
                        grantee: grantee.to_string(),
                        privileges,
                        grant_option_for,
                    });
                }
            }
            "TYPE" => {
                let (obj_schema, obj_name) = parse_object_name(object_name_raw);
                let key = qualified_name(&obj_schema, &obj_name);

                if let Some(enum_type) = schema.enums.get_mut(&key) {
                    revoke_from_grants(
                        &mut enum_type.grants,
                        grantee,
                        &privileges,
                        grant_option_for,
                    );
                } else if let Some(domain) = schema.domains.get_mut(&key) {
                    revoke_from_grants(&mut domain.grants, grantee, &privileges, grant_option_for);
                } else {
                    schema.pending_revokes.push(PendingRevoke {
                        object_type: PendingGrantObjectType::Enum,
                        object_key: key,
                        grantee: grantee.to_string(),
                        privileges,
                        grant_option_for,
                    });
                }
            }
            _ => {}
        }
    }

    Ok(())
}

pub(super) fn parse_alter_default_privileges(sql: &str, schema: &mut Schema) -> Result<()> {
    let grant_re = Regex::new(
        r"(?is)ALTER\s+DEFAULT\s+PRIVILEGES\s+(?:FOR\s+ROLE\s+(\w+)\s+)?(?:IN\s+SCHEMA\s+(\w+)\s+)?GRANT\s+(.+?)\s+ON\s+(TABLES|SEQUENCES|FUNCTIONS|ROUTINES|TYPES|SCHEMAS)\s+TO\s+(\w+|PUBLIC)\s*(WITH\s+GRANT\s+OPTION)?\s*;"
    ).unwrap();

    for cap in grant_re.captures_iter(sql) {
        let target_role = cap
            .get(1)
            .map(|m| m.as_str().to_string())
            .unwrap_or_else(|| "CURRENT_ROLE".to_string());
        let schema_scope = cap.get(2).map(|m| m.as_str().to_string());
        let privileges_str = cap.get(3).unwrap().as_str();
        let object_type_str = cap.get(4).unwrap().as_str().to_uppercase();
        let grantee = cap.get(5).unwrap().as_str().to_string();
        let with_grant_option = cap.get(6).is_some();

        let object_type = match DefaultPrivilegeObjectType::from_sql_str(&object_type_str) {
            Some(ot) => ot,
            None => continue,
        };

        let mut privileges = BTreeSet::new();
        let priv_upper = privileges_str.to_uppercase();
        if priv_upper.contains("ALL") {
            match object_type {
                DefaultPrivilegeObjectType::Tables => {
                    privileges.insert(Privilege::Select);
                    privileges.insert(Privilege::Insert);
                    privileges.insert(Privilege::Update);
                    privileges.insert(Privilege::Delete);
                    privileges.insert(Privilege::Truncate);
                    privileges.insert(Privilege::References);
                    privileges.insert(Privilege::Trigger);
                }
                DefaultPrivilegeObjectType::Sequences => {
                    privileges.insert(Privilege::Usage);
                    privileges.insert(Privilege::Select);
                    privileges.insert(Privilege::Update);
                }
                DefaultPrivilegeObjectType::Functions | DefaultPrivilegeObjectType::Routines => {
                    privileges.insert(Privilege::Execute);
                }
                DefaultPrivilegeObjectType::Types => {
                    privileges.insert(Privilege::Usage);
                }
                DefaultPrivilegeObjectType::Schemas => {
                    privileges.insert(Privilege::Usage);
                    privileges.insert(Privilege::Create);
                }
            }
        } else {
            for priv_str in privileges_str.split(',') {
                let priv_trimmed = priv_str.trim().to_uppercase();
                match priv_trimmed.as_str() {
                    "SELECT" => privileges.insert(Privilege::Select),
                    "INSERT" => privileges.insert(Privilege::Insert),
                    "UPDATE" => privileges.insert(Privilege::Update),
                    "DELETE" => privileges.insert(Privilege::Delete),
                    "TRUNCATE" => privileges.insert(Privilege::Truncate),
                    "REFERENCES" => privileges.insert(Privilege::References),
                    "TRIGGER" => privileges.insert(Privilege::Trigger),
                    "USAGE" => privileges.insert(Privilege::Usage),
                    "EXECUTE" => privileges.insert(Privilege::Execute),
                    "CREATE" => privileges.insert(Privilege::Create),
                    _ => continue,
                };
            }
        }

        if privileges.is_empty() {
            continue;
        }

        schema.default_privileges.push(DefaultPrivilege {
            target_role,
            schema: schema_scope,
            object_type,
            grantee,
            privileges,
            with_grant_option,
        });
    }

    let revoke_re = Regex::new(
        r"(?is)ALTER\s+DEFAULT\s+PRIVILEGES\s+(?:FOR\s+ROLE\s+(\w+)\s+)?(?:IN\s+SCHEMA\s+(\w+)\s+)?REVOKE\s+(.+?)\s+ON\s+(TABLES|SEQUENCES|FUNCTIONS|ROUTINES|TYPES|SCHEMAS)\s+FROM\s+(\w+|PUBLIC)\s*;"
    ).unwrap();

    for cap in revoke_re.captures_iter(sql) {
        let target_role = cap
            .get(1)
            .map(|m| m.as_str().to_string())
            .unwrap_or_else(|| "CURRENT_ROLE".to_string());
        let schema_scope = cap.get(2).map(|m| m.as_str().to_string());
        let privileges_str = cap.get(3).unwrap().as_str();
        let object_type_str = cap.get(4).unwrap().as_str().to_uppercase();
        let grantee = cap.get(5).unwrap().as_str().to_string();

        let object_type = match DefaultPrivilegeObjectType::from_sql_str(&object_type_str) {
            Some(ot) => ot,
            None => continue,
        };

        let mut privs_to_revoke = BTreeSet::new();
        let priv_upper = privileges_str.to_uppercase();
        if priv_upper.contains("ALL") {
            match object_type {
                DefaultPrivilegeObjectType::Tables => {
                    privs_to_revoke.insert(Privilege::Select);
                    privs_to_revoke.insert(Privilege::Insert);
                    privs_to_revoke.insert(Privilege::Update);
                    privs_to_revoke.insert(Privilege::Delete);
                    privs_to_revoke.insert(Privilege::Truncate);
                    privs_to_revoke.insert(Privilege::References);
                    privs_to_revoke.insert(Privilege::Trigger);
                }
                DefaultPrivilegeObjectType::Sequences => {
                    privs_to_revoke.insert(Privilege::Usage);
                    privs_to_revoke.insert(Privilege::Select);
                    privs_to_revoke.insert(Privilege::Update);
                }
                DefaultPrivilegeObjectType::Functions | DefaultPrivilegeObjectType::Routines => {
                    privs_to_revoke.insert(Privilege::Execute);
                }
                DefaultPrivilegeObjectType::Types => {
                    privs_to_revoke.insert(Privilege::Usage);
                }
                DefaultPrivilegeObjectType::Schemas => {
                    privs_to_revoke.insert(Privilege::Usage);
                    privs_to_revoke.insert(Privilege::Create);
                }
            }
        } else {
            for priv_str in privileges_str.split(',') {
                let priv_trimmed = priv_str.trim().to_uppercase();
                match priv_trimmed.as_str() {
                    "SELECT" => privs_to_revoke.insert(Privilege::Select),
                    "INSERT" => privs_to_revoke.insert(Privilege::Insert),
                    "UPDATE" => privs_to_revoke.insert(Privilege::Update),
                    "DELETE" => privs_to_revoke.insert(Privilege::Delete),
                    "TRUNCATE" => privs_to_revoke.insert(Privilege::Truncate),
                    "REFERENCES" => privs_to_revoke.insert(Privilege::References),
                    "TRIGGER" => privs_to_revoke.insert(Privilege::Trigger),
                    "USAGE" => privs_to_revoke.insert(Privilege::Usage),
                    "EXECUTE" => privs_to_revoke.insert(Privilege::Execute),
                    "CREATE" => privs_to_revoke.insert(Privilege::Create),
                    _ => continue,
                };
            }
        }

        for dp in &mut schema.default_privileges {
            if dp.target_role == target_role
                && dp.schema == schema_scope
                && dp.object_type == object_type
                && dp.grantee == grantee
            {
                for privilege in &privs_to_revoke {
                    dp.privileges.remove(privilege);
                }
            }
        }

        schema
            .default_privileges
            .retain(|dp| !dp.privileges.is_empty());
    }

    Ok(())
}

fn parse_object_name(name: &str) -> (String, String) {
    let trimmed = name.trim().trim_matches('"');
    match trimmed.split_once('.') {
        Some((schema, obj)) => (
            schema.trim_matches('"').to_string(),
            obj.trim_matches('"').to_string(),
        ),
        None => ("public".to_string(), trimmed.to_string()),
    }
}

fn parse_function_signature(sig: &str) -> String {
    let trimmed = sig.trim();

    if let Some(paren_pos) = trimmed.find('(') {
        let before_paren = &trimmed[..paren_pos];
        let args_part = &trimmed[paren_pos..];

        if let Some(dot_pos) = before_paren.rfind('.') {
            let schema_part = &before_paren[..dot_pos].trim_matches('"');
            let func_name = &before_paren[dot_pos + 1..].trim_matches('"');
            format!("{schema_part}.{func_name}{args_part}")
        } else {
            let name = before_paren.trim_matches('"');
            format!("public.{name}{args_part}")
        }
    } else if trimmed.contains('.') {
        trimmed.to_string()
    } else {
        format!("public.{}", trimmed.trim_matches('"'))
    }
}
