use crate::model::*;
use regex::Regex;

pub(super) fn parse_owner_statements(sql: &str, schema: &mut Schema) {
    let alter_function_owner_re = Regex::new(
        r#"(?i)ALTER\s+FUNCTION\s+(?:["']?([^"'\s(]+)["']?\.)?["']?([^"'\s(]+)["']?\s*\(([^)]*)\)\s+OWNER\s+TO\s+["']?([^"'\s;]+)["']?"#
    ).unwrap();

    for cap in alter_function_owner_re.captures_iter(sql) {
        let schema_part = cap.get(1).map(|m| m.as_str().trim_matches('"'));
        let func_name = cap.get(2).unwrap().as_str().trim_matches('"');
        let args_str = cap.get(3).unwrap().as_str();
        let owner = cap.get(4).unwrap().as_str().trim_matches('"');

        let func_schema = schema_part.unwrap_or("public");
        let object_key = format!("{func_schema}.{func_name}({args_str})");
        schema.pending_owners.push(PendingOwner {
            object_type: PendingOwnerObjectType::Function,
            object_key,
            owner: owner.to_string(),
        });
    }

    let alter_type_owner_re = Regex::new(
        r#"(?i)ALTER\s+TYPE\s+(?:["']?([^"'\s]+)["']?\.)?["']?([^"'\s;]+)["']?\s+OWNER\s+TO\s+["']?([^"'\s;]+)["']?"#
    ).unwrap();

    for cap in alter_type_owner_re.captures_iter(sql) {
        let schema_part = cap.get(1).map(|m| m.as_str().trim_matches('"'));
        let type_name = cap.get(2).unwrap().as_str().trim_matches('"');
        let owner = cap.get(3).unwrap().as_str().trim_matches('"');

        let type_schema = schema_part.unwrap_or("public");
        let object_key = qualified_name(type_schema, type_name);
        schema.pending_owners.push(PendingOwner {
            object_type: PendingOwnerObjectType::Enum,
            object_key,
            owner: owner.to_string(),
        });
    }

    let alter_domain_owner_re = Regex::new(
        r#"(?i)ALTER\s+DOMAIN\s+(?:["']?([^"'\s]+)["']?\.)?["']?([^"'\s;]+)["']?\s+OWNER\s+TO\s+["']?([^"'\s;]+)["']?"#
    ).unwrap();

    for cap in alter_domain_owner_re.captures_iter(sql) {
        let schema_part = cap.get(1).map(|m| m.as_str().trim_matches('"'));
        let domain_name = cap.get(2).unwrap().as_str().trim_matches('"');
        let owner = cap.get(3).unwrap().as_str().trim_matches('"');

        let domain_schema = schema_part.unwrap_or("public");
        let object_key = qualified_name(domain_schema, domain_name);
        schema.pending_owners.push(PendingOwner {
            object_type: PendingOwnerObjectType::Domain,
            object_key,
            owner: owner.to_string(),
        });
    }

    let alter_table_owner_re = Regex::new(
        r#"(?i)ALTER\s+TABLE\s+(?:["']?([^"'\s]+)["']?\.)?["']?([^"'\s;]+)["']?\s+OWNER\s+TO\s+["']?([^"'\s;]+)["']?"#
    ).unwrap();

    for cap in alter_table_owner_re.captures_iter(sql) {
        let schema_part = cap.get(1).map(|m| m.as_str().trim_matches('"'));
        let table_name = cap.get(2).unwrap().as_str().trim_matches('"');
        let owner = cap.get(3).unwrap().as_str().trim_matches('"');

        let table_schema = schema_part.unwrap_or("public");
        let object_key = qualified_name(table_schema, table_name);
        schema.pending_owners.push(PendingOwner {
            object_type: PendingOwnerObjectType::Table,
            object_key,
            owner: owner.to_string(),
        });
    }

    let alter_view_owner_re = Regex::new(
        r#"(?i)ALTER\s+VIEW\s+(?:["']?([^"'\s]+)["']?\.)?["']?([^"'\s;]+)["']?\s+OWNER\s+TO\s+["']?([^"'\s;]+)["']?"#
    ).unwrap();

    for cap in alter_view_owner_re.captures_iter(sql) {
        let schema_part = cap.get(1).map(|m| m.as_str().trim_matches('"'));
        let view_name = cap.get(2).unwrap().as_str().trim_matches('"');
        let owner = cap.get(3).unwrap().as_str().trim_matches('"');

        let view_schema = schema_part.unwrap_or("public");
        let object_key = qualified_name(view_schema, view_name);
        schema.pending_owners.push(PendingOwner {
            object_type: PendingOwnerObjectType::View,
            object_key,
            owner: owner.to_string(),
        });
    }

    let alter_sequence_owner_re = Regex::new(
        r#"(?i)ALTER\s+SEQUENCE\s+(?:["']?([^"'\s]+)["']?\.)?["']?([^"'\s;]+)["']?\s+OWNER\s+TO\s+["']?([^"'\s;]+)["']?"#
    ).unwrap();

    for cap in alter_sequence_owner_re.captures_iter(sql) {
        let schema_part = cap.get(1).map(|m| m.as_str().trim_matches('"'));
        let sequence_name = cap.get(2).unwrap().as_str().trim_matches('"');
        let owner = cap.get(3).unwrap().as_str().trim_matches('"');

        let seq_schema = schema_part.unwrap_or("public");
        let object_key = qualified_name(seq_schema, sequence_name);
        schema.pending_owners.push(PendingOwner {
            object_type: PendingOwnerObjectType::Sequence,
            object_key,
            owner: owner.to_string(),
        });
    }
}
