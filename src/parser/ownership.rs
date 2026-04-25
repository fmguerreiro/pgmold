use crate::model::*;
use regex::Regex;

use super::util::unquote_ident;

pub(super) fn parse_owner_statements(sql: &str, schema: &mut Schema) {
    let alter_function_owner_re = Regex::new(
        r#"(?i)ALTER\s+FUNCTION\s+(?:["']?([^"'\s(]+)["']?\.)?["']?([^"'\s(]+)["']?\s*\(([^)]*)\)\s+OWNER\s+TO\s+["']?([^"'\s;]+)["']?"#
    ).unwrap();

    for cap in alter_function_owner_re.captures_iter(sql) {
        let schema_part = cap.get(1).map(|m| unquote_ident(m.as_str()));
        let func_name = unquote_ident(cap.get(2).unwrap().as_str());
        let args_str = cap.get(3).unwrap().as_str();
        let owner = unquote_ident(cap.get(4).unwrap().as_str());

        let func_schema = schema_part.unwrap_or("public");
        let object_key = format!("{func_schema}.{func_name}({args_str})");
        schema.pending_owners.push(PendingOwner {
            object_type: PendingOwnerObjectType::Function,
            object_key,
            owner: owner.to_string(),
        });
    }

    let alter_domain_owner_re = Regex::new(
        r#"(?i)ALTER\s+DOMAIN\s+(?:["']?([^"'\s]+)["']?\.)?["']?([^"'\s;]+)["']?\s+OWNER\s+TO\s+["']?([^"'\s;]+)["']?"#
    ).unwrap();

    for cap in alter_domain_owner_re.captures_iter(sql) {
        let schema_part = cap.get(1).map(|m| unquote_ident(m.as_str()));
        let domain_name = unquote_ident(cap.get(2).unwrap().as_str());
        let owner = unquote_ident(cap.get(3).unwrap().as_str());

        let domain_schema = schema_part.unwrap_or("public");
        let object_key = qualified_name(domain_schema, domain_name);
        schema.pending_owners.push(PendingOwner {
            object_type: PendingOwnerObjectType::Domain,
            object_key,
            owner: owner.to_string(),
        });
    }

    let alter_materialized_view_owner_re = Regex::new(
        r#"(?i)ALTER\s+MATERIALIZED\s+VIEW\s+(?:["']?([^"'\s]+)["']?\.)?["']?([^"'\s;]+)["']?\s+OWNER\s+TO\s+["']?([^"'\s;]+)["']?"#
    ).unwrap();

    for cap in alter_materialized_view_owner_re.captures_iter(sql) {
        let schema_part = cap.get(1).map(|m| unquote_ident(m.as_str()));
        let view_name = unquote_ident(cap.get(2).unwrap().as_str());
        let owner = unquote_ident(cap.get(3).unwrap().as_str());

        let view_schema = schema_part.unwrap_or("public");
        let object_key = qualified_name(view_schema, view_name);
        schema.pending_owners.push(PendingOwner {
            object_type: PendingOwnerObjectType::View,
            object_key,
            owner: owner.to_string(),
        });
    }

    let alter_view_owner_re = Regex::new(
        r#"(?i)ALTER\s+VIEW\s+(?:["']?([^"'\s]+)["']?\.)?["']?([^"'\s;]+)["']?\s+OWNER\s+TO\s+["']?([^"'\s;]+)["']?"#
    ).unwrap();

    for cap in alter_view_owner_re.captures_iter(sql) {
        let schema_part = cap.get(1).map(|m| unquote_ident(m.as_str()));
        let view_name = unquote_ident(cap.get(2).unwrap().as_str());
        let owner = unquote_ident(cap.get(3).unwrap().as_str());

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
        let schema_part = cap.get(1).map(|m| unquote_ident(m.as_str()));
        let sequence_name = unquote_ident(cap.get(2).unwrap().as_str());
        let owner = unquote_ident(cap.get(3).unwrap().as_str());

        let seq_schema = schema_part.unwrap_or("public");
        let object_key = qualified_name(seq_schema, sequence_name);
        schema.pending_owners.push(PendingOwner {
            object_type: PendingOwnerObjectType::Sequence,
            object_key,
            owner: owner.to_string(),
        });
    }
}
