use crate::diff::{compute_diff, MigrationOp};
use crate::parser::load_schema_sources;
use crate::pg::connection::PgConnection;
use crate::pg::introspect::introspect_schema;
use crate::util::Result;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DriftReport {
    pub has_drift: bool,
    pub expected_fingerprint: String,
    pub actual_fingerprint: String,
    pub differences: Vec<MigrationOp>,
}

pub async fn detect_drift(schema_sources: &[String], conn: &PgConnection) -> Result<DriftReport> {
    let expected = load_schema_sources(schema_sources)?;
    let actual = introspect_schema(conn, &[String::from("public")]).await?;

    let expected_fingerprint = expected.fingerprint();
    let actual_fingerprint = actual.fingerprint();
    let has_drift = expected_fingerprint != actual_fingerprint;

    let differences = if has_drift {
        compute_diff(&actual, &expected)
    } else {
        vec![]
    };

    Ok(DriftReport {
        has_drift,
        expected_fingerprint,
        actual_fingerprint,
        differences,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Column, PgType, Table};
    use std::collections::BTreeMap;

    #[test]
    fn drift_report_fields() {
        let report = DriftReport {
            has_drift: true,
            expected_fingerprint: "abc123".to_string(),
            actual_fingerprint: "def456".to_string(),
            differences: vec![],
        };

        assert!(report.has_drift);
        assert_eq!(report.expected_fingerprint, "abc123");
        assert_eq!(report.actual_fingerprint, "def456");
        assert!(report.differences.is_empty());
    }

    #[test]
    fn drift_report_with_differences() {
        let mut table = Table {
            name: "users".to_string(),
            schema: "public".to_string(),
            columns: BTreeMap::new(),
            indexes: Vec::new(),
            primary_key: None,
            foreign_keys: Vec::new(),
            check_constraints: Vec::new(),
            comment: None,
            row_level_security: false,
            policies: Vec::new(),
        };
        table.columns.insert(
            "email".to_string(),
            Column {
                name: "email".to_string(),
                data_type: PgType::Text,
                nullable: true,
                default: None,
                comment: None,
            },
        );

        let differences = vec![MigrationOp::AddColumn {
            table: "users".to_string(),
            column: table.columns.get("email").unwrap().clone(),
        }];

        let report = DriftReport {
            has_drift: true,
            expected_fingerprint: "abc".to_string(),
            actual_fingerprint: "xyz".to_string(),
            differences,
        };

        assert!(report.has_drift);
        assert_eq!(report.differences.len(), 1);
    }
}
