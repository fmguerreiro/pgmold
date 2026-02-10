use crate::baseline::unsupported::UnsupportedObject;
use crate::model::Schema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectCounts {
    pub extensions: usize,
    pub enums: usize,
    pub tables: usize,
    pub functions: usize,
    pub views: usize,
    pub triggers: usize,
    pub sequences: usize,
}

impl ObjectCounts {
    pub fn from_schema(schema: &Schema) -> Self {
        Self {
            extensions: schema.extensions.len(),
            enums: schema.enums.len(),
            tables: schema.tables.len(),
            functions: schema.functions.len(),
            views: schema.views.len(),
            triggers: schema.triggers.len(),
            sequences: schema.sequences.len(),
        }
    }

    pub fn total(&self) -> usize {
        self.extensions
            + self.enums
            + self.tables
            + self.functions
            + self.views
            + self.triggers
            + self.sequences
    }

    pub fn is_empty(&self) -> bool {
        self.total() == 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaselineReport {
    pub database_url: String,
    pub target_schemas: Vec<String>,
    pub output_path: String,
    pub object_counts: ObjectCounts,
    pub round_trip_ok: bool,
    pub zero_diff_ok: bool,
    pub fingerprint: String,
    pub warnings: Vec<UnsupportedObject>,
}

impl BaselineReport {
    pub fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }

    pub fn is_success(&self) -> bool {
        self.round_trip_ok && self.zero_diff_ok
    }
}

pub fn generate_text_report(report: &BaselineReport) -> String {
    let mut output = String::new();

    output.push_str("=== pgmold baseline ===\n");
    output.push_str(&format!("Database: {}\n", report.database_url));
    output.push_str(&format!("Schemas: {}\n", report.target_schemas.join(", ")));
    output.push('\n');

    output.push_str("Objects captured:\n");
    for (label, count) in [
        ("Extensions:", report.object_counts.extensions),
        ("Enums:", report.object_counts.enums),
        ("Tables:", report.object_counts.tables),
        ("Functions:", report.object_counts.functions),
        ("Views:", report.object_counts.views),
        ("Triggers:", report.object_counts.triggers),
        ("Sequences:", report.object_counts.sequences),
    ] {
        output.push_str(&format!("  {label:<14}{count:>3}\n"));
    }
    output.push('\n');

    output.push_str("Verification:\n");
    let rt_status = if report.round_trip_ok { "✓" } else { "✗" };
    output.push_str(&format!(
        "  {rt_status} Round-trip fidelity: {}\n",
        status_text(report.round_trip_ok)
    ));
    let zd_status = if report.zero_diff_ok { "✓" } else { "✗" };
    output.push_str(&format!(
        "  {zd_status} Zero-diff guarantee: {}\n",
        status_text(report.zero_diff_ok)
    ));
    output.push_str(&format!("  Fingerprint: {}\n", report.fingerprint));
    output.push('\n');

    if !report.warnings.is_empty() {
        output.push_str("Warnings:\n");
        let grouped = group_warnings(&report.warnings);
        for (kind, objects) in grouped {
            output.push_str(&format!(
                "  ⚠ {} {} detected (not supported)\n",
                objects.len(),
                kind
            ));
        }
        output.push('\n');
    }

    output.push_str(&format!("Output written to: {}\n", report.output_path));
    output.push('\n');

    output.push_str("Next steps:\n");
    output.push_str("  1. Review the output file and commit to version control\n");
    output.push_str("  2. Run 'pgmold plan' against the same database to verify zero changes\n");
    output.push_str("  3. Use 'pgmold apply' for future migrations\n");

    output
}

pub fn generate_json_report(report: &BaselineReport) -> String {
    serde_json::to_string_pretty(report).unwrap()
}

fn status_text(ok: bool) -> &'static str {
    if ok {
        "PASS"
    } else {
        "FAIL"
    }
}

fn group_warnings(
    warnings: &[UnsupportedObject],
) -> BTreeMap<&'static str, Vec<&UnsupportedObject>> {
    let mut grouped: BTreeMap<&'static str, Vec<&UnsupportedObject>> = BTreeMap::new();
    for warning in warnings {
        grouped.entry(warning.kind()).or_default().push(warning);
    }
    grouped
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_report() -> BaselineReport {
        BaselineReport {
            database_url: "postgres://user:****@localhost:5432/db".into(),
            target_schemas: vec!["public".into()],
            output_path: "schema.sql".into(),
            object_counts: ObjectCounts {
                extensions: 2,
                enums: 1,
                tables: 5,
                functions: 3,
                views: 1,
                triggers: 2,
                sequences: 4,
            },
            round_trip_ok: true,
            zero_diff_ok: true,
            fingerprint: "abc123def456".into(),
            warnings: vec![],
        }
    }

    #[test]
    fn object_counts_from_schema() {
        let schema = Schema::default();
        let counts = ObjectCounts::from_schema(&schema);
        assert!(counts.is_empty());
        assert_eq!(counts.total(), 0);
    }

    #[test]
    fn object_counts_total() {
        let counts = ObjectCounts {
            extensions: 1,
            enums: 2,
            tables: 3,
            functions: 4,
            views: 5,
            triggers: 6,
            sequences: 7,
        };
        assert_eq!(counts.total(), 28);
        assert!(!counts.is_empty());
    }

    #[test]
    fn baseline_report_success() {
        let report = sample_report();
        assert!(report.is_success());
        assert!(!report.has_warnings());
    }

    #[test]
    fn baseline_report_failure() {
        let mut report = sample_report();
        report.round_trip_ok = false;
        assert!(!report.is_success());
    }

    #[test]
    fn baseline_report_with_warnings() {
        let mut report = sample_report();
        report.warnings.push(UnsupportedObject::CompositeType {
            schema: "public".into(),
            name: "address".into(),
        });
        assert!(report.has_warnings());
        assert!(report.is_success());
    }

    #[test]
    fn text_report_contains_sections() {
        let report = sample_report();
        let text = generate_text_report(&report);

        assert!(text.contains("=== pgmold baseline ==="));
        assert!(text.contains("Objects captured:"));
        assert!(text.contains("Verification:"));
        assert!(text.contains("Round-trip fidelity: PASS"));
        assert!(text.contains("Zero-diff guarantee: PASS"));
        assert!(text.contains("Next steps:"));
    }

    #[test]
    fn text_report_includes_database_url() {
        let report = sample_report();
        let text = generate_text_report(&report);

        assert!(text.contains(&report.database_url));
    }

    #[test]
    fn json_report_does_not_leak_credentials() {
        let report = sample_report();
        let json = generate_json_report(&report);

        assert!(!json.contains("password"));
        assert!(json.contains("****"));
    }

    #[test]
    fn text_report_shows_warnings() {
        let mut report = sample_report();
        report.warnings.push(UnsupportedObject::CompositeType {
            schema: "public".into(),
            name: "address".into(),
        });
        report.warnings.push(UnsupportedObject::CompositeType {
            schema: "public".into(),
            name: "person".into(),
        });
        report.warnings.push(UnsupportedObject::Aggregate {
            schema: "public".into(),
            name: "my_agg".into(),
        });

        let text = generate_text_report(&report);

        assert!(text.contains("Warnings:"));
        assert!(text.contains("2 composite type"));
        assert!(text.contains("1 aggregate"));
    }

    #[test]
    fn json_report_serializes() {
        let report = sample_report();
        let json = generate_json_report(&report);

        assert!(json.contains("\"round_trip_ok\": true"));
        assert!(json.contains("\"zero_diff_ok\": true"));
        assert!(json.contains("\"fingerprint\": \"abc123def456\""));
    }
}
