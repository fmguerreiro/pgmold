use crate::diff::MigrationOp;
use crate::lint::locks::LockWarning;
use crate::lint::LintResult as LintIssue;
use crate::model::Schema;

/// Result of a migration plan operation.
#[derive(Debug, Clone)]
pub struct PlanResult {
    /// Migration operations in execution order
    pub operations: Vec<MigrationOp>,
    /// SQL statements to execute
    pub statements: Vec<String>,
    /// Lock hazard warnings
    pub lock_warnings: Vec<LockWarning>,
    /// Whether the plan is empty (no changes needed)
    pub is_empty: bool,
}

impl PlanResult {
    pub fn empty() -> Self {
        Self {
            operations: Vec::new(),
            statements: Vec::new(),
            lock_warnings: Vec::new(),
            is_empty: true,
        }
    }
}

/// Result of a zero-downtime migration plan.
#[derive(Debug, Clone)]
pub struct PhasedPlanResult {
    /// Expand phase: additive, safe changes
    pub expand: Vec<String>,
    /// Backfill phase: data migration statements
    pub backfill: Vec<String>,
    /// Contract phase: cleanup, requires verification
    pub contract: Vec<String>,
}

impl PhasedPlanResult {
    pub fn is_empty(&self) -> bool {
        self.expand.is_empty() && self.backfill.is_empty() && self.contract.is_empty()
    }

    pub fn total_statements(&self) -> usize {
        self.expand.len() + self.backfill.len() + self.contract.len()
    }
}

/// Result of applying migrations.
#[derive(Debug, Clone)]
pub struct ApplyResult {
    /// Number of statements executed
    pub statements_executed: usize,
    /// Whether this was a dry run
    pub dry_run: bool,
}

/// Result of comparing two schemas.
#[derive(Debug, Clone)]
pub struct DiffResult {
    /// Migration operations representing differences
    pub operations: Vec<MigrationOp>,
    /// Whether schemas are identical
    pub is_empty: bool,
}

impl DiffResult {
    pub fn empty() -> Self {
        Self {
            operations: Vec::new(),
            is_empty: true,
        }
    }
}

/// Result of drift detection.
#[derive(Debug, Clone)]
pub struct DriftResult {
    /// Whether drift was detected
    pub has_drift: bool,
    /// Expected schema fingerprint (from sources)
    pub expected_fingerprint: String,
    /// Actual schema fingerprint (from database)
    pub actual_fingerprint: String,
    /// Operations representing drift
    pub differences: Vec<MigrationOp>,
}

/// Result of schema dump.
#[derive(Debug, Clone)]
pub struct DumpResult {
    /// Generated SQL DDL
    pub sql: String,
    /// Parsed schema (for further inspection)
    pub schema: Schema,
}

/// Result of schema/migration linting.
#[derive(Debug, Clone)]
pub struct LintApiResult {
    /// Lint issues found
    pub issues: Vec<LintIssue>,
    /// Whether any errors (not just warnings) were found
    pub has_errors: bool,
}

impl LintApiResult {
    pub fn is_clean(&self) -> bool {
        self.issues.is_empty()
    }
}
