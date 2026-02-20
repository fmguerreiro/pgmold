//! Convenient re-exports for common pgmold usage.
//!
//! # Example
//!
//! ```no_run
//! use pgmold::prelude::*;
//!
//! let result = plan_blocking(PlanOptions::new(
//!     vec!["sql:schema.sql".into()],
//!     "postgres://localhost/mydb",
//! )).unwrap();
//!
//! println!("Generated {} statements", result.statements.len());
//! ```

// Async functions
pub use crate::api::{apply, diff, drift, dump, lint, plan, plan_phased};

// Blocking functions
pub use crate::api::{
    apply_blocking, diff_blocking, drift_blocking, dump_blocking, lint_blocking, plan_blocking,
    plan_phased_blocking,
};

// Options
pub use crate::api::{
    ApplyOptions, DiffOptions, DriftOptions, DumpOptions, LintApiOptions, PlanOptions,
};

// Results
pub use crate::api::{
    ApplyResult, DiffResult, DriftResult, DumpResult, LintApiResult, PhasedPlanResult, PlanResult,
};

// Error types
pub use crate::api::{Error, ValidationError};

// Core types
pub use crate::diff::MigrationOp;
pub use crate::filter::{Filter, ObjectType};
pub use crate::model::Schema;

// Re-export LintResult as LintIssue for accessing Error::LintFailed issues
pub use crate::lint::LintResult as LintIssue;
