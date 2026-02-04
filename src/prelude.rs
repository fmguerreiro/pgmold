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

pub use crate::api::{
    // Async functions
    apply,
    // Blocking functions
    apply_blocking,
    diff,
    diff_blocking,
    drift,
    drift_blocking,
    dump,
    dump_blocking,
    lint,
    lint_blocking,
    plan,
    plan_blocking,
    plan_phased,
    plan_phased_blocking,
    // Options
    ApplyOptions,
    // Results
    ApplyResult,
    DiffOptions,
    DiffResult,
    DriftOptions,
    DriftResult,
    DumpOptions,
    DumpResult,
    // Error
    Error,
    LintApiOptions,
    LintApiResult,
    PhasedPlanResult,
    PlanOptions,
    PlanResult,
    ValidationError,
};

pub use crate::diff::MigrationOp;
pub use crate::filter::{Filter, ObjectType};
pub use crate::model::Schema;
