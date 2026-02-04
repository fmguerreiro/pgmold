//! pgmold - PostgreSQL schema-as-code management library.
//!
//! This crate provides tools for managing PostgreSQL schemas declaratively.
//! Define schemas in native SQL, diff against live databases, and apply migrations safely.
//!
//! # Quick Start
//!
//! Use the high-level API via the [`api`] module or [`prelude`]:
//!
//! ```no_run
//! use pgmold::prelude::*;
//!
//! // Generate a migration plan
//! let result = plan_blocking(PlanOptions::new(
//!     vec!["sql:schema.sql".into()],
//!     "postgres://localhost/mydb",
//! )).unwrap();
//!
//! for statement in &result.statements {
//!     println!("{}", statement);
//! }
//! ```
//!
//! # Modules
//!
//! - [`api`] - High-level API mirroring CLI commands
//! - [`prelude`] - Convenient re-exports for common usage
//! - [`model`] - Schema model types (Table, Column, Index, etc.)
//! - [`diff`] - Schema comparison and migration operations
//! - [`filter`] - Object filtering by name and type

pub mod api;
pub mod apply;
pub mod baseline;
pub mod diff;
pub mod drift;
pub mod dump;
pub mod expand_contract;
pub mod filter;
pub mod lint;
pub mod migrate;
pub mod model;
pub mod parser;
pub mod pg;
pub mod prelude;
pub mod provider;
pub mod util;
pub mod validate;
