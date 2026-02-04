use crate::filter::Filter;

/// Options for generating a migration plan.
#[derive(Debug, Clone)]
pub struct PlanOptions {
    /// Schema sources with prefix (e.g., "sql:schema.sql", "drizzle:config.ts")
    pub schema_sources: Vec<String>,
    /// Database connection URL (without "db:" prefix)
    pub database_url: String,
    /// PostgreSQL schemas to target (default: ["public"])
    pub target_schemas: Vec<String>,
    /// Optional filter for including/excluding objects
    pub filter: Option<Filter>,
    /// Generate rollback SQL (schema → database direction)
    pub reverse: bool,
    /// Generate zero-downtime expand/contract migration
    pub zero_downtime: bool,
    /// Include ownership management (ALTER ... OWNER TO)
    pub manage_ownership: bool,
    /// Include grant management (GRANT/REVOKE)
    pub manage_grants: bool,
    /// Include objects owned by extensions
    pub include_extension_objects: bool,
}

impl Default for PlanOptions {
    fn default() -> Self {
        Self {
            schema_sources: Vec::new(),
            database_url: String::new(),
            target_schemas: vec!["public".into()],
            filter: None,
            reverse: false,
            zero_downtime: false,
            manage_ownership: false,
            manage_grants: true,
            include_extension_objects: false,
        }
    }
}

impl PlanOptions {
    pub fn new(schema_sources: Vec<String>, database_url: impl Into<String>) -> Self {
        Self {
            schema_sources,
            database_url: database_url.into(),
            ..Default::default()
        }
    }
}

/// Options for applying migrations.
#[derive(Debug, Clone)]
pub struct ApplyOptions {
    /// Schema sources with prefix
    pub schema_sources: Vec<String>,
    /// Database connection URL (without "db:" prefix)
    pub database_url: String,
    /// PostgreSQL schemas to target
    pub target_schemas: Vec<String>,
    /// Optional filter for including/excluding objects
    pub filter: Option<Filter>,
    /// Allow destructive operations (DROP, etc.)
    pub allow_destructive: bool,
    /// Preview only, don't execute
    pub dry_run: bool,
    /// Include ownership management
    pub manage_ownership: bool,
    /// Include grant management
    pub manage_grants: bool,
    /// Include objects owned by extensions
    pub include_extension_objects: bool,
}

impl Default for ApplyOptions {
    fn default() -> Self {
        Self {
            schema_sources: Vec::new(),
            database_url: String::new(),
            target_schemas: vec!["public".into()],
            filter: None,
            allow_destructive: false,
            dry_run: false,
            manage_ownership: false,
            manage_grants: true,
            include_extension_objects: false,
        }
    }
}

impl ApplyOptions {
    pub fn new(schema_sources: Vec<String>, database_url: impl Into<String>) -> Self {
        Self {
            schema_sources,
            database_url: database_url.into(),
            ..Default::default()
        }
    }
}

/// Options for comparing two schemas.
#[derive(Debug, Clone)]
pub struct DiffOptions {
    /// Source schema (e.g., "sql:old.sql")
    pub from: String,
    /// Target schema (e.g., "sql:new.sql")
    pub to: String,
}

impl DiffOptions {
    pub fn new(from: impl Into<String>, to: impl Into<String>) -> Self {
        Self {
            from: from.into(),
            to: to.into(),
        }
    }
}

/// Options for detecting schema drift.
#[derive(Debug, Clone)]
pub struct DriftOptions {
    /// Schema sources with prefix
    pub schema_sources: Vec<String>,
    /// Database connection URL (without "db:" prefix)
    pub database_url: String,
    /// PostgreSQL schemas to target
    pub target_schemas: Vec<String>,
}

impl Default for DriftOptions {
    fn default() -> Self {
        Self {
            schema_sources: Vec::new(),
            database_url: String::new(),
            target_schemas: vec!["public".into()],
        }
    }
}

impl DriftOptions {
    pub fn new(schema_sources: Vec<String>, database_url: impl Into<String>) -> Self {
        Self {
            schema_sources,
            database_url: database_url.into(),
            ..Default::default()
        }
    }
}

/// Options for dumping database schema.
#[derive(Debug, Clone)]
pub struct DumpOptions {
    /// Database connection URL (without "db:" prefix)
    pub database_url: String,
    /// PostgreSQL schemas to dump
    pub target_schemas: Vec<String>,
    /// Optional filter for including/excluding objects
    pub filter: Option<Filter>,
    /// Include objects owned by extensions
    pub include_extension_objects: bool,
}

impl Default for DumpOptions {
    fn default() -> Self {
        Self {
            database_url: String::new(),
            target_schemas: vec!["public".into()],
            filter: None,
            include_extension_objects: false,
        }
    }
}

impl DumpOptions {
    pub fn new(database_url: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
            ..Default::default()
        }
    }
}

/// Options for linting schema or migration plan.
#[derive(Debug, Clone)]
pub struct LintApiOptions {
    /// Schema sources with prefix
    pub schema_sources: Vec<String>,
    /// Optional database URL for migration linting
    pub database_url: Option<String>,
    /// PostgreSQL schemas to target
    pub target_schemas: Vec<String>,
}

impl Default for LintApiOptions {
    fn default() -> Self {
        Self {
            schema_sources: Vec::new(),
            database_url: None,
            target_schemas: vec!["public".into()],
        }
    }
}

impl LintApiOptions {
    pub fn new(schema_sources: Vec<String>) -> Self {
        Self {
            schema_sources,
            ..Default::default()
        }
    }

    pub fn with_database(mut self, database_url: impl Into<String>) -> Self {
        self.database_url = Some(database_url.into());
        self
    }
}
