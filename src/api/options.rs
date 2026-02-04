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
            manage_ownership: false,
            manage_grants: true,
            include_extension_objects: false,
        }
    }
}

impl PlanOptions {
    /// Create new plan options with required fields.
    pub fn new(schema_sources: Vec<String>, database_url: impl Into<String>) -> Self {
        Self {
            schema_sources,
            database_url: database_url.into(),
            ..Default::default()
        }
    }

    /// Set target schemas.
    pub fn with_target_schemas(mut self, schemas: Vec<String>) -> Self {
        self.target_schemas = schemas;
        self
    }

    /// Set filter for including/excluding objects.
    pub fn with_filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Generate rollback SQL (reverse direction).
    pub fn reverse(mut self) -> Self {
        self.reverse = true;
        self
    }

    /// Include ownership management.
    pub fn manage_ownership(mut self) -> Self {
        self.manage_ownership = true;
        self
    }

    /// Disable grant management (enabled by default).
    pub fn without_grants(mut self) -> Self {
        self.manage_grants = false;
        self
    }

    /// Include extension-owned objects.
    pub fn include_extension_objects(mut self) -> Self {
        self.include_extension_objects = true;
        self
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
    /// Create new apply options with required fields.
    pub fn new(schema_sources: Vec<String>, database_url: impl Into<String>) -> Self {
        Self {
            schema_sources,
            database_url: database_url.into(),
            ..Default::default()
        }
    }

    /// Set target schemas.
    pub fn with_target_schemas(mut self, schemas: Vec<String>) -> Self {
        self.target_schemas = schemas;
        self
    }

    /// Set filter for including/excluding objects.
    pub fn with_filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Allow destructive operations (DROP, etc.).
    pub fn allow_destructive(mut self) -> Self {
        self.allow_destructive = true;
        self
    }

    /// Enable dry run mode (preview only).
    pub fn dry_run(mut self) -> Self {
        self.dry_run = true;
        self
    }

    /// Include ownership management.
    pub fn manage_ownership(mut self) -> Self {
        self.manage_ownership = true;
        self
    }

    /// Disable grant management (enabled by default).
    pub fn without_grants(mut self) -> Self {
        self.manage_grants = false;
        self
    }

    /// Include extension-owned objects.
    pub fn include_extension_objects(mut self) -> Self {
        self.include_extension_objects = true;
        self
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
    /// Create new diff options.
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
    /// Create new drift options with required fields.
    pub fn new(schema_sources: Vec<String>, database_url: impl Into<String>) -> Self {
        Self {
            schema_sources,
            database_url: database_url.into(),
            ..Default::default()
        }
    }

    /// Set target schemas.
    pub fn with_target_schemas(mut self, schemas: Vec<String>) -> Self {
        self.target_schemas = schemas;
        self
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
    /// Create new dump options with required fields.
    pub fn new(database_url: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
            ..Default::default()
        }
    }

    /// Set target schemas.
    pub fn with_target_schemas(mut self, schemas: Vec<String>) -> Self {
        self.target_schemas = schemas;
        self
    }

    /// Set filter for including/excluding objects.
    pub fn with_filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Include extension-owned objects.
    pub fn include_extension_objects(mut self) -> Self {
        self.include_extension_objects = true;
        self
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
    /// Create new lint options with required fields.
    pub fn new(schema_sources: Vec<String>) -> Self {
        Self {
            schema_sources,
            ..Default::default()
        }
    }

    /// Set database URL for migration linting.
    pub fn with_database(mut self, database_url: impl Into<String>) -> Self {
        self.database_url = Some(database_url.into());
        self
    }

    /// Set target schemas.
    pub fn with_target_schemas(mut self, schemas: Vec<String>) -> Self {
        self.target_schemas = schemas;
        self
    }
}
