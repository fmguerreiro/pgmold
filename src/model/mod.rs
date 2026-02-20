use crate::util::{canonicalize_expression, views_semantically_equal};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Represents a pending ownership assignment parsed from ALTER ... OWNER TO statements.
/// Used for cross-file resolution when object definitions and ownership are in separate files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingOwner {
    pub object_type: PendingOwnerObjectType,
    /// Qualified name for most objects (schema.name), or function signature for functions
    pub object_key: String,
    pub owner: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingOwnerObjectType {
    Table,
    View,
    Sequence,
    Function,
    Enum,
    Domain,
}

/// Represents a pending GRANT parsed from a GRANT statement.
/// Used for cross-file resolution when GRANT statements are in separate files from object definitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingGrant {
    pub object_type: PendingGrantObjectType,
    /// Qualified name for most objects (schema.name), function signature for functions,
    /// or bare name for schemas
    pub object_key: String,
    pub grant: Grant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingGrantObjectType {
    Table,
    View,
    Sequence,
    Function,
    Schema,
    Enum,
    Domain,
}

/// Represents a pending REVOKE parsed from a REVOKE statement.
/// Used for cross-file resolution when REVOKE statements are in separate files from object definitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingRevoke {
    pub object_type: PendingGrantObjectType,
    pub object_key: String,
    pub grantee: String,
    pub privileges: BTreeSet<Privilege>,
    pub grant_option_for: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Schema {
    pub schemas: BTreeMap<String, PgSchema>,
    pub extensions: BTreeMap<String, Extension>,
    pub tables: BTreeMap<String, Table>,
    pub enums: BTreeMap<String, EnumType>,
    pub domains: BTreeMap<String, Domain>,
    pub functions: BTreeMap<String, Function>,
    pub views: BTreeMap<String, View>,
    pub triggers: BTreeMap<String, Trigger>,
    pub sequences: BTreeMap<String, Sequence>,
    pub partitions: BTreeMap<String, Partition>,
    /// Policies collected during parsing, awaiting association with tables.
    /// Cleared after finalize() is called.
    #[serde(skip)]
    pub pending_policies: Vec<Policy>,
    /// Ownership assignments collected during parsing, awaiting application to objects.
    /// Cleared after finalize() is called.
    #[serde(skip)]
    pub pending_owners: Vec<PendingOwner>,
    /// Grant statements collected during parsing, awaiting association with objects.
    /// Cleared after finalize() is called.
    #[serde(skip)]
    pub pending_grants: Vec<PendingGrant>,
    /// Revoke statements collected during parsing, awaiting association with objects.
    /// Cleared after finalize() is called.
    #[serde(skip)]
    pub pending_revokes: Vec<PendingRevoke>,
    pub default_privileges: Vec<DefaultPrivilege>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Domain {
    pub schema: String,
    pub name: String,
    pub data_type: PgType,
    pub default: Option<String>,
    pub not_null: bool,
    pub collation: Option<String>,
    pub check_constraints: Vec<DomainConstraint>,
    pub owner: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub grants: Vec<Grant>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DomainConstraint {
    pub name: Option<String>,
    pub expression: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Table {
    pub schema: String,
    pub name: String,
    pub columns: BTreeMap<String, Column>,
    pub indexes: Vec<Index>,
    pub primary_key: Option<PrimaryKey>,
    pub foreign_keys: Vec<ForeignKey>,
    pub check_constraints: Vec<CheckConstraint>,
    pub comment: Option<String>,
    pub row_level_security: bool,
    pub policies: Vec<Policy>,
    pub partition_by: Option<PartitionKey>,
    pub owner: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub grants: Vec<Grant>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub data_type: PgType,
    pub nullable: bool,
    pub default: Option<String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PgType {
    Integer,
    BigInt,
    SmallInt,
    Real,
    DoublePrecision,
    Varchar(Option<u32>),
    Text,
    Boolean,
    TimestampTz,
    Timestamp,
    Date,
    Uuid,
    Json,
    Jsonb,
    Vector(Option<u32>),
    CustomEnum(String),
    Named(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Index {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub index_type: IndexType,
    pub predicate: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum IndexType {
    BTree,
    Hash,
    Gin,
    Gist,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrimaryKey {
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ForeignKey {
    pub name: String,
    pub columns: Vec<String>,
    pub referenced_schema: String,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
    pub on_delete: ReferentialAction,
    pub on_update: ReferentialAction,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum ReferentialAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct CheckConstraint {
    pub name: String,
    pub expression: String,
}

impl CheckConstraint {
    /// Compares two check constraints semantically, accounting for PostgreSQL's expression normalization.
    pub fn semantically_equals(&self, other: &CheckConstraint) -> bool {
        self.name == other.name
            && canonicalize_expression(&self.expression)
                == canonicalize_expression(&other.expression)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PartitionStrategy {
    Range,
    List,
    Hash,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionKey {
    pub strategy: PartitionStrategy,
    pub columns: Vec<String>,
    pub expressions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PartitionBound {
    Range { from: Vec<String>, to: Vec<String> },
    List { values: Vec<String> },
    Hash { modulus: u32, remainder: u32 },
    Default,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Partition {
    pub schema: String,
    pub name: String,
    pub parent_schema: String,
    pub parent_name: String,
    pub bound: PartitionBound,
    pub indexes: Vec<Index>,
    pub check_constraints: Vec<CheckConstraint>,
    pub owner: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EnumType {
    pub schema: String,
    pub name: String,
    pub values: Vec<String>,
    pub owner: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub grants: Vec<Grant>,
}

/// Represents a PostgreSQL schema (namespace).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct PgSchema {
    pub name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub grants: Vec<Grant>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Extension {
    pub name: String,
    pub version: Option<String>,
    pub schema: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Privilege {
    Select,
    Insert,
    Update,
    Delete,
    Truncate,
    References,
    Trigger,
    Usage,
    Execute,
    Create,
}

/// Object types that can have default privileges
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum DefaultPrivilegeObjectType {
    Tables,
    Sequences,
    Functions,
    Routines,
    Types,
    Schemas,
}

impl DefaultPrivilegeObjectType {
    /// Returns the SQL keyword for this object type
    pub fn as_sql_str(&self) -> &'static str {
        match self {
            DefaultPrivilegeObjectType::Tables => "TABLES",
            DefaultPrivilegeObjectType::Sequences => "SEQUENCES",
            DefaultPrivilegeObjectType::Functions => "FUNCTIONS",
            DefaultPrivilegeObjectType::Routines => "ROUTINES",
            DefaultPrivilegeObjectType::Types => "TYPES",
            DefaultPrivilegeObjectType::Schemas => "SCHEMAS",
        }
    }

    /// Parse from SQL keyword string
    pub fn from_sql_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "TABLES" => Some(DefaultPrivilegeObjectType::Tables),
            "SEQUENCES" => Some(DefaultPrivilegeObjectType::Sequences),
            "FUNCTIONS" => Some(DefaultPrivilegeObjectType::Functions),
            "ROUTINES" => Some(DefaultPrivilegeObjectType::Routines),
            "TYPES" => Some(DefaultPrivilegeObjectType::Types),
            "SCHEMAS" => Some(DefaultPrivilegeObjectType::Schemas),
            _ => None,
        }
    }
}

/// A default privilege entry - privileges automatically granted when a role creates objects
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DefaultPrivilege {
    /// Role whose created objects get this privilege (or "CURRENT_ROLE" sentinel)
    pub target_role: String,
    /// Schema scope (None = global, Some = per-schema)
    pub schema: Option<String>,
    /// Type of objects affected
    pub object_type: DefaultPrivilegeObjectType,
    /// Role receiving the privilege
    pub grantee: String,
    /// Privileges being granted
    pub privileges: BTreeSet<Privilege>,
    /// Whether grantee can grant to others
    pub with_grant_option: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Grant {
    pub grantee: String,
    pub privileges: BTreeSet<Privilege>,
    pub with_grant_option: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Policy {
    pub name: String,
    pub table_schema: String,
    pub table: String,
    pub command: PolicyCommand,
    pub roles: Vec<String>,
    pub using_expr: Option<String>,
    pub check_expr: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum PolicyCommand {
    All,
    Select,
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Function {
    pub name: String,
    pub schema: String,
    pub arguments: Vec<FunctionArg>,
    pub return_type: String,
    pub language: String,
    pub body: String,
    pub volatility: Volatility,
    pub security: SecurityType,
    pub config_params: Vec<(String, String)>,
    pub owner: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub grants: Vec<Grant>,
}

impl Function {
    /// Compares two functions semantically, ignoring whitespace differences in body.
    /// Note: ownership (`owner` field) is not compared here. pgmold does not
    /// manage function ownership - PostgreSQL sets it to the creating user.
    pub fn semantically_equals(&self, other: &Function) -> bool {
        // Compare arguments semantically to handle default value normalization
        let args_equal = self.arguments.len() == other.arguments.len()
            && self
                .arguments
                .iter()
                .zip(other.arguments.iter())
                .all(|(a, b)| a.semantically_equals(b));

        self.name == other.name
            && self.schema == other.schema
            && args_equal
            && normalize_return_type(&self.return_type) == normalize_return_type(&other.return_type)
            && self.language == other.language
            && self.volatility == other.volatility
            && self.security == other.security
            && self.config_params == other.config_params
            && normalize_sql_body(&self.body) == normalize_sql_body(&other.body)
    }

    /// Checks if the function differences require DROP + CREATE instead of CREATE OR REPLACE.
    /// PostgreSQL doesn't allow changing parameter names or defaults via CREATE OR REPLACE.
    pub fn requires_drop_recreate(&self, other: &Function) -> bool {
        if self.arguments.len() != other.arguments.len() {
            return false; // Different signature entirely, not a name change
        }

        for (self_arg, other_arg) in self.arguments.iter().zip(other.arguments.iter()) {
            // Check if types/modes match but names or defaults differ
            // Use semantic comparison for defaults to handle PostgreSQL normalization
            let defaults_differ =
                !crate::util::optional_expressions_equal(&self_arg.default, &other_arg.default);
            if self_arg.data_type == other_arg.data_type
                && self_arg.mode == other_arg.mode
                && (self_arg.name != other_arg.name || defaults_differ)
            {
                return true;
            }
        }

        false
    }
}

/// Normalizes a function return type for comparison.
/// Strips double-quotes, lowercases, and normalizes type aliases within RETURNS TABLE.
/// e.g., `table("userid" uuid, "croptype" text, "totalyield" float8)`
///     → `table(userid uuid, croptype text, totalyield double precision)`
fn normalize_return_type(return_type: &str) -> String {
    let unquoted = return_type.replace('"', "");
    let lower = unquoted.to_lowercase();

    if !lower.starts_with("table(") {
        return normalize_pg_type(&lower);
    }

    debug_assert!(lower.ends_with(')'), "expected closing ')' in: {lower}");
    let inner = &lower["table(".len()..lower.len() - 1];
    let normalized_cols: Vec<String> = split_top_level_commas(inner)
        .iter()
        .map(|col| {
            let parts: Vec<&str> = col.trim().splitn(2, ' ').collect();
            if parts.len() == 2 {
                format!("{} {}", parts[0], normalize_pg_type(parts[1].trim()))
            } else {
                col.trim().to_string()
            }
        })
        .collect();

    format!("table({})", normalized_cols.join(", "))
}

/// Splits on commas that are not inside parentheses.
/// Handles types like `numeric(10,2)` without breaking on the inner comma.
fn split_top_level_commas(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0;
    for (i, ch) in s.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(&s[start..]);
    parts
}

fn normalize_sql_body(body: &str) -> String {
    let stripped = strip_dollar_quotes(body);
    stripped.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Strips dollar-quote delimiters from a function body.
/// Handles both `$$...$$` and `$tag$...$tag$` formats.
fn strip_dollar_quotes(body: &str) -> String {
    let trimmed = body.trim();

    if !trimmed.starts_with('$') {
        return body.to_string();
    }

    if let Some(tag_end) = trimmed[1..].find('$') {
        let tag = &trimmed[..=tag_end + 1];
        if let Some(content) = trimmed.strip_prefix(tag) {
            if let Some(inner) = content.strip_suffix(tag) {
                return inner.to_string();
            }
        }
    }

    body.to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FunctionArg {
    pub name: Option<String>,
    pub data_type: String,
    pub mode: ArgMode,
    pub default: Option<String>,
}

impl FunctionArg {
    /// Compares two function arguments semantically.
    /// Uses AST-based comparison for defaults to handle PostgreSQL normalization
    /// (e.g., NULL vs NULL::uuid, 0 vs 0::integer).
    pub fn semantically_equals(&self, other: &FunctionArg) -> bool {
        self.name == other.name
            && self.data_type == other.data_type
            && self.mode == other.mode
            && crate::util::optional_expressions_equal(&self.default, &other.default)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum ArgMode {
    #[default]
    In,
    Out,
    InOut,
    Variadic,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum Volatility {
    Immutable,
    Stable,
    #[default]
    Volatile,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum SecurityType {
    #[default]
    Invoker,
    Definer,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct View {
    pub name: String,
    pub schema: String,
    pub query: String,
    pub materialized: bool,
    pub owner: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub grants: Vec<Grant>,
}

impl View {
    /// Compares two views semantically using AST-based comparison.
    /// This handles PostgreSQL's normalization differences robustly:
    /// - Parentheses (structural, not textual)
    /// - 'literal' vs 'literal'::text
    /// - LIKE vs ~~ operators
    /// - Type cast case differences
    /// - Whitespace formatting
    pub fn semantically_equals(&self, other: &View) -> bool {
        self.name == other.name
            && self.schema == other.schema
            && self.materialized == other.materialized
            && views_semantically_equal(&self.query, &other.query)
    }
}

/// Mapping from virtual column name (what apps see) to physical column name in the base table.
/// Used during expand/contract migrations where temporary columns (e.g., _pgroll_new_*) are created.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnMapping {
    /// The column name that applications see through the versioned view
    pub virtual_name: String,
    /// The actual column name in the underlying base table
    pub physical_name: String,
}

/// A view in a version schema that maps to a base table.
/// Used for zero-downtime migrations where multiple schema versions coexist.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VersionView {
    /// Name of the view (typically matches base table name)
    pub name: String,
    /// Base schema where the underlying table lives (e.g., "public")
    pub base_schema: String,
    /// Version schema where this view lives (e.g., "public_v0001")
    pub version_schema: String,
    /// Name of the underlying table in the base schema
    pub base_table: String,
    /// Column mappings (virtual -> physical)
    pub column_mappings: Vec<ColumnMapping>,
    /// Use security_invoker for PG 15+ (required for RLS to work through views).
    ///
    /// When true, generates `WITH (security_invoker = true)` in CREATE VIEW.
    ///
    /// **PostgreSQL Version Compatibility:**
    /// - PG 15+: Supported, required for RLS through views
    /// - PG 14 and earlier: PostgreSQL will error on this syntax
    ///
    /// **When to set true:**
    /// - Tables have RLS policies enabled
    /// - Application needs per-user data filtering through views
    ///
    /// **When to set false:**
    /// - No RLS policies in use
    /// - Pre-PG15 compatibility required
    pub security_invoker: bool,
    /// Optional owner for the view (matches base table owner if set)
    pub owner: Option<String>,
}

/// Metadata about a version schema used for expand/contract migrations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VersionSchema {
    /// Base schema name (e.g., "public")
    pub base_schema: String,
    /// Version identifier (e.g., "v0001" or "add_email_column")
    pub version: String,
}

impl VersionSchema {
    /// Returns the full schema name (e.g., "public_v0001")
    pub fn full_name(&self) -> String {
        format!("{}_{}", self.base_schema, self.version)
    }
}

/// Helper to create a versioned schema name from base schema and version.
///
/// # Panics
/// Panics if the version identifier is empty, contains underscores, or has invalid characters.
/// Valid characters are: alphanumeric (a-z, A-Z, 0-9) and hyphens.
pub fn versioned_schema_name(base_schema: &str, version: &str) -> String {
    assert!(!version.is_empty(), "Version identifier cannot be empty");
    assert!(
        !version.contains('_'),
        "Version identifier '{version}' cannot contain underscores (use hyphens instead)"
    );
    assert!(
        version
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-'),
        "Version identifier '{version}' contains invalid characters (only alphanumeric and hyphens allowed)"
    );
    format!("{base_schema}_{version}")
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum TriggerTiming {
    Before,
    After,
    InsteadOf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
    Truncate,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum TriggerEnabled {
    #[default]
    Origin,
    Disabled,
    Replica,
    Always,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Trigger {
    pub name: String,
    pub target_schema: String,
    pub target_name: String,
    pub timing: TriggerTiming,
    pub events: Vec<TriggerEvent>,
    pub update_columns: Vec<String>,
    pub for_each_row: bool,
    pub when_clause: Option<String>,
    pub function_schema: String,
    pub function_name: String,
    pub function_args: Vec<String>,
    pub enabled: TriggerEnabled,
    pub old_table_name: Option<String>,
    pub new_table_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct SequenceOwner {
    pub table_schema: String,
    pub table_name: String,
    pub column_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Sequence {
    pub name: String,
    pub schema: String,
    pub data_type: SequenceDataType,
    pub start: Option<i64>,
    pub increment: Option<i64>,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub cycle: bool,
    pub cache: Option<i64>,
    pub owned_by: Option<SequenceOwner>,
    pub owner: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub grants: Vec<Grant>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum SequenceDataType {
    SmallInt,
    Integer,
    BigInt,
}

/// Creates a qualified name from schema and object name.
/// Used as map keys for schema-aware lookups.
pub fn qualified_name(schema: &str, name: &str) -> String {
    format!("{schema}.{name}")
}

/// Parses a qualified name into (schema, name) tuple.
/// Defaults to "public" schema if no dot separator found.
pub fn parse_qualified_name(qname: &str) -> (String, String) {
    match qname.split_once('.') {
        Some((schema, name)) => (schema.to_string(), name.to_string()),
        None => ("public".to_string(), qname.to_string()),
    }
}

impl Schema {
    pub fn new() -> Self {
        Schema {
            schemas: BTreeMap::new(),
            extensions: BTreeMap::new(),
            tables: BTreeMap::new(),
            enums: BTreeMap::new(),
            domains: BTreeMap::new(),
            functions: BTreeMap::new(),
            views: BTreeMap::new(),
            triggers: BTreeMap::new(),
            sequences: BTreeMap::new(),
            partitions: BTreeMap::new(),
            pending_policies: Vec::new(),
            pending_owners: Vec::new(),
            pending_grants: Vec::new(),
            pending_revokes: Vec::new(),
            default_privileges: Vec::new(),
        }
    }

    pub fn fingerprint(&self) -> String {
        use sha2::{Digest, Sha256};
        let json = serde_json::to_string(self).expect("Schema must serialize");
        let hash = Sha256::digest(json.as_bytes());
        hex::encode(hash)
    }

    /// Associates pending policies with their respective tables and applies pending ownership.
    /// Returns an error if a policy references a table that doesn't exist.
    pub fn finalize(&mut self) -> Result<(), String> {
        let pending = std::mem::take(&mut self.pending_policies);
        for policy in pending {
            let table_key = qualified_name(&policy.table_schema, &policy.table);
            if let Some(table) = self.tables.get_mut(&table_key) {
                table.policies.push(policy);
                table.policies.sort();
            } else {
                return Err(format!(
                    "Policy \"{}\" references non-existent table \"{}\"",
                    policy.name, table_key
                ));
            }
        }

        self.apply_pending_owners();
        self.apply_pending_grants();
        self.apply_pending_revokes();
        Ok(())
    }

    /// Associates pending policies with their respective tables.
    /// Policies referencing non-existent tables are returned.
    /// Also applies pending ownership and grants (keeping unapplied ones for cross-file resolution).
    pub fn finalize_partial(&mut self) -> Vec<Policy> {
        let pending = std::mem::take(&mut self.pending_policies);
        let mut orphaned = Vec::new();
        for policy in pending {
            let table_key = qualified_name(&policy.table_schema, &policy.table);
            if let Some(table) = self.tables.get_mut(&table_key) {
                table.policies.push(policy);
                table.policies.sort();
            } else {
                orphaned.push(policy);
            }
        }

        self.apply_pending_owners_partial();
        self.apply_pending_grants_partial();
        self.apply_pending_revokes_partial();
        orphaned
    }

    /// Applies pending ownership assignments to their respective objects.
    /// Ownership for non-existent objects is silently ignored and pending_owners is cleared.
    fn apply_pending_owners(&mut self) {
        let pending = std::mem::take(&mut self.pending_owners);
        for po in pending {
            self.apply_single_owner(&po);
        }
    }

    /// Applies pending ownership assignments, keeping unapplied ones for cross-file resolution.
    /// Used by finalize_partial() to handle same-file ownership while preserving cross-file ones.
    fn apply_pending_owners_partial(&mut self) {
        let pending = std::mem::take(&mut self.pending_owners);
        let mut unapplied = Vec::new();
        for po in pending {
            if !self.apply_single_owner(&po) {
                unapplied.push(po);
            }
        }
        self.pending_owners = unapplied;
    }

    /// Applies a single ownership assignment. Returns true if the object was found.
    fn apply_single_owner(&mut self, po: &PendingOwner) -> bool {
        match po.object_type {
            PendingOwnerObjectType::Table => {
                if let Some(table) = self.tables.get_mut(&po.object_key) {
                    table.owner = Some(po.owner.clone());
                    true
                } else if let Some(partition) = self.partitions.get_mut(&po.object_key) {
                    partition.owner = Some(po.owner.clone());
                    true
                } else {
                    false
                }
            }
            PendingOwnerObjectType::View => {
                if let Some(view) = self.views.get_mut(&po.object_key) {
                    view.owner = Some(po.owner.clone());
                    true
                } else {
                    false
                }
            }
            PendingOwnerObjectType::Sequence => {
                if let Some(seq) = self.sequences.get_mut(&po.object_key) {
                    seq.owner = Some(po.owner.clone());
                    true
                } else {
                    false
                }
            }
            PendingOwnerObjectType::Function => {
                if let Some(func) = self.functions.get_mut(&po.object_key) {
                    func.owner = Some(po.owner.clone());
                    true
                } else {
                    false
                }
            }
            PendingOwnerObjectType::Enum => {
                if let Some(enum_type) = self.enums.get_mut(&po.object_key) {
                    enum_type.owner = Some(po.owner.clone());
                    true
                } else {
                    false
                }
            }
            PendingOwnerObjectType::Domain => {
                if let Some(domain) = self.domains.get_mut(&po.object_key) {
                    domain.owner = Some(po.owner.clone());
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Applies all pending grants. Grants for non-existent objects are silently ignored.
    fn apply_pending_grants(&mut self) {
        let pending = std::mem::take(&mut self.pending_grants);
        for pg in pending {
            self.apply_single_grant(&pg);
        }
    }

    /// Applies pending grants, keeping unapplied ones for cross-file resolution.
    fn apply_pending_grants_partial(&mut self) {
        let pending = std::mem::take(&mut self.pending_grants);
        let mut unapplied = Vec::new();
        for pg in pending {
            if !self.apply_single_grant(&pg) {
                unapplied.push(pg);
            }
        }
        self.pending_grants = unapplied;
    }

    /// Resolves the grants vector for a given object type and key.
    /// Handles Table/View and Enum/Domain fallback logic.
    fn resolve_grants_mut(
        &mut self,
        object_type: PendingGrantObjectType,
        key: &str,
    ) -> Option<&mut Vec<Grant>> {
        match object_type {
            PendingGrantObjectType::Table => self
                .tables
                .get_mut(key)
                .map(|t| &mut t.grants)
                .or_else(|| self.views.get_mut(key).map(|v| &mut v.grants)),
            PendingGrantObjectType::View => self
                .views
                .get_mut(key)
                .map(|v| &mut v.grants)
                .or_else(|| self.tables.get_mut(key).map(|t| &mut t.grants)),
            PendingGrantObjectType::Sequence => self.sequences.get_mut(key).map(|s| &mut s.grants),
            PendingGrantObjectType::Function => self.functions.get_mut(key).map(|f| &mut f.grants),
            PendingGrantObjectType::Schema => self.schemas.get_mut(key).map(|s| &mut s.grants),
            PendingGrantObjectType::Enum => self
                .enums
                .get_mut(key)
                .map(|e| &mut e.grants)
                .or_else(|| self.domains.get_mut(key).map(|d| &mut d.grants)),
            PendingGrantObjectType::Domain => self
                .domains
                .get_mut(key)
                .map(|d| &mut d.grants)
                .or_else(|| self.enums.get_mut(key).map(|e| &mut e.grants)),
        }
    }

    /// Applies a single pending grant. Returns true if the object was found.
    fn apply_single_grant(&mut self, pg: &PendingGrant) -> bool {
        if let Some(grants) = self.resolve_grants_mut(pg.object_type, &pg.object_key) {
            grants.push(pg.grant.clone());
            true
        } else {
            false
        }
    }

    /// Applies all pending revokes. Revokes for non-existent objects are silently ignored.
    fn apply_pending_revokes(&mut self) {
        let pending = std::mem::take(&mut self.pending_revokes);
        for pr in pending {
            self.apply_single_revoke(&pr);
        }
    }

    /// Applies pending revokes, keeping unapplied ones for cross-file resolution.
    fn apply_pending_revokes_partial(&mut self) {
        let pending = std::mem::take(&mut self.pending_revokes);
        let mut unapplied = Vec::new();
        for pr in pending {
            if !self.apply_single_revoke(&pr) {
                unapplied.push(pr);
            }
        }
        self.pending_revokes = unapplied;
    }

    /// Applies a single pending revoke. Returns true if the object was found.
    fn apply_single_revoke(&mut self, pr: &PendingRevoke) -> bool {
        if let Some(grants) = self.resolve_grants_mut(pr.object_type, &pr.object_key) {
            revoke_from_grants(grants, &pr.grantee, &pr.privileges, pr.grant_option_for);
            true
        } else {
            false
        }
    }
}

/// Removes or modifies grants matching the given grantee and privileges.
pub fn revoke_from_grants(
    grants: &mut Vec<Grant>,
    grantee: &str,
    privileges_to_revoke: &BTreeSet<Privilege>,
    grant_option_for: bool,
) {
    grants.retain_mut(|grant| {
        if grant.grantee == grantee {
            if grant_option_for {
                grant.with_grant_option = false;
                true
            } else {
                grant
                    .privileges
                    .retain(|p| !privileges_to_revoke.contains(p));
                !grant.privileges.is_empty()
            }
        } else {
            true
        }
    });
}

impl Default for Schema {
    fn default() -> Self {
        Self::new()
    }
}

impl Function {
    pub fn signature(&self) -> String {
        let args = self
            .arguments
            .iter()
            .map(|a| normalize_pg_type(&a.data_type))
            .collect::<Vec<_>>()
            .join(", ");
        format!("{}({})", self.name, args)
    }
}

/// Normalizes PostgreSQL type aliases to their canonical forms.
/// This ensures consistent comparison between parsed SQL and introspected schemas.
pub fn normalize_pg_type(type_name: &str) -> String {
    let lower = type_name.to_lowercase();
    match lower.as_str() {
        "int" | "int4" => "integer".to_string(),
        "int8" => "bigint".to_string(),
        "int2" => "smallint".to_string(),
        "float4" => "real".to_string(),
        "float8" => "double precision".to_string(),
        "bool" => "boolean".to_string(),
        "varchar" => "character varying".to_string(),
        "timestamptz" => "timestamp with time zone".to_string(),
        "timetz" => "time with time zone".to_string(),
        _ => lower,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_schema_produces_same_fingerprint() {
        let schema1 = Schema::new();
        let schema2 = Schema::new();
        assert_eq!(schema1.fingerprint(), schema2.fingerprint());

        let mut schema3 = Schema::new();
        schema3.tables.insert(
            "users".to_string(),
            Table {
                schema: "public".to_string(),
                name: "users".to_string(),
                columns: BTreeMap::new(),
                indexes: Vec::new(),
                primary_key: None,
                foreign_keys: Vec::new(),
                check_constraints: Vec::new(),
                comment: None,
                row_level_security: false,
                policies: Vec::new(),
                partition_by: None,
                owner: None,
                grants: Vec::new(),
            },
        );

        let mut schema4 = Schema::new();
        schema4.tables.insert(
            "users".to_string(),
            Table {
                schema: "public".to_string(),
                name: "users".to_string(),
                columns: BTreeMap::new(),
                indexes: Vec::new(),
                primary_key: None,
                foreign_keys: Vec::new(),
                check_constraints: Vec::new(),
                comment: None,
                row_level_security: false,
                policies: Vec::new(),
                partition_by: None,
                owner: None,
                grants: Vec::new(),
            },
        );

        assert_eq!(schema3.fingerprint(), schema4.fingerprint());
        assert_ne!(schema1.fingerprint(), schema3.fingerprint());
    }

    #[test]
    fn strip_dollar_quotes_simple() {
        assert_eq!(strip_dollar_quotes("$$BEGIN END;$$"), "BEGIN END;");
        assert_eq!(strip_dollar_quotes("$$ BEGIN END; $$"), " BEGIN END; ");
    }

    #[test]
    fn strip_dollar_quotes_with_tag() {
        assert_eq!(strip_dollar_quotes("$body$SELECT 1$body$"), "SELECT 1");
    }

    #[test]
    fn strip_dollar_quotes_no_quotes() {
        assert_eq!(strip_dollar_quotes("BEGIN END;"), "BEGIN END;");
    }

    #[test]
    fn strip_dollar_quotes_with_whitespace() {
        let body = "  $$\n    BEGIN\n        RETURN 42;\n    END;\n$$  ";
        let expected = "\n    BEGIN\n        RETURN 42;\n    END;\n";
        assert_eq!(strip_dollar_quotes(body), expected);
    }

    #[test]
    fn function_semantically_equals_ignores_whitespace() {
        let func1 = Function {
            name: "test".to_string(),
            schema: "public".to_string(),
            arguments: vec![],
            return_type: "INTEGER".to_string(),
            language: "plpgsql".to_string(),
            body: "BEGIN\n    RETURN 42;\nEND;".to_string(),
            volatility: Volatility::Volatile,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
        };

        let func2 = Function {
            name: "test".to_string(),
            schema: "public".to_string(),
            arguments: vec![],
            return_type: "INTEGER".to_string(),
            language: "plpgsql".to_string(),
            body: "BEGIN RETURN 42; END;".to_string(),
            volatility: Volatility::Volatile,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
        };

        assert!(func1.semantically_equals(&func2));
    }

    #[test]
    fn function_semantically_equals_with_dollar_quotes() {
        let parsed_body = Function {
            name: "test".to_string(),
            schema: "public".to_string(),
            arguments: vec![],
            return_type: "INTEGER".to_string(),
            language: "plpgsql".to_string(),
            body: "$$BEGIN RETURN 42; END;$$".to_string(),
            volatility: Volatility::Volatile,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
        };

        let introspected_body = Function {
            name: "test".to_string(),
            schema: "public".to_string(),
            arguments: vec![],
            return_type: "INTEGER".to_string(),
            language: "plpgsql".to_string(),
            body: "BEGIN RETURN 42; END;".to_string(),
            volatility: Volatility::Volatile,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
        };

        assert!(parsed_body.semantically_equals(&introspected_body));
    }

    #[test]
    fn qualified_name_combines_schema_and_name() {
        assert_eq!(qualified_name("public", "users"), "public.users");
        assert_eq!(qualified_name("auth", "accounts"), "auth.accounts");
    }

    #[test]
    fn parse_qualified_name_splits_correctly() {
        assert_eq!(
            parse_qualified_name("public.users"),
            ("public".to_string(), "users".to_string())
        );
        assert_eq!(
            parse_qualified_name("auth.accounts"),
            ("auth".to_string(), "accounts".to_string())
        );
    }

    #[test]
    fn parse_qualified_name_defaults_to_public() {
        assert_eq!(
            parse_qualified_name("users"),
            ("public".to_string(), "users".to_string())
        );
    }

    #[test]
    fn table_has_schema_field() {
        let table = Table {
            schema: "auth".to_string(),
            name: "users".to_string(),
            columns: BTreeMap::new(),
            indexes: Vec::new(),
            primary_key: None,
            foreign_keys: Vec::new(),
            check_constraints: Vec::new(),
            comment: None,
            row_level_security: false,
            policies: Vec::new(),
            partition_by: None,
            owner: None,
            grants: Vec::new(),
        };
        assert_eq!(table.schema, "auth");
    }

    #[test]
    fn enum_type_has_schema_field() {
        let enum_type = EnumType {
            schema: "auth".to_string(),
            name: "role".to_string(),
            values: vec!["admin".to_string(), "user".to_string()],

            owner: None,
            grants: Vec::new(),
        };
        assert_eq!(enum_type.schema, "auth");
    }

    #[test]
    fn foreign_key_has_referenced_schema() {
        let fk = ForeignKey {
            name: "fk_user".to_string(),
            columns: vec!["user_id".to_string()],
            referenced_schema: "auth".to_string(),
            referenced_table: "users".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: ReferentialAction::Cascade,
            on_update: ReferentialAction::NoAction,
        };
        assert_eq!(fk.referenced_schema, "auth");
    }

    #[test]
    fn policy_has_table_schema() {
        let policy = Policy {
            name: "user_isolation".to_string(),
            table_schema: "auth".to_string(),
            table: "users".to_string(),
            command: PolicyCommand::All,
            roles: vec!["authenticated".to_string()],
            using_expr: Some("user_id = current_user_id()".to_string()),
            check_expr: None,
        };
        assert_eq!(policy.table_schema, "auth");
    }

    #[test]
    fn trigger_struct_captures_all_fields() {
        let trigger = Trigger {
            name: "audit_log".to_string(),
            target_schema: "public".to_string(),
            target_name: "users".to_string(),
            timing: TriggerTiming::After,
            events: vec![TriggerEvent::Insert, TriggerEvent::Update],
            update_columns: vec!["email".to_string(), "name".to_string()],
            for_each_row: true,
            when_clause: Some("NEW.updated_at IS NOT NULL".to_string()),
            function_schema: "public".to_string(),
            function_name: "audit_trigger_fn".to_string(),
            function_args: vec![],
            enabled: TriggerEnabled::Origin,
            old_table_name: None,
            new_table_name: None,
        };

        assert_eq!(trigger.name, "audit_log");
        assert_eq!(trigger.target_schema, "public");
        assert_eq!(trigger.timing, TriggerTiming::After);
        assert_eq!(trigger.events.len(), 2);
        assert_eq!(trigger.update_columns, vec!["email", "name"]);
        assert!(trigger.for_each_row);
    }

    #[test]
    fn schema_has_triggers_field() {
        let mut schema = Schema::new();
        let trigger = Trigger {
            name: "audit_log".to_string(),
            target_schema: "public".to_string(),
            target_name: "users".to_string(),
            timing: TriggerTiming::Before,
            events: vec![TriggerEvent::Delete],
            update_columns: vec![],
            for_each_row: true,
            when_clause: None,
            function_schema: "public".to_string(),
            function_name: "prevent_delete".to_string(),
            function_args: vec![],
            enabled: TriggerEnabled::Origin,
            old_table_name: None,
            new_table_name: None,
        };
        schema
            .triggers
            .insert("public.users.audit_log".to_string(), trigger);
        assert_eq!(schema.triggers.len(), 1);
    }

    #[test]
    fn fingerprint_differs_by_schema() {
        let mut schema1 = Schema::new();
        schema1.tables.insert(
            "public.users".to_string(),
            Table {
                schema: "public".to_string(),
                name: "users".to_string(),
                columns: BTreeMap::new(),
                indexes: Vec::new(),
                primary_key: None,
                foreign_keys: Vec::new(),
                check_constraints: Vec::new(),
                comment: None,
                row_level_security: false,
                policies: Vec::new(),
                partition_by: None,
                owner: None,
                grants: Vec::new(),
            },
        );

        let mut schema2 = Schema::new();
        schema2.tables.insert(
            "auth.users".to_string(),
            Table {
                schema: "auth".to_string(),
                name: "users".to_string(),
                columns: BTreeMap::new(),
                indexes: Vec::new(),
                primary_key: None,
                foreign_keys: Vec::new(),
                check_constraints: Vec::new(),
                comment: None,
                row_level_security: false,
                policies: Vec::new(),
                partition_by: None,
                owner: None,
                grants: Vec::new(),
            },
        );

        assert_ne!(schema1.fingerprint(), schema2.fingerprint());
    }

    #[test]
    fn sequence_serialization_roundtrip() {
        let sequence = Sequence {
            name: "user_id_seq".to_string(),
            schema: "public".to_string(),
            data_type: SequenceDataType::BigInt,
            start: Some(1),
            increment: Some(1),
            min_value: Some(1),
            max_value: Some(9223372036854775807),
            cycle: false,
            cache: Some(1),
            owned_by: Some(SequenceOwner {
                table_schema: "public".to_string(),
                table_name: "users".to_string(),
                column_name: "id".to_string(),
            }),
            owner: None,
            grants: Vec::new(),
        };

        let json = serde_json::to_string(&sequence).expect("Failed to serialize");
        let deserialized: Sequence = serde_json::from_str(&json).expect("Failed to deserialize");
        assert_eq!(sequence, deserialized);
    }

    #[test]
    fn schema_new_has_empty_sequences() {
        let schema = Schema::new();
        assert_eq!(schema.sequences.len(), 0);
    }

    #[test]
    fn sequence_data_type_serialization() {
        let small_int = SequenceDataType::SmallInt;
        let integer = SequenceDataType::Integer;
        let big_int = SequenceDataType::BigInt;

        let small_json = serde_json::to_string(&small_int).expect("Failed to serialize SmallInt");
        let int_json = serde_json::to_string(&integer).expect("Failed to serialize Integer");
        let big_json = serde_json::to_string(&big_int).expect("Failed to serialize BigInt");

        let small_deserialized: SequenceDataType =
            serde_json::from_str(&small_json).expect("Failed to deserialize SmallInt");
        let int_deserialized: SequenceDataType =
            serde_json::from_str(&int_json).expect("Failed to deserialize Integer");
        let big_deserialized: SequenceDataType =
            serde_json::from_str(&big_json).expect("Failed to deserialize BigInt");

        assert_eq!(small_int, small_deserialized);
        assert_eq!(integer, int_deserialized);
        assert_eq!(big_int, big_deserialized);
    }

    #[test]
    fn function_signature_is_case_insensitive() {
        let func_uppercase = Function {
            schema: "public".to_string(),
            name: "my_func".to_string(),
            arguments: vec![FunctionArg {
                name: Some("user_id".to_string()),
                data_type: "UUID".to_string(),
                mode: ArgMode::In,
                default: None,
            }],
            return_type: "void".to_string(),
            language: "sql".to_string(),
            body: "SELECT 1".to_string(),
            volatility: Volatility::Volatile,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
        };

        let func_lowercase = Function {
            schema: "public".to_string(),
            name: "my_func".to_string(),
            arguments: vec![FunctionArg {
                name: Some("user_id".to_string()),
                data_type: "uuid".to_string(),
                mode: ArgMode::In,
                default: None,
            }],
            return_type: "void".to_string(),
            language: "sql".to_string(),
            body: "SELECT 1".to_string(),
            volatility: Volatility::Volatile,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
        };

        assert_eq!(
            func_uppercase.signature(),
            func_lowercase.signature(),
            "Function signatures should match regardless of type case"
        );
        // Both should produce lowercase signature
        assert_eq!(func_uppercase.signature(), "my_func(uuid)");
    }

    #[test]
    fn function_signature_normalizes_type_aliases() {
        // PostgreSQL normalizes int → integer, int8 → bigint, etc.
        // Signatures should match regardless of which alias is used
        let func_int = Function {
            schema: "public".to_string(),
            name: "add_numbers".to_string(),
            arguments: vec![
                FunctionArg {
                    name: Some("a".to_string()),
                    data_type: "int".to_string(),
                    mode: ArgMode::In,
                    default: None,
                },
                FunctionArg {
                    name: Some("b".to_string()),
                    data_type: "int".to_string(),
                    mode: ArgMode::In,
                    default: None,
                },
            ],
            return_type: "int".to_string(),
            language: "sql".to_string(),
            body: "SELECT a + b".to_string(),
            volatility: Volatility::Volatile,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
        };

        let func_integer = Function {
            schema: "public".to_string(),
            name: "add_numbers".to_string(),
            arguments: vec![
                FunctionArg {
                    name: Some("a".to_string()),
                    data_type: "integer".to_string(),
                    mode: ArgMode::In,
                    default: None,
                },
                FunctionArg {
                    name: Some("b".to_string()),
                    data_type: "integer".to_string(),
                    mode: ArgMode::In,
                    default: None,
                },
            ],
            return_type: "integer".to_string(),
            language: "sql".to_string(),
            body: "SELECT a + b".to_string(),
            volatility: Volatility::Volatile,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
        };

        assert_eq!(
            func_int.signature(),
            func_integer.signature(),
            "int and integer should produce the same signature"
        );
        // Canonical form should be 'integer'
        assert_eq!(func_int.signature(), "add_numbers(integer, integer)");
    }

    #[test]
    fn normalize_pg_type_handles_common_aliases() {
        // Test the normalization function directly
        assert_eq!(normalize_pg_type("int"), "integer");
        assert_eq!(normalize_pg_type("int4"), "integer");
        assert_eq!(normalize_pg_type("int8"), "bigint");
        assert_eq!(normalize_pg_type("int2"), "smallint");
        assert_eq!(normalize_pg_type("float4"), "real");
        assert_eq!(normalize_pg_type("float8"), "double precision");
        assert_eq!(normalize_pg_type("bool"), "boolean");
        assert_eq!(normalize_pg_type("varchar"), "character varying");
        assert_eq!(normalize_pg_type("timestamptz"), "timestamp with time zone");
        assert_eq!(normalize_pg_type("timetz"), "time with time zone");
        // Already canonical types should remain unchanged
        assert_eq!(normalize_pg_type("integer"), "integer");
        assert_eq!(normalize_pg_type("text"), "text");
        assert_eq!(normalize_pg_type("uuid"), "uuid");
    }

    #[test]
    fn view_semantically_equals_with_text_cast_difference() {
        let parsed_view = View {
            name: "test_view".to_string(),
            schema: "public".to_string(),
            query: "SELECT 'supplier' AS type FROM users".to_string(),
            materialized: false,
            owner: None,
            grants: Vec::new(),
        };

        let introspected_view = View {
            name: "test_view".to_string(),
            schema: "public".to_string(),
            query: "SELECT 'supplier'::text AS type FROM users".to_string(),
            materialized: false,
            owner: None,
            grants: Vec::new(),
        };

        assert!(parsed_view.semantically_equals(&introspected_view));
    }

    #[test]
    fn view_semantically_equals_with_like_operator_difference() {
        let parsed_view = View {
            name: "test_view".to_string(),
            schema: "public".to_string(),
            query: "SELECT * FROM users WHERE name LIKE 'test%'".to_string(),
            materialized: false,
            owner: None,
            grants: Vec::new(),
        };

        let introspected_view = View {
            name: "test_view".to_string(),
            schema: "public".to_string(),
            query: "SELECT * FROM users WHERE name ~~ 'test%'::text".to_string(),
            materialized: false,
            owner: None,
            grants: Vec::new(),
        };

        assert!(parsed_view.semantically_equals(&introspected_view));
    }

    #[test]
    fn view_semantically_equals_with_type_cast_case_difference() {
        let parsed_view = View {
            name: "test_view".to_string(),
            schema: "public".to_string(),
            query: "SELECT id::TEXT FROM users".to_string(),
            materialized: false,
            owner: None,
            grants: Vec::new(),
        };

        let introspected_view = View {
            name: "test_view".to_string(),
            schema: "public".to_string(),
            query: "SELECT id::text FROM users".to_string(),
            materialized: false,
            owner: None,
            grants: Vec::new(),
        };

        assert!(parsed_view.semantically_equals(&introspected_view));
    }

    #[test]
    fn view_semantically_equals_with_whitespace_and_newline_differences() {
        let parsed_view = View {
            name: "test_view".to_string(),
            schema: "public".to_string(),
            query: "SELECT id, name FROM users WHERE active = true".to_string(),
            materialized: false,
            owner: None,
            grants: Vec::new(),
        };

        let introspected_view = View {
            name: "test_view".to_string(),
            schema: "public".to_string(),
            query: "SELECT  id,  name  FROM  users  WHERE  active  =  true".to_string(),
            materialized: false,
            owner: None,
            grants: Vec::new(),
        };

        assert!(parsed_view.semantically_equals(&introspected_view));
    }

    #[test]
    fn view_semantically_equals_with_paren_whitespace_differences() {
        let parsed_view = View {
            name: "test_view".to_string(),
            schema: "public".to_string(),
            query: "SELECT * FROM (SELECT id FROM users)".to_string(),
            materialized: false,
            owner: None,
            grants: Vec::new(),
        };

        let introspected_view = View {
            name: "test_view".to_string(),
            schema: "public".to_string(),
            query: "SELECT * FROM ( SELECT id FROM users )".to_string(),
            materialized: false,
            owner: None,
            grants: Vec::new(),
        };

        assert!(parsed_view.semantically_equals(&introspected_view));
    }

    #[test]
    fn view_semantically_not_equals_with_different_query() {
        let view1 = View {
            name: "test_view".to_string(),
            schema: "public".to_string(),
            query: "SELECT id FROM users".to_string(),
            materialized: false,
            owner: None,
            grants: Vec::new(),
        };

        let view2 = View {
            name: "test_view".to_string(),
            schema: "public".to_string(),
            query: "SELECT id FROM accounts".to_string(),
            materialized: false,
            owner: None,
            grants: Vec::new(),
        };

        assert!(!view1.semantically_equals(&view2));
    }

    #[test]
    fn vector_type_without_dimension() {
        let vector_type = PgType::Vector(None);
        match vector_type {
            PgType::Vector(None) => (),
            _ => panic!("Expected Vector(None)"),
        }
    }

    #[test]
    fn vector_type_with_dimension() {
        let vector_type = PgType::Vector(Some(1536));
        match vector_type {
            PgType::Vector(Some(1536)) => (),
            _ => panic!("Expected Vector(Some(1536))"),
        }
    }

    #[test]
    fn function_with_config_params() {
        let func = Function {
            name: "test".to_string(),
            schema: "public".to_string(),
            arguments: vec![],
            return_type: "void".to_string(),
            language: "sql".to_string(),
            body: "SELECT 1".to_string(),
            volatility: Volatility::Volatile,
            security: SecurityType::Invoker,
            config_params: vec![("search_path".to_string(), "public".to_string())],
            owner: None,
            grants: Vec::new(),
        };
        assert_eq!(func.config_params.len(), 1);
        assert_eq!(func.config_params[0].0, "search_path");
    }

    #[test]
    fn table_has_owner_field() {
        let table = Table {
            schema: "public".to_string(),
            name: "users".to_string(),
            columns: BTreeMap::new(),
            indexes: Vec::new(),
            primary_key: None,
            foreign_keys: Vec::new(),
            check_constraints: Vec::new(),
            comment: None,
            row_level_security: false,
            policies: Vec::new(),
            partition_by: None,
            owner: Some("postgres".to_string()),
            grants: Vec::new(),
        };
        assert_eq!(table.owner, Some("postgres".to_string()));
    }

    #[test]
    fn view_has_owner_field() {
        let view = View {
            name: "user_view".to_string(),
            schema: "public".to_string(),
            query: "SELECT * FROM users".to_string(),
            materialized: false,
            owner: Some("postgres".to_string()),
            grants: Vec::new(),
        };
        assert_eq!(view.owner, Some("postgres".to_string()));
    }

    #[test]
    fn sequence_has_owner_field() {
        let sequence = Sequence {
            name: "user_id_seq".to_string(),
            schema: "public".to_string(),
            data_type: SequenceDataType::BigInt,
            start: Some(1),
            increment: Some(1),
            min_value: Some(1),
            max_value: Some(9223372036854775807),
            cycle: false,
            cache: Some(1),
            owned_by: None,
            owner: Some("postgres".to_string()),
            grants: Vec::new(),
        };
        assert_eq!(sequence.owner, Some("postgres".to_string()));
    }

    #[test]
    fn enum_type_has_owner_field() {
        let enum_type = EnumType {
            schema: "public".to_string(),
            name: "role".to_string(),
            values: vec!["admin".to_string(), "user".to_string()],
            owner: Some("postgres".to_string()),
            grants: Vec::new(),
        };
        assert_eq!(enum_type.owner, Some("postgres".to_string()));
    }

    #[test]
    fn domain_has_owner_field() {
        let domain = Domain {
            schema: "public".to_string(),
            name: "email".to_string(),
            data_type: PgType::Text,
            default: None,
            not_null: false,
            collation: None,
            check_constraints: Vec::new(),
            owner: Some("postgres".to_string()),
            grants: Vec::new(),
        };
        assert_eq!(domain.owner, Some("postgres".to_string()));
    }

    #[test]
    fn partition_has_owner_field() {
        let partition = Partition {
            schema: "public".to_string(),
            name: "users_2024".to_string(),
            parent_schema: "public".to_string(),
            parent_name: "users".to_string(),
            bound: PartitionBound::Default,
            indexes: Vec::new(),
            check_constraints: Vec::new(),
            owner: Some("postgres".to_string()),
        };
        assert_eq!(partition.owner, Some("postgres".to_string()));
    }

    #[test]
    fn owner_field_included_in_equality() {
        let table1 = Table {
            schema: "public".to_string(),
            name: "users".to_string(),
            columns: BTreeMap::new(),
            indexes: Vec::new(),
            primary_key: None,
            foreign_keys: Vec::new(),
            check_constraints: Vec::new(),
            comment: None,
            row_level_security: false,
            policies: Vec::new(),
            partition_by: None,
            owner: Some("postgres".to_string()),
            grants: Vec::new(),
        };

        let table2 = Table {
            schema: "public".to_string(),
            name: "users".to_string(),
            columns: BTreeMap::new(),
            indexes: Vec::new(),
            primary_key: None,
            foreign_keys: Vec::new(),
            check_constraints: Vec::new(),
            comment: None,
            row_level_security: false,
            policies: Vec::new(),
            partition_by: None,
            owner: Some("admin".to_string()),
            grants: Vec::new(),
        };

        assert_ne!(table1, table2);
    }

    #[test]
    fn owner_field_serialization() {
        let table = Table {
            schema: "public".to_string(),
            name: "users".to_string(),
            columns: BTreeMap::new(),
            indexes: Vec::new(),
            primary_key: None,
            foreign_keys: Vec::new(),
            check_constraints: Vec::new(),
            comment: None,
            row_level_security: false,
            policies: Vec::new(),
            partition_by: None,
            owner: Some("postgres".to_string()),
            grants: Vec::new(),
        };

        let json = serde_json::to_string(&table).expect("Failed to serialize");
        let deserialized: Table = serde_json::from_str(&json).expect("Failed to deserialize");
        assert_eq!(table, deserialized);
        assert_eq!(deserialized.owner, Some("postgres".to_string()));
    }

    #[test]
    fn fingerprint_differs_when_owner_differs() {
        let mut schema1 = Schema::new();
        schema1.tables.insert(
            "public.users".to_string(),
            Table {
                schema: "public".to_string(),
                name: "users".to_string(),
                columns: BTreeMap::new(),
                indexes: Vec::new(),
                primary_key: None,
                foreign_keys: Vec::new(),
                check_constraints: Vec::new(),
                comment: None,
                row_level_security: false,
                policies: Vec::new(),
                partition_by: None,
                owner: Some("postgres".to_string()),
                grants: Vec::new(),
            },
        );

        let mut schema2 = Schema::new();
        schema2.tables.insert(
            "public.users".to_string(),
            Table {
                schema: "public".to_string(),
                name: "users".to_string(),
                columns: BTreeMap::new(),
                indexes: Vec::new(),
                primary_key: None,
                foreign_keys: Vec::new(),
                check_constraints: Vec::new(),
                comment: None,
                row_level_security: false,
                policies: Vec::new(),
                partition_by: None,
                owner: None,
                grants: Vec::new(),
            },
        );

        assert_ne!(schema1.fingerprint(), schema2.fingerprint());
    }

    #[test]
    fn table_with_grants_serialization() {
        use std::collections::BTreeSet;

        let mut privileges = BTreeSet::new();
        privileges.insert(Privilege::Select);
        privileges.insert(Privilege::Insert);

        let grant = Grant {
            grantee: "app_user".to_string(),
            privileges,
            with_grant_option: false,
        };

        let table = Table {
            schema: "public".to_string(),
            name: "users".to_string(),
            columns: BTreeMap::new(),
            indexes: Vec::new(),
            primary_key: None,
            foreign_keys: Vec::new(),
            check_constraints: Vec::new(),
            comment: None,
            row_level_security: false,
            policies: Vec::new(),
            partition_by: None,
            owner: Some("postgres".to_string()),
            grants: vec![grant],
        };

        let json = serde_json::to_string(&table).expect("Failed to serialize");
        let deserialized: Table = serde_json::from_str(&json).expect("Failed to deserialize");
        assert_eq!(table, deserialized);
        assert_eq!(deserialized.grants.len(), 1);
        assert_eq!(deserialized.grants[0].grantee, "app_user");
    }

    #[test]
    fn empty_grants_serialization_omitted() {
        let table = Table {
            schema: "public".to_string(),
            name: "users".to_string(),
            columns: BTreeMap::new(),
            indexes: Vec::new(),
            primary_key: None,
            foreign_keys: Vec::new(),
            check_constraints: Vec::new(),
            comment: None,
            row_level_security: false,
            policies: Vec::new(),
            partition_by: None,
            owner: Some("postgres".to_string()),
            grants: Vec::new(),
        };

        let json = serde_json::to_string(&table).expect("Failed to serialize");
        assert!(
            !json.contains("grants"),
            "Empty grants should not be serialized"
        );
    }

    #[test]
    fn grant_privilege_ordering() {
        use std::collections::BTreeSet;

        let mut privileges = BTreeSet::new();
        privileges.insert(Privilege::Delete);
        privileges.insert(Privilege::Select);
        privileges.insert(Privilege::Insert);
        privileges.insert(Privilege::Update);

        let items: Vec<_> = privileges.iter().collect();
        // BTreeSet should maintain sorted order
        assert!(items.windows(2).all(|w| w[0] <= w[1]));
    }

    #[test]
    fn view_with_grants() {
        use std::collections::BTreeSet;

        let mut privileges = BTreeSet::new();
        privileges.insert(Privilege::Select);

        let grant = Grant {
            grantee: "PUBLIC".to_string(),
            privileges,
            with_grant_option: false,
        };

        let view = View {
            name: "user_view".to_string(),
            schema: "public".to_string(),
            query: "SELECT id, name FROM users".to_string(),
            materialized: false,
            owner: Some("postgres".to_string()),
            grants: vec![grant],
        };

        assert_eq!(view.grants.len(), 1);
        assert_eq!(view.grants[0].grantee, "PUBLIC");
    }

    #[test]
    fn function_with_execute_grant() {
        use std::collections::BTreeSet;

        let mut privileges = BTreeSet::new();
        privileges.insert(Privilege::Execute);

        let grant = Grant {
            grantee: "app_user".to_string(),
            privileges,
            with_grant_option: true,
        };

        let func = Function {
            name: "calculate".to_string(),
            schema: "public".to_string(),
            arguments: vec![],
            return_type: "integer".to_string(),
            language: "sql".to_string(),
            body: "SELECT 42".to_string(),
            volatility: Volatility::Immutable,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: Some("postgres".to_string()),
            grants: vec![grant],
        };

        assert_eq!(func.grants.len(), 1);
        assert!(func.grants[0].with_grant_option);
    }

    #[test]
    fn sequence_with_usage_grant() {
        use std::collections::BTreeSet;

        let mut privileges = BTreeSet::new();
        privileges.insert(Privilege::Usage);

        let grant = Grant {
            grantee: "app_user".to_string(),
            privileges,
            with_grant_option: false,
        };

        let sequence = Sequence {
            name: "user_id_seq".to_string(),
            schema: "public".to_string(),
            data_type: SequenceDataType::BigInt,
            start: Some(1),
            increment: Some(1),
            min_value: Some(1),
            max_value: Some(9223372036854775807),
            cycle: false,
            cache: Some(1),
            owned_by: None,
            owner: Some("postgres".to_string()),
            grants: vec![grant],
        };

        assert_eq!(sequence.grants.len(), 1);
        assert!(sequence.grants[0].privileges.contains(&Privilege::Usage));
    }

    #[test]
    fn column_mapping_creation() {
        let mapping = ColumnMapping {
            virtual_name: "description".to_string(),
            physical_name: "_pgroll_new_description".to_string(),
        };
        assert_eq!(mapping.virtual_name, "description");
        assert_eq!(mapping.physical_name, "_pgroll_new_description");
    }

    #[test]
    fn column_mapping_identity() {
        let mapping = ColumnMapping {
            virtual_name: "id".to_string(),
            physical_name: "id".to_string(),
        };
        assert_eq!(mapping.virtual_name, mapping.physical_name);
    }

    #[test]
    fn version_view_with_mappings() {
        let view = VersionView {
            name: "users".to_string(),
            base_schema: "public".to_string(),
            version_schema: "public_v0001".to_string(),
            base_table: "users".to_string(),
            column_mappings: vec![
                ColumnMapping {
                    virtual_name: "id".to_string(),
                    physical_name: "id".to_string(),
                },
                ColumnMapping {
                    virtual_name: "description".to_string(),
                    physical_name: "_pgroll_new_description".to_string(),
                },
            ],
            security_invoker: true,
            owner: None,
        };
        assert_eq!(view.name, "users");
        assert_eq!(view.version_schema, "public_v0001");
        assert_eq!(view.column_mappings.len(), 2);
        assert!(view.security_invoker);
    }

    #[test]
    fn version_schema_full_name() {
        let schema = VersionSchema {
            base_schema: "public".to_string(),
            version: "v0001".to_string(),
        };
        assert_eq!(schema.full_name(), "public_v0001");
    }

    #[test]
    fn version_schema_full_name_with_descriptive_version() {
        let schema = VersionSchema {
            base_schema: "public".to_string(),
            version: "add_email_column".to_string(),
        };
        assert_eq!(schema.full_name(), "public_add_email_column");
    }

    #[test]
    fn versioned_schema_name_helper() {
        assert_eq!(versioned_schema_name("public", "v0001"), "public_v0001");
        assert_eq!(versioned_schema_name("auth", "v0002"), "auth_v0002");
    }

    #[test]
    fn versioned_schema_name_accepts_hyphens() {
        assert_eq!(
            versioned_schema_name("public", "add-email-column"),
            "public_add-email-column"
        );
    }

    #[test]
    #[should_panic(expected = "Version identifier cannot be empty")]
    fn versioned_schema_name_rejects_empty_version() {
        versioned_schema_name("public", "");
    }

    #[test]
    #[should_panic(expected = "cannot contain underscores")]
    fn versioned_schema_name_rejects_underscores() {
        versioned_schema_name("public", "v_001");
    }

    #[test]
    #[should_panic(expected = "contains invalid characters")]
    fn versioned_schema_name_rejects_special_chars() {
        versioned_schema_name("public", "v001!");
    }

    #[test]
    fn version_view_serialization_roundtrip() {
        let view = VersionView {
            name: "users".to_string(),
            base_schema: "public".to_string(),
            version_schema: "public_v0001".to_string(),
            base_table: "users".to_string(),
            column_mappings: vec![ColumnMapping {
                virtual_name: "id".to_string(),
                physical_name: "id".to_string(),
            }],
            security_invoker: true,
            owner: None,
        };

        let json = serde_json::to_string(&view).expect("Failed to serialize");
        let deserialized: VersionView = serde_json::from_str(&json).expect("Failed to deserialize");
        assert_eq!(view, deserialized);
    }

    #[test]
    fn default_privilege_ordering() {
        use std::collections::BTreeSet;

        let mut privileges = BTreeSet::new();
        privileges.insert(Privilege::Select);

        let dp1 = DefaultPrivilege {
            target_role: "admin".to_string(),
            schema: Some("public".to_string()),
            object_type: DefaultPrivilegeObjectType::Tables,
            grantee: "app_user".to_string(),
            privileges: privileges.clone(),
            with_grant_option: false,
        };

        let dp2 = DefaultPrivilege {
            target_role: "admin".to_string(),
            schema: None,
            object_type: DefaultPrivilegeObjectType::Tables,
            grantee: "app_user".to_string(),
            privileges: privileges.clone(),
            with_grant_option: false,
        };

        // Global (None) should sort before per-schema
        assert!(dp2 < dp1);
    }
}

#[test]
fn function_semantically_equals_handles_null_type_cast() {
    use crate::model::{ArgMode, Function, FunctionArg, SecurityType, Volatility};

    // Function from schema file (without cast)
    let from_file = Function {
        name: "test_func".to_string(),
        schema: "public".to_string(),
        arguments: vec![FunctionArg {
            name: Some("p_id".to_string()),
            data_type: "uuid".to_string(),
            mode: ArgMode::In,
            default: Some("NULL".to_string()),
        }],
        return_type: "void".to_string(),
        language: "plpgsql".to_string(),
        body: "BEGIN END;".to_string(),
        volatility: Volatility::Volatile,
        security: SecurityType::Invoker,
        config_params: vec![],
        owner: None,
        grants: vec![],
    };

    // Function from database (with cast added by PostgreSQL)
    let from_db = Function {
        name: "test_func".to_string(),
        schema: "public".to_string(),
        arguments: vec![FunctionArg {
            name: Some("p_id".to_string()),
            data_type: "uuid".to_string(),
            mode: ArgMode::In,
            default: Some("NULL::uuid".to_string()),
        }],
        return_type: "void".to_string(),
        language: "plpgsql".to_string(),
        body: "BEGIN END;".to_string(),
        volatility: Volatility::Volatile,
        security: SecurityType::Invoker,
        config_params: vec![],
        owner: None,
        grants: vec![],
    };

    // This currently FAILS because defaults are compared as strings
    // Bug: "NULL" != "NULL::uuid"
    assert!(
        from_file.semantically_equals(&from_db),
        "Function defaults NULL vs NULL::uuid should be semantically equal"
    );
}

// --- #58: RETURNS TABLE column name casing ---

#[test]
fn function_returns_table_case_insensitive_comparison() {
    // pg_get_function_result lowercases column names and drops quotes in RETURNS TABLE.
    // After normalize_pg_type:
    //   Schema: table("userid" uuid, "firstname" text) — quotes preserved by sqlparser
    //   DB:     table(userid uuid, firstname text)     — no quotes from pg_get_function_result
    let from_schema = Function {
        name: "example_func".to_string(),
        schema: "public".to_string(),
        arguments: vec![],
        return_type: "table(\"userid\" uuid, \"firstname\" text)".to_string(),
        language: "plpgsql".to_string(),
        body: "BEGIN RETURN QUERY SELECT gen_random_uuid(), 'test'::text; END;".to_string(),
        volatility: Volatility::Stable,
        security: SecurityType::Invoker,
        config_params: vec![],
        owner: None,
        grants: vec![],
    };

    let from_db = Function {
        name: "example_func".to_string(),
        schema: "public".to_string(),
        arguments: vec![],
        return_type: "table(userid uuid, firstname text)".to_string(),
        language: "plpgsql".to_string(),
        body: "BEGIN RETURN QUERY SELECT gen_random_uuid(), 'test'::text; END;".to_string(),
        volatility: Volatility::Stable,
        security: SecurityType::Invoker,
        config_params: vec![],
        owner: None,
        grants: vec![],
    };

    assert!(
        from_schema.semantically_equals(&from_db),
        "RETURNS TABLE column names with/without quotes should be equal after normalization"
    );
}

#[test]
fn function_returns_table_type_alias_normalization() {
    // pg_get_function_result returns canonical type names (double precision, integer)
    // but schema SQL uses aliases (float8, int)
    let from_schema = Function {
        name: "example_func".to_string(),
        schema: "public".to_string(),
        arguments: vec![],
        return_type: "table(id uuid, croptype text, totalyield float8, farmercount int)".to_string(),
        language: "plpgsql".to_string(),
        body: "BEGIN END;".to_string(),
        volatility: Volatility::Stable,
        security: SecurityType::Invoker,
        config_params: vec![],
        owner: None,
        grants: vec![],
    };

    let from_db = Function {
        name: "example_func".to_string(),
        schema: "public".to_string(),
        arguments: vec![],
        return_type: "TABLE(id uuid, croptype text, totalyield double precision, farmercount integer)".to_string(),
        language: "plpgsql".to_string(),
        body: "BEGIN END;".to_string(),
        volatility: Volatility::Stable,
        security: SecurityType::Invoker,
        config_params: vec![],
        owner: None,
        grants: vec![],
    };

    assert!(
        from_schema.semantically_equals(&from_db),
        "RETURNS TABLE type aliases should be normalized for comparison"
    );
}

#[test]
fn function_returns_table_parameterized_types() {
    let from_schema = Function {
        name: "example_func".to_string(),
        schema: "public".to_string(),
        arguments: vec![],
        return_type: "table(amount numeric(10,2), name text)".to_string(),
        language: "plpgsql".to_string(),
        body: "BEGIN END;".to_string(),
        volatility: Volatility::Stable,
        security: SecurityType::Invoker,
        config_params: vec![],
        owner: None,
        grants: vec![],
    };

    let from_db = Function {
        name: "example_func".to_string(),
        schema: "public".to_string(),
        arguments: vec![],
        return_type: "TABLE(amount numeric(10,2), name text)".to_string(),
        language: "plpgsql".to_string(),
        body: "BEGIN END;".to_string(),
        volatility: Volatility::Stable,
        security: SecurityType::Invoker,
        config_params: vec![],
        owner: None,
        grants: vec![],
    };

    assert!(
        from_schema.semantically_equals(&from_db),
        "RETURNS TABLE with parameterized types like numeric(10,2) should not break on inner comma"
    );
}

#[test]
fn function_returns_table_multiword_type_aliases() {
    let from_schema = Function {
        name: "example_func".to_string(),
        schema: "public".to_string(),
        arguments: vec![],
        return_type: "table(created_at timestamptz, active bool)".to_string(),
        language: "plpgsql".to_string(),
        body: "BEGIN END;".to_string(),
        volatility: Volatility::Stable,
        security: SecurityType::Invoker,
        config_params: vec![],
        owner: None,
        grants: vec![],
    };

    let from_db = Function {
        name: "example_func".to_string(),
        schema: "public".to_string(),
        arguments: vec![],
        return_type: "TABLE(created_at timestamp with time zone, active boolean)".to_string(),
        language: "plpgsql".to_string(),
        body: "BEGIN END;".to_string(),
        volatility: Volatility::Stable,
        security: SecurityType::Invoker,
        config_params: vec![],
        owner: None,
        grants: vec![],
    };

    assert!(
        from_schema.semantically_equals(&from_db),
        "RETURNS TABLE with multi-word type aliases (timestamptz, bool) should normalize"
    );
}
