use crate::util::{canonicalize_expression, views_semantically_equal};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Schema {
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EnumType {
    pub schema: String,
    pub name: String,
    pub values: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Extension {
    pub name: String,
    pub version: Option<String>,
    pub schema: Option<String>,
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
}

impl Function {
    /// Compares two functions ignoring whitespace differences in their bodies.
    pub fn semantically_equals(&self, other: &Function) -> bool {
        self.name == other.name
            && self.schema == other.schema
            && self.arguments == other.arguments
            && self.return_type == other.return_type
            && self.language == other.language
            && self.volatility == other.volatility
            && self.security == other.security
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
            if self_arg.data_type == other_arg.data_type && self_arg.mode == other_arg.mode {
                if self_arg.name != other_arg.name || self_arg.default != other_arg.default {
                    return true;
                }
            }
        }

        false
    }
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
        }
    }

    pub fn fingerprint(&self) -> String {
        use sha2::{Digest, Sha256};
        let json = serde_json::to_string(self).expect("Schema must serialize");
        let hash = Sha256::digest(json.as_bytes());
        hex::encode(hash)
    }

    /// Associates pending policies with their respective tables.
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
        Ok(())
    }

    /// Associates pending policies with their respective tables.
    /// Policies referencing non-existent tables are returned.
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
        orphaned
    }
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
        };
        assert_eq!(table.schema, "auth");
    }

    #[test]
    fn enum_type_has_schema_field() {
        let enum_type = EnumType {
            schema: "auth".to_string(),
            name: "role".to_string(),
            values: vec!["admin".to_string(), "user".to_string()],
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
        };

        let introspected_view = View {
            name: "test_view".to_string(),
            schema: "public".to_string(),
            query: "SELECT 'supplier'::text AS type FROM users".to_string(),
            materialized: false,
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
        };

        let introspected_view = View {
            name: "test_view".to_string(),
            schema: "public".to_string(),
            query: "SELECT * FROM users WHERE name ~~ 'test%'::text".to_string(),
            materialized: false,
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
        };

        let introspected_view = View {
            name: "test_view".to_string(),
            schema: "public".to_string(),
            query: "SELECT id::text FROM users".to_string(),
            materialized: false,
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
        };

        let introspected_view = View {
            name: "test_view".to_string(),
            schema: "public".to_string(),
            query: "SELECT  id,  name  FROM  users  WHERE  active  =  true".to_string(),
            materialized: false,
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
        };

        let introspected_view = View {
            name: "test_view".to_string(),
            schema: "public".to_string(),
            query: "SELECT * FROM ( SELECT id FROM users )".to_string(),
            materialized: false,
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
        };

        let view2 = View {
            name: "test_view".to_string(),
            schema: "public".to_string(),
            query: "SELECT id FROM accounts".to_string(),
            materialized: false,
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
}
