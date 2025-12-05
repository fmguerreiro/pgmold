use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Schema {
    pub extensions: BTreeMap<String, Extension>,
    pub tables: BTreeMap<String, Table>,
    pub enums: BTreeMap<String, EnumType>,
    pub functions: BTreeMap<String, Function>,
    pub views: BTreeMap<String, View>,
    pub triggers: BTreeMap<String, Trigger>,
    pub sequences: BTreeMap<String, Sequence>,
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
    CustomEnum(String),
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
            functions: BTreeMap::new(),
            views: BTreeMap::new(),
            triggers: BTreeMap::new(),
            sequences: BTreeMap::new(),
        }
    }

    pub fn fingerprint(&self) -> String {
        use sha2::{Digest, Sha256};
        let json = serde_json::to_string(self).expect("Schema must serialize");
        let hash = Sha256::digest(json.as_bytes());
        hex::encode(hash)
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
            .map(|a| a.data_type.clone())
            .collect::<Vec<_>>()
            .join(", ");
        format!("{}({})", self.name, args)
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
}
