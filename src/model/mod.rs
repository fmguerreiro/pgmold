use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Schema {
    pub tables: BTreeMap<String, Table>,
    pub enums: BTreeMap<String, EnumType>,
    pub functions: BTreeMap<String, Function>,
    pub views: BTreeMap<String, View>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Table {
    pub name: String,
    pub columns: BTreeMap<String, Column>,
    pub indexes: Vec<Index>,
    pub primary_key: Option<PrimaryKey>,
    pub foreign_keys: Vec<ForeignKey>,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EnumType {
    pub name: String,
    pub values: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Policy {
    pub name: String,
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

impl Schema {
    pub fn new() -> Self {
        Schema {
            tables: BTreeMap::new(),
            enums: BTreeMap::new(),
            functions: BTreeMap::new(),
            views: BTreeMap::new(),
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
                name: "users".to_string(),
                columns: BTreeMap::new(),
                indexes: Vec::new(),
                primary_key: None,
                foreign_keys: Vec::new(),
                comment: None,
                row_level_security: false,
                policies: Vec::new(),
            },
        );

        let mut schema4 = Schema::new();
        schema4.tables.insert(
            "users".to_string(),
            Table {
                name: "users".to_string(),
                columns: BTreeMap::new(),
                indexes: Vec::new(),
                primary_key: None,
                foreign_keys: Vec::new(),
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
}
