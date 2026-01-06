use glob::Pattern;
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::str::FromStr;

use crate::model::Schema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObjectType {
    Extensions,
    Tables,
    Enums,
    Domains,
    Functions,
    Views,
    Triggers,
    Sequences,
    Partitions,
    Policies,
    Indexes,
    ForeignKeys,
    CheckConstraints,
}

impl FromStr for ObjectType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "extensions" => Ok(ObjectType::Extensions),
            "tables" => Ok(ObjectType::Tables),
            "enums" => Ok(ObjectType::Enums),
            "domains" => Ok(ObjectType::Domains),
            "functions" => Ok(ObjectType::Functions),
            "views" => Ok(ObjectType::Views),
            "triggers" => Ok(ObjectType::Triggers),
            "sequences" => Ok(ObjectType::Sequences),
            "partitions" => Ok(ObjectType::Partitions),
            "policies" => Ok(ObjectType::Policies),
            "indexes" => Ok(ObjectType::Indexes),
            "foreignkeys" => Ok(ObjectType::ForeignKeys),
            "checkconstraints" => Ok(ObjectType::CheckConstraints),
            _ => Err(format!(
                "Invalid object type '{s}'. Valid types: extensions, tables, enums, domains, functions, views, triggers, sequences, partitions, policies, indexes, foreignkeys, checkconstraints"
            )),
        }
    }
}

impl fmt::Display for ObjectType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ObjectType::Extensions => "extensions",
            ObjectType::Tables => "tables",
            ObjectType::Enums => "enums",
            ObjectType::Domains => "domains",
            ObjectType::Functions => "functions",
            ObjectType::Views => "views",
            ObjectType::Triggers => "triggers",
            ObjectType::Sequences => "sequences",
            ObjectType::Partitions => "partitions",
            ObjectType::Policies => "policies",
            ObjectType::Indexes => "indexes",
            ObjectType::ForeignKeys => "foreignkeys",
            ObjectType::CheckConstraints => "checkconstraints",
        };
        write!(f, "{s}")
    }
}

impl ObjectType {
    pub fn is_nested(&self) -> bool {
        matches!(
            self,
            ObjectType::Policies
                | ObjectType::Indexes
                | ObjectType::ForeignKeys
                | ObjectType::CheckConstraints
        )
    }
}

pub struct Filter {
    include: Vec<Pattern>,
    exclude: Vec<Pattern>,
    include_types: HashSet<ObjectType>,
    exclude_types: HashSet<ObjectType>,
}

impl Filter {
    pub fn new(
        include: &[String],
        exclude: &[String],
        include_types: &[ObjectType],
        exclude_types: &[ObjectType],
    ) -> Result<Self, glob::PatternError> {
        let include_patterns = include
            .iter()
            .map(|s| Pattern::new(s))
            .collect::<Result<Vec<_>, _>>()?;

        let exclude_patterns = exclude
            .iter()
            .map(|s| Pattern::new(s))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Filter {
            include: include_patterns,
            exclude: exclude_patterns,
            include_types: include_types.iter().copied().collect(),
            exclude_types: exclude_types.iter().copied().collect(),
        })
    }

    pub fn should_include(&self, name: &str) -> bool {
        if !self.exclude.is_empty() {
            for pattern in &self.exclude {
                if pattern.matches(name) {
                    return false;
                }
            }
        }

        if !self.include.is_empty() {
            for pattern in &self.include {
                if pattern.matches(name) {
                    return true;
                }
            }
            return false;
        }

        true
    }

    pub fn should_include_with_both(&self, qualified_name: &str, unqualified_name: &str) -> bool {
        if !self.exclude.is_empty() {
            for pattern in &self.exclude {
                if pattern.matches(qualified_name) || pattern.matches(unqualified_name) {
                    return false;
                }
            }
        }

        if !self.include.is_empty() {
            for pattern in &self.include {
                if pattern.matches(qualified_name) || pattern.matches(unqualified_name) {
                    return true;
                }
            }
            return false;
        }

        true
    }

    pub fn should_include_type(&self, obj_type: ObjectType) -> bool {
        if self.exclude_types.contains(&obj_type) {
            return false;
        }

        if self.include_types.is_empty() {
            return true;
        }

        if self.include_types.contains(&obj_type) {
            return true;
        }

        // Check if include_types contains only nested types
        let has_only_nested =
            !self.include_types.is_empty() && self.include_types.iter().all(|t| t.is_nested());

        // Include Tables when any nested type is in include_types
        // (nested types like Policies, Indexes, etc. are stored inside tables)
        if obj_type == ObjectType::Tables && has_only_nested {
            return true;
        }

        // If include_types has only nested types, include only those specific nested types
        // If include_types has top-level types, all nested types are included by default
        if obj_type.is_nested() && !has_only_nested {
            return true;
        }

        false
    }
}

fn filter_table(table: &crate::model::Table, filter: &Filter) -> crate::model::Table {
    let mut result = table.clone();
    if !filter.should_include_type(ObjectType::Policies) {
        result.policies = vec![];
    }
    if !filter.should_include_type(ObjectType::Indexes) {
        result.indexes = vec![];
    }
    if !filter.should_include_type(ObjectType::ForeignKeys) {
        result.foreign_keys = vec![];
    }
    if !filter.should_include_type(ObjectType::CheckConstraints) {
        result.check_constraints = vec![];
    }
    result
}

pub fn filter_schema(schema: &Schema, filter: &Filter) -> Schema {
    fn filter_field<T: Clone + HasName>(
        map: &BTreeMap<String, T>,
        filter: &Filter,
        object_type: ObjectType,
    ) -> BTreeMap<String, T> {
        if filter.should_include_type(object_type) {
            filter_map(map, filter)
        } else {
            BTreeMap::new()
        }
    }

    Schema {
        extensions: filter_field(&schema.extensions, filter, ObjectType::Extensions),
        tables: if filter.should_include_type(ObjectType::Tables) {
            schema
                .tables
                .iter()
                .filter(|(key, value)| filter.should_include_with_both(key, &value.name))
                .map(|(k, v)| (k.clone(), filter_table(v, filter)))
                .collect()
        } else {
            BTreeMap::new()
        },
        enums: filter_field(&schema.enums, filter, ObjectType::Enums),
        domains: filter_field(&schema.domains, filter, ObjectType::Domains),
        functions: filter_field(&schema.functions, filter, ObjectType::Functions),
        views: filter_field(&schema.views, filter, ObjectType::Views),
        triggers: filter_field(&schema.triggers, filter, ObjectType::Triggers),
        sequences: filter_field(&schema.sequences, filter, ObjectType::Sequences),
        partitions: filter_field(&schema.partitions, filter, ObjectType::Partitions),
        pending_policies: Vec::new(),
    }
}

fn filter_map<T>(map: &BTreeMap<String, T>, filter: &Filter) -> BTreeMap<String, T>
where
    T: Clone + HasName,
{
    map.iter()
        .filter(|(key, value)| filter.should_include_with_both(key, value.name()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

trait HasName {
    fn name(&self) -> &str;
}

impl HasName for crate::model::Table {
    fn name(&self) -> &str {
        &self.name
    }
}

impl HasName for crate::model::Function {
    fn name(&self) -> &str {
        &self.name
    }
}

impl HasName for crate::model::View {
    fn name(&self) -> &str {
        &self.name
    }
}

impl HasName for crate::model::Trigger {
    fn name(&self) -> &str {
        &self.name
    }
}

impl HasName for crate::model::EnumType {
    fn name(&self) -> &str {
        &self.name
    }
}

impl HasName for crate::model::Domain {
    fn name(&self) -> &str {
        &self.name
    }
}

impl HasName for crate::model::Sequence {
    fn name(&self) -> &str {
        &self.name
    }
}

impl HasName for crate::model::Partition {
    fn name(&self) -> &str {
        &self.name
    }
}

impl HasName for crate::model::Extension {
    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        Domain, EnumType, Extension, Function, Partition, PartitionBound, PgType, SecurityType,
        Sequence, SequenceDataType, Table, Trigger, TriggerEnabled, TriggerEvent, TriggerTiming,
        View, Volatility,
    };

    #[test]
    fn empty_filter_returns_clone() {
        let mut schema = Schema::default();
        schema.functions.insert(
            "public.api_test".to_string(),
            Function {
                name: "api_test".to_string(),
                schema: "public".to_string(),
                arguments: vec![],
                return_type: "void".to_string(),
                language: "sql".to_string(),
                body: "SELECT 1".to_string(),
                volatility: Volatility::Volatile,
                security: SecurityType::Invoker,
            },
        );
        schema.functions.insert(
            "public._internal".to_string(),
            Function {
                name: "_internal".to_string(),
                schema: "public".to_string(),
                arguments: vec![],
                return_type: "void".to_string(),
                language: "sql".to_string(),
                body: "SELECT 2".to_string(),
                volatility: Volatility::Volatile,
                security: SecurityType::Invoker,
            },
        );

        let filter = Filter::new(&[], &[], &[], &[]).unwrap();
        let filtered = filter_schema(&schema, &filter);

        assert_eq!(filtered.functions.len(), 2);
    }

    #[test]
    fn exclude_filters_functions() {
        let mut schema = Schema::default();
        schema.functions.insert(
            "public.api_test".to_string(),
            Function {
                name: "api_test".to_string(),
                schema: "public".to_string(),
                arguments: vec![],
                return_type: "void".to_string(),
                language: "sql".to_string(),
                body: "SELECT 1".to_string(),
                volatility: Volatility::Volatile,
                security: SecurityType::Invoker,
            },
        );
        schema.functions.insert(
            "public._internal".to_string(),
            Function {
                name: "_internal".to_string(),
                schema: "public".to_string(),
                arguments: vec![],
                return_type: "void".to_string(),
                language: "sql".to_string(),
                body: "SELECT 2".to_string(),
                volatility: Volatility::Volatile,
                security: SecurityType::Invoker,
            },
        );

        let filter = Filter::new(&[], &["_*".to_string()], &[], &[]).unwrap();
        let filtered = filter_schema(&schema, &filter);

        assert_eq!(filtered.functions.len(), 1);
        assert!(filtered.functions.contains_key("public.api_test"));
        assert!(!filtered.functions.contains_key("public._internal"));
    }

    #[test]
    fn include_filters_tables() {
        let mut schema = Schema::default();
        schema.tables.insert(
            "public.users".to_string(),
            Table {
                schema: "public".to_string(),
                name: "users".to_string(),
                columns: BTreeMap::new(),
                indexes: vec![],
                primary_key: None,
                foreign_keys: vec![],
                check_constraints: vec![],
                comment: None,
                row_level_security: false,
                policies: vec![],
                partition_by: None,
            },
        );
        schema.tables.insert(
            "public.posts".to_string(),
            Table {
                schema: "public".to_string(),
                name: "posts".to_string(),
                columns: BTreeMap::new(),
                indexes: vec![],
                primary_key: None,
                foreign_keys: vec![],
                check_constraints: vec![],
                comment: None,
                row_level_security: false,
                policies: vec![],
                partition_by: None,
            },
        );
        schema.tables.insert(
            "public._migrations".to_string(),
            Table {
                schema: "public".to_string(),
                name: "_migrations".to_string(),
                columns: BTreeMap::new(),
                indexes: vec![],
                primary_key: None,
                foreign_keys: vec![],
                check_constraints: vec![],
                comment: None,
                row_level_security: false,
                policies: vec![],
                partition_by: None,
            },
        );

        let filter =
            Filter::new(&["users".to_string(), "posts".to_string()], &[], &[], &[]).unwrap();
        let filtered = filter_schema(&schema, &filter);

        assert_eq!(filtered.tables.len(), 2);
    }

    #[test]
    fn extensions_are_filtered() {
        let mut schema = Schema::default();
        schema.extensions.insert(
            "uuid-ossp".to_string(),
            Extension {
                name: "uuid-ossp".to_string(),
                version: None,
                schema: None,
            },
        );

        let filter = Filter::new(&[], &["*".to_string()], &[], &[]).unwrap();
        let filtered = filter_schema(&schema, &filter);

        assert_eq!(filtered.extensions.len(), 0);
    }

    #[test]
    fn all_object_types_filtered() {
        let mut schema = Schema::default();

        schema.tables.insert(
            "public.users".to_string(),
            Table {
                schema: "public".to_string(),
                name: "users".to_string(),
                columns: BTreeMap::new(),
                indexes: vec![],
                primary_key: None,
                foreign_keys: vec![],
                check_constraints: vec![],
                comment: None,
                row_level_security: false,
                policies: vec![],
                partition_by: None,
            },
        );
        schema.tables.insert(
            "public._temp".to_string(),
            Table {
                schema: "public".to_string(),
                name: "_temp".to_string(),
                columns: BTreeMap::new(),
                indexes: vec![],
                primary_key: None,
                foreign_keys: vec![],
                check_constraints: vec![],
                comment: None,
                row_level_security: false,
                policies: vec![],
                partition_by: None,
            },
        );

        schema.views.insert(
            "public.user_view".to_string(),
            View {
                name: "user_view".to_string(),
                schema: "public".to_string(),
                query: "SELECT * FROM users".to_string(),
                materialized: false,
            },
        );
        schema.views.insert(
            "public._temp_view".to_string(),
            View {
                name: "_temp_view".to_string(),
                schema: "public".to_string(),
                query: "SELECT 1".to_string(),
                materialized: false,
            },
        );

        schema.triggers.insert(
            "public.users.audit_trigger".to_string(),
            Trigger {
                name: "audit_trigger".to_string(),
                target_schema: "public".to_string(),
                target_name: "users".to_string(),
                timing: TriggerTiming::Before,
                events: vec![TriggerEvent::Insert],
                update_columns: vec![],
                for_each_row: true,
                when_clause: None,
                function_schema: "public".to_string(),
                function_name: "audit_fn".to_string(),
                function_args: vec![],
                enabled: TriggerEnabled::Origin,
                old_table_name: None,
                new_table_name: None,
            },
        );
        schema.triggers.insert(
            "public.users._temp_trigger".to_string(),
            Trigger {
                name: "_temp_trigger".to_string(),
                target_schema: "public".to_string(),
                target_name: "users".to_string(),
                timing: TriggerTiming::Before,
                events: vec![TriggerEvent::Insert],
                update_columns: vec![],
                for_each_row: true,
                when_clause: None,
                function_schema: "public".to_string(),
                function_name: "temp_fn".to_string(),
                function_args: vec![],
                enabled: TriggerEnabled::Origin,
                old_table_name: None,
                new_table_name: None,
            },
        );

        schema.enums.insert(
            "public.status".to_string(),
            EnumType {
                schema: "public".to_string(),
                name: "status".to_string(),
                values: vec!["active".to_string(), "inactive".to_string()],
            },
        );
        schema.enums.insert(
            "public._temp_enum".to_string(),
            EnumType {
                schema: "public".to_string(),
                name: "_temp_enum".to_string(),
                values: vec!["a".to_string(), "b".to_string()],
            },
        );

        schema.domains.insert(
            "public.email".to_string(),
            Domain {
                schema: "public".to_string(),
                name: "email".to_string(),
                data_type: PgType::Text,
                default: None,
                not_null: false,
                collation: None,
                check_constraints: vec![],
            },
        );
        schema.domains.insert(
            "public._temp_domain".to_string(),
            Domain {
                schema: "public".to_string(),
                name: "_temp_domain".to_string(),
                data_type: PgType::Text,
                default: None,
                not_null: false,
                collation: None,
                check_constraints: vec![],
            },
        );

        schema.sequences.insert(
            "public.user_seq".to_string(),
            Sequence {
                name: "user_seq".to_string(),
                schema: "public".to_string(),
                data_type: SequenceDataType::BigInt,
                start: Some(1),
                increment: Some(1),
                min_value: None,
                max_value: None,
                cycle: false,
                cache: None,
                owned_by: None,
            },
        );
        schema.sequences.insert(
            "public._temp_seq".to_string(),
            Sequence {
                name: "_temp_seq".to_string(),
                schema: "public".to_string(),
                data_type: SequenceDataType::BigInt,
                start: Some(1),
                increment: Some(1),
                min_value: None,
                max_value: None,
                cycle: false,
                cache: None,
                owned_by: None,
            },
        );

        schema.partitions.insert(
            "public.users_2024".to_string(),
            Partition {
                schema: "public".to_string(),
                name: "users_2024".to_string(),
                parent_schema: "public".to_string(),
                parent_name: "users".to_string(),
                bound: PartitionBound::Default,
                indexes: vec![],
                check_constraints: vec![],
            },
        );
        schema.partitions.insert(
            "public._temp_part".to_string(),
            Partition {
                schema: "public".to_string(),
                name: "_temp_part".to_string(),
                parent_schema: "public".to_string(),
                parent_name: "users".to_string(),
                bound: PartitionBound::Default,
                indexes: vec![],
                check_constraints: vec![],
            },
        );

        let filter = Filter::new(&[], &["_*".to_string()], &[], &[]).unwrap();
        let filtered = filter_schema(&schema, &filter);

        assert_eq!(filtered.tables.len(), 1);
        assert!(filtered.tables.contains_key("public.users"));

        assert_eq!(filtered.views.len(), 1);
        assert!(filtered.views.contains_key("public.user_view"));

        assert_eq!(filtered.triggers.len(), 1);
        assert!(filtered.triggers.contains_key("public.users.audit_trigger"));

        assert_eq!(filtered.enums.len(), 1);
        assert!(filtered.enums.contains_key("public.status"));

        assert_eq!(filtered.domains.len(), 1);
        assert!(filtered.domains.contains_key("public.email"));

        assert_eq!(filtered.sequences.len(), 1);
        assert!(filtered.sequences.contains_key("public.user_seq"));

        assert_eq!(filtered.partitions.len(), 1);
        assert!(filtered.partitions.contains_key("public.users_2024"));
    }

    #[test]
    fn no_filters_includes_everything() {
        let filter = Filter::new(&[], &[], &[], &[]).unwrap();
        assert!(filter.should_include("anything"));
    }

    #[test]
    fn exclude_underscore_prefix() {
        let filter = Filter::new(&[], &["_*".to_string()], &[], &[]).unwrap();
        assert!(!filter.should_include("_add"));
        assert!(filter.should_include("api_change"));
    }

    #[test]
    fn include_pattern_filters() {
        let include = vec!["api_*".to_string()];
        let filter = Filter::new(&include, &[], &[], &[]).unwrap();
        assert!(filter.should_include("api_user"));
        assert!(!filter.should_include("st_distance"));
    }

    #[test]
    fn exclude_takes_precedence() {
        let include = vec!["api_*".to_string()];
        let exclude = vec!["*_test".to_string()];
        let filter = Filter::new(&include, &exclude, &[], &[]).unwrap();
        assert!(!filter.should_include("api_test"));
    }

    #[test]
    fn qualified_name_patterns() {
        let include = vec!["public.api_*".to_string()];
        let filter = Filter::new(&include, &[], &[], &[]).unwrap();
        assert!(filter.should_include("public.api_user"));
        assert!(!filter.should_include("auth.api_user"));
    }

    #[test]
    fn question_mark_matches_single_char() {
        let include = vec!["api_?".to_string()];
        let filter = Filter::new(&include, &[], &[], &[]).unwrap();
        assert!(filter.should_include("api_a"));
        assert!(!filter.should_include("api_ab"));
    }

    #[test]
    fn invalid_pattern_returns_error() {
        let invalid_include = vec!["[invalid".to_string()];
        assert!(Filter::new(&invalid_include, &[], &[], &[]).is_err());

        let invalid_exclude = vec!["[invalid".to_string()];
        assert!(Filter::new(&[], &invalid_exclude, &[], &[]).is_err());
    }

    #[test]
    fn object_type_from_str_valid_lowercase() {
        assert_eq!(
            "extensions".parse::<ObjectType>().unwrap(),
            ObjectType::Extensions
        );
        assert_eq!("tables".parse::<ObjectType>().unwrap(), ObjectType::Tables);
        assert_eq!("enums".parse::<ObjectType>().unwrap(), ObjectType::Enums);
        assert_eq!(
            "domains".parse::<ObjectType>().unwrap(),
            ObjectType::Domains
        );
        assert_eq!(
            "functions".parse::<ObjectType>().unwrap(),
            ObjectType::Functions
        );
        assert_eq!("views".parse::<ObjectType>().unwrap(), ObjectType::Views);
        assert_eq!(
            "triggers".parse::<ObjectType>().unwrap(),
            ObjectType::Triggers
        );
        assert_eq!(
            "sequences".parse::<ObjectType>().unwrap(),
            ObjectType::Sequences
        );
        assert_eq!(
            "partitions".parse::<ObjectType>().unwrap(),
            ObjectType::Partitions
        );
    }

    #[test]
    fn object_type_from_str_case_insensitive() {
        assert_eq!(
            "EXTENSIONS".parse::<ObjectType>().unwrap(),
            ObjectType::Extensions
        );
        assert_eq!("Tables".parse::<ObjectType>().unwrap(), ObjectType::Tables);
        assert_eq!("EnUmS".parse::<ObjectType>().unwrap(), ObjectType::Enums);
        assert_eq!(
            "DOMAINS".parse::<ObjectType>().unwrap(),
            ObjectType::Domains
        );
    }

    #[test]
    fn object_type_from_str_invalid() {
        let result = "invalid".parse::<ObjectType>();
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.contains("Invalid object type"));
        assert!(error.contains("extensions"));
        assert!(error.contains("tables"));
        assert!(error.contains("enums"));
        assert!(error.contains("domains"));
        assert!(error.contains("functions"));
        assert!(error.contains("views"));
        assert!(error.contains("triggers"));
        assert!(error.contains("sequences"));
        assert!(error.contains("partitions"));
    }

    #[test]
    fn object_type_display() {
        assert_eq!(ObjectType::Extensions.to_string(), "extensions");
        assert_eq!(ObjectType::Tables.to_string(), "tables");
        assert_eq!(ObjectType::Enums.to_string(), "enums");
        assert_eq!(ObjectType::Domains.to_string(), "domains");
        assert_eq!(ObjectType::Functions.to_string(), "functions");
        assert_eq!(ObjectType::Views.to_string(), "views");
        assert_eq!(ObjectType::Triggers.to_string(), "triggers");
        assert_eq!(ObjectType::Sequences.to_string(), "sequences");
        assert_eq!(ObjectType::Partitions.to_string(), "partitions");
    }

    #[test]
    fn should_include_type_empty_filters_returns_true() {
        let filter = Filter::new(&[], &[], &[], &[]).unwrap();
        assert!(filter.should_include_type(ObjectType::Tables));
        assert!(filter.should_include_type(ObjectType::Functions));
        assert!(filter.should_include_type(ObjectType::Views));
    }

    #[test]
    fn should_include_type_with_include_types() {
        let filter =
            Filter::new(&[], &[], &[ObjectType::Tables, ObjectType::Functions], &[]).unwrap();
        assert!(filter.should_include_type(ObjectType::Tables));
        assert!(filter.should_include_type(ObjectType::Functions));
        assert!(!filter.should_include_type(ObjectType::Views));
        assert!(!filter.should_include_type(ObjectType::Enums));
    }

    #[test]
    fn should_include_type_with_exclude_types() {
        let filter = Filter::new(
            &[],
            &[],
            &[],
            &[ObjectType::Triggers, ObjectType::Sequences],
        )
        .unwrap();
        assert!(filter.should_include_type(ObjectType::Tables));
        assert!(filter.should_include_type(ObjectType::Functions));
        assert!(!filter.should_include_type(ObjectType::Triggers));
        assert!(!filter.should_include_type(ObjectType::Sequences));
    }

    #[test]
    fn should_include_type_exclude_takes_precedence() {
        let filter = Filter::new(
            &[],
            &[],
            &[ObjectType::Tables, ObjectType::Functions],
            &[ObjectType::Functions],
        )
        .unwrap();
        assert!(filter.should_include_type(ObjectType::Tables));
        assert!(!filter.should_include_type(ObjectType::Functions));
        assert!(!filter.should_include_type(ObjectType::Views));
    }

    #[test]
    fn filter_schema_exclude_types_functions() {
        let mut schema = Schema::default();
        schema.functions.insert(
            "public.api_test".to_string(),
            Function {
                name: "api_test".to_string(),
                schema: "public".to_string(),
                arguments: vec![],
                return_type: "void".to_string(),
                language: "sql".to_string(),
                body: "SELECT 1".to_string(),
                volatility: Volatility::Volatile,
                security: SecurityType::Invoker,
            },
        );
        schema.tables.insert(
            "public.users".to_string(),
            Table {
                schema: "public".to_string(),
                name: "users".to_string(),
                columns: BTreeMap::new(),
                indexes: vec![],
                primary_key: None,
                foreign_keys: vec![],
                check_constraints: vec![],
                comment: None,
                row_level_security: false,
                policies: vec![],
                partition_by: None,
            },
        );

        let filter = Filter::new(&[], &[], &[], &[ObjectType::Functions]).unwrap();
        let filtered = filter_schema(&schema, &filter);

        assert_eq!(filtered.functions.len(), 0);
        assert_eq!(filtered.tables.len(), 1);
    }

    #[test]
    fn filter_schema_include_types_only_tables() {
        let mut schema = Schema::default();
        schema.functions.insert(
            "public.api_test".to_string(),
            Function {
                name: "api_test".to_string(),
                schema: "public".to_string(),
                arguments: vec![],
                return_type: "void".to_string(),
                language: "sql".to_string(),
                body: "SELECT 1".to_string(),
                volatility: Volatility::Volatile,
                security: SecurityType::Invoker,
            },
        );
        schema.tables.insert(
            "public.users".to_string(),
            Table {
                schema: "public".to_string(),
                name: "users".to_string(),
                columns: BTreeMap::new(),
                indexes: vec![],
                primary_key: None,
                foreign_keys: vec![],
                check_constraints: vec![],
                comment: None,
                row_level_security: false,
                policies: vec![],
                partition_by: None,
            },
        );
        schema.views.insert(
            "public.user_view".to_string(),
            View {
                name: "user_view".to_string(),
                schema: "public".to_string(),
                query: "SELECT * FROM users".to_string(),
                materialized: false,
            },
        );

        let filter = Filter::new(&[], &[], &[ObjectType::Tables], &[]).unwrap();
        let filtered = filter_schema(&schema, &filter);

        assert_eq!(filtered.tables.len(), 1);
        assert_eq!(filtered.functions.len(), 0);
        assert_eq!(filtered.views.len(), 0);
    }

    #[test]
    fn filter_schema_exclude_types_extensions() {
        let mut schema = Schema::default();
        schema.extensions.insert(
            "uuid-ossp".to_string(),
            Extension {
                name: "uuid-ossp".to_string(),
                version: None,
                schema: None,
            },
        );
        schema.tables.insert(
            "public.users".to_string(),
            Table {
                schema: "public".to_string(),
                name: "users".to_string(),
                columns: BTreeMap::new(),
                indexes: vec![],
                primary_key: None,
                foreign_keys: vec![],
                check_constraints: vec![],
                comment: None,
                row_level_security: false,
                policies: vec![],
                partition_by: None,
            },
        );

        let filter = Filter::new(&[], &[], &[], &[ObjectType::Extensions]).unwrap();
        let filtered = filter_schema(&schema, &filter);

        assert_eq!(filtered.extensions.len(), 0);
        assert_eq!(filtered.tables.len(), 1);
    }

    #[test]
    fn object_type_from_str_nested_types() {
        assert_eq!(
            "policies".parse::<ObjectType>().unwrap(),
            ObjectType::Policies
        );
        assert_eq!(
            "indexes".parse::<ObjectType>().unwrap(),
            ObjectType::Indexes
        );
        assert_eq!(
            "foreignkeys".parse::<ObjectType>().unwrap(),
            ObjectType::ForeignKeys
        );
        assert_eq!(
            "checkconstraints".parse::<ObjectType>().unwrap(),
            ObjectType::CheckConstraints
        );
    }

    #[test]
    fn object_type_from_str_nested_types_case_insensitive() {
        assert_eq!(
            "POLICIES".parse::<ObjectType>().unwrap(),
            ObjectType::Policies
        );
        assert_eq!(
            "Indexes".parse::<ObjectType>().unwrap(),
            ObjectType::Indexes
        );
        assert_eq!(
            "ForeignKeys".parse::<ObjectType>().unwrap(),
            ObjectType::ForeignKeys
        );
        assert_eq!(
            "CheckConstraints".parse::<ObjectType>().unwrap(),
            ObjectType::CheckConstraints
        );
    }

    #[test]
    fn object_type_display_nested_types() {
        assert_eq!(ObjectType::Policies.to_string(), "policies");
        assert_eq!(ObjectType::Indexes.to_string(), "indexes");
        assert_eq!(ObjectType::ForeignKeys.to_string(), "foreignkeys");
        assert_eq!(ObjectType::CheckConstraints.to_string(), "checkconstraints");
    }

    #[test]
    fn is_nested_returns_true_for_nested_types() {
        assert!(ObjectType::Policies.is_nested());
        assert!(ObjectType::Indexes.is_nested());
        assert!(ObjectType::ForeignKeys.is_nested());
        assert!(ObjectType::CheckConstraints.is_nested());
    }

    #[test]
    fn is_nested_returns_false_for_top_level_types() {
        assert!(!ObjectType::Extensions.is_nested());
        assert!(!ObjectType::Tables.is_nested());
        assert!(!ObjectType::Enums.is_nested());
        assert!(!ObjectType::Domains.is_nested());
        assert!(!ObjectType::Functions.is_nested());
        assert!(!ObjectType::Views.is_nested());
        assert!(!ObjectType::Triggers.is_nested());
        assert!(!ObjectType::Sequences.is_nested());
        assert!(!ObjectType::Partitions.is_nested());
    }

    #[test]
    fn nested_type_included_by_default_when_include_has_only_top_level() {
        let filter = Filter::new(&[], &[], &[ObjectType::Tables], &[]).unwrap();
        assert!(filter.should_include_type(ObjectType::Tables));
        assert!(filter.should_include_type(ObjectType::Policies));
        assert!(filter.should_include_type(ObjectType::Indexes));
        assert!(filter.should_include_type(ObjectType::ForeignKeys));
        assert!(!filter.should_include_type(ObjectType::Functions));
    }

    #[test]
    fn nested_type_excluded_when_in_exclude_types() {
        let filter = Filter::new(&[], &[], &[], &[ObjectType::Policies]).unwrap();
        assert!(filter.should_include_type(ObjectType::Tables));
        assert!(!filter.should_include_type(ObjectType::Policies));
        assert!(filter.should_include_type(ObjectType::Indexes));
    }

    #[test]
    fn include_types_with_nested_same_as_without_nested() {
        let filter_without = Filter::new(&[], &[], &[ObjectType::Tables], &[]).unwrap();
        let filter_with =
            Filter::new(&[], &[], &[ObjectType::Tables, ObjectType::Policies], &[]).unwrap();

        assert_eq!(
            filter_without.should_include_type(ObjectType::Tables),
            filter_with.should_include_type(ObjectType::Tables)
        );
        assert_eq!(
            filter_without.should_include_type(ObjectType::Policies),
            filter_with.should_include_type(ObjectType::Policies)
        );
        assert_eq!(
            filter_without.should_include_type(ObjectType::Indexes),
            filter_with.should_include_type(ObjectType::Indexes)
        );
        assert_eq!(
            filter_without.should_include_type(ObjectType::Functions),
            filter_with.should_include_type(ObjectType::Functions)
        );
    }

    #[test]
    fn nested_type_defaults_to_included_even_with_exclude_on_unrelated_type() {
        let filter =
            Filter::new(&[], &[], &[ObjectType::Functions], &[ObjectType::Indexes]).unwrap();
        assert!(filter.should_include_type(ObjectType::Functions));
        assert!(!filter.should_include_type(ObjectType::Indexes));
        assert!(filter.should_include_type(ObjectType::Policies));
        assert!(filter.should_include_type(ObjectType::ForeignKeys));
        assert!(!filter.should_include_type(ObjectType::Tables));
    }

    #[test]
    fn filter_table_strips_policies() {
        use crate::model::{Policy, PolicyCommand};

        let table = Table {
            schema: "public".to_string(),
            name: "users".to_string(),
            columns: BTreeMap::new(),
            indexes: vec![],
            primary_key: None,
            foreign_keys: vec![],
            check_constraints: vec![],
            comment: None,
            row_level_security: true,
            policies: vec![Policy {
                name: "user_policy".to_string(),
                table_schema: "public".to_string(),
                table: "users".to_string(),
                command: PolicyCommand::All,
                roles: vec!["authenticated".to_string()],
                using_expr: Some("user_id = current_user_id()".to_string()),
                check_expr: None,
            }],
            partition_by: None,
        };

        let filter = Filter::new(&[], &[], &[], &[ObjectType::Policies]).unwrap();
        let filtered_schema = filter_schema(
            &Schema {
                tables: vec![("public.users".to_string(), table)]
                    .into_iter()
                    .collect(),
                ..Default::default()
            },
            &filter,
        );

        let filtered_table = filtered_schema.tables.get("public.users").unwrap();
        assert_eq!(filtered_table.policies.len(), 0);
        assert!(filtered_table.row_level_security);
    }

    #[test]
    fn filter_table_strips_indexes_and_foreign_keys() {
        use crate::model::{ForeignKey, Index, IndexType, ReferentialAction};

        let table = Table {
            schema: "public".to_string(),
            name: "users".to_string(),
            columns: BTreeMap::new(),
            indexes: vec![Index {
                name: "users_email_idx".to_string(),
                columns: vec!["email".to_string()],
                unique: false,
                index_type: IndexType::BTree,
            }],
            primary_key: None,
            foreign_keys: vec![ForeignKey {
                name: "users_org_id_fkey".to_string(),
                columns: vec!["org_id".to_string()],
                referenced_schema: "public".to_string(),
                referenced_table: "orgs".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: ReferentialAction::Cascade,
                on_update: ReferentialAction::NoAction,
            }],
            check_constraints: vec![],
            comment: None,
            row_level_security: false,
            policies: vec![],
            partition_by: None,
        };

        let filter = Filter::new(
            &[],
            &[],
            &[],
            &[ObjectType::Indexes, ObjectType::ForeignKeys],
        )
        .unwrap();
        let filtered_schema = filter_schema(
            &Schema {
                tables: vec![("public.users".to_string(), table)]
                    .into_iter()
                    .collect(),
                ..Default::default()
            },
            &filter,
        );

        let filtered_table = filtered_schema.tables.get("public.users").unwrap();
        assert_eq!(filtered_table.indexes.len(), 0);
        assert_eq!(filtered_table.foreign_keys.len(), 0);
    }

    #[test]
    fn filter_table_no_exclusions_returns_full_table() {
        use crate::model::{Index, IndexType, Policy, PolicyCommand};

        let table = Table {
            schema: "public".to_string(),
            name: "users".to_string(),
            columns: BTreeMap::new(),
            indexes: vec![Index {
                name: "users_email_idx".to_string(),
                columns: vec!["email".to_string()],
                unique: false,
                index_type: IndexType::BTree,
            }],
            primary_key: None,
            foreign_keys: vec![],
            check_constraints: vec![],
            comment: None,
            row_level_security: true,
            policies: vec![Policy {
                name: "user_policy".to_string(),
                table_schema: "public".to_string(),
                table: "users".to_string(),
                command: PolicyCommand::All,
                roles: vec!["authenticated".to_string()],
                using_expr: Some("user_id = current_user_id()".to_string()),
                check_expr: None,
            }],
            partition_by: None,
        };

        let filter = Filter::new(&[], &[], &[], &[]).unwrap();
        let filtered_schema = filter_schema(
            &Schema {
                tables: vec![("public.users".to_string(), table.clone())]
                    .into_iter()
                    .collect(),
                ..Default::default()
            },
            &filter,
        );

        let filtered_table = filtered_schema.tables.get("public.users").unwrap();
        assert_eq!(filtered_table.indexes.len(), 1);
        assert_eq!(filtered_table.policies.len(), 1);
        assert_eq!(filtered_table.foreign_keys.len(), 0);
    }

    #[test]
    fn filter_table_include_types_tables_keeps_all_nested_types() {
        use crate::model::{Index, IndexType, Policy, PolicyCommand};

        let table = Table {
            schema: "public".to_string(),
            name: "users".to_string(),
            columns: BTreeMap::new(),
            indexes: vec![Index {
                name: "users_email_idx".to_string(),
                columns: vec!["email".to_string()],
                unique: false,
                index_type: IndexType::BTree,
            }],
            primary_key: None,
            foreign_keys: vec![],
            check_constraints: vec![],
            comment: None,
            row_level_security: true,
            policies: vec![Policy {
                name: "user_policy".to_string(),
                table_schema: "public".to_string(),
                table: "users".to_string(),
                command: PolicyCommand::All,
                roles: vec!["authenticated".to_string()],
                using_expr: Some("user_id = current_user_id()".to_string()),
                check_expr: None,
            }],
            partition_by: None,
        };

        let filter = Filter::new(&[], &[], &[ObjectType::Tables], &[]).unwrap();
        let filtered_schema = filter_schema(
            &Schema {
                tables: vec![("public.users".to_string(), table)]
                    .into_iter()
                    .collect(),
                functions: vec![(
                    "public.fn".to_string(),
                    Function {
                        name: "fn".to_string(),
                        schema: "public".to_string(),
                        arguments: vec![],
                        return_type: "void".to_string(),
                        language: "sql".to_string(),
                        body: "SELECT 1".to_string(),
                        volatility: Volatility::Volatile,
                        security: SecurityType::Invoker,
                    },
                )]
                .into_iter()
                .collect(),
                ..Default::default()
            },
            &filter,
        );

        assert_eq!(filtered_schema.tables.len(), 1);
        assert_eq!(filtered_schema.functions.len(), 0);

        let filtered_table = filtered_schema.tables.get("public.users").unwrap();
        assert_eq!(filtered_table.indexes.len(), 1);
        assert_eq!(filtered_table.policies.len(), 1);
    }

    #[test]
    fn include_only_policies_preserves_tables_with_policies() {
        use crate::model::{Policy, PolicyCommand};

        let table = Table {
            schema: "public".to_string(),
            name: "users".to_string(),
            columns: BTreeMap::new(),
            indexes: vec![],
            primary_key: None,
            foreign_keys: vec![],
            check_constraints: vec![],
            comment: None,
            row_level_security: true,
            policies: vec![Policy {
                name: "user_policy".to_string(),
                table_schema: "public".to_string(),
                table: "users".to_string(),
                command: PolicyCommand::All,
                roles: vec!["authenticated".to_string()],
                using_expr: Some("user_id = current_user_id()".to_string()),
                check_expr: None,
            }],
            partition_by: None,
        };

        let filter = Filter::new(&[], &[], &[ObjectType::Policies], &[]).unwrap();

        assert!(filter.should_include_type(ObjectType::Tables));
        assert!(filter.should_include_type(ObjectType::Policies));
        assert!(!filter.should_include_type(ObjectType::Functions));

        let filtered_schema = filter_schema(
            &Schema {
                tables: vec![("public.users".to_string(), table)]
                    .into_iter()
                    .collect(),
                functions: vec![(
                    "public.fn".to_string(),
                    Function {
                        name: "fn".to_string(),
                        schema: "public".to_string(),
                        arguments: vec![],
                        return_type: "void".to_string(),
                        language: "sql".to_string(),
                        body: "SELECT 1".to_string(),
                        volatility: Volatility::Volatile,
                        security: SecurityType::Invoker,
                    },
                )]
                .into_iter()
                .collect(),
                ..Default::default()
            },
            &filter,
        );

        assert_eq!(filtered_schema.tables.len(), 1);
        assert_eq!(filtered_schema.functions.len(), 0);

        let filtered_table = filtered_schema.tables.get("public.users").unwrap();
        assert_eq!(filtered_table.policies.len(), 1);
        assert_eq!(filtered_table.indexes.len(), 0);
    }

    #[test]
    fn include_only_indexes_preserves_tables_with_indexes() {
        use crate::model::{Index, IndexType};

        let table = Table {
            schema: "public".to_string(),
            name: "users".to_string(),
            columns: BTreeMap::new(),
            indexes: vec![Index {
                name: "users_email_idx".to_string(),
                columns: vec!["email".to_string()],
                unique: false,
                index_type: IndexType::BTree,
            }],
            primary_key: None,
            foreign_keys: vec![],
            check_constraints: vec![],
            comment: None,
            row_level_security: false,
            policies: vec![],
            partition_by: None,
        };

        let filter = Filter::new(&[], &[], &[ObjectType::Indexes], &[]).unwrap();

        assert!(filter.should_include_type(ObjectType::Tables));
        assert!(filter.should_include_type(ObjectType::Indexes));
        assert!(!filter.should_include_type(ObjectType::Policies));
        assert!(!filter.should_include_type(ObjectType::Functions));

        let filtered_schema = filter_schema(
            &Schema {
                tables: vec![("public.users".to_string(), table)]
                    .into_iter()
                    .collect(),
                ..Default::default()
            },
            &filter,
        );

        assert_eq!(filtered_schema.tables.len(), 1);
        let filtered_table = filtered_schema.tables.get("public.users").unwrap();
        assert_eq!(filtered_table.indexes.len(), 1);
        assert_eq!(filtered_table.policies.len(), 0);
    }
}
