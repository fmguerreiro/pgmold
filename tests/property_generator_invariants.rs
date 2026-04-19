mod common;
use common::*;

use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::TestRunner;
use std::collections::HashSet;

fn extract_create_type_enum_names(sql: &str) -> Vec<String> {
    let mut names = Vec::new();
    for line in sql.lines() {
        let trimmed = line.trim_start();
        if let Some(rest) = trimmed.strip_prefix("CREATE TYPE ") {
            if let Some(as_index) = rest.find(" AS ENUM") {
                names.push(rest[..as_index].trim().to_string());
            }
        }
    }
    names
}

fn extract_create_table_names(sql: &str) -> Vec<String> {
    let mut names = Vec::new();
    for line in sql.lines() {
        let trimmed = line.trim_start();
        if let Some(rest) = trimmed.strip_prefix("CREATE TABLE ") {
            let name_end = rest.find(['(', ' ']).unwrap_or(rest.len());
            names.push(rest[..name_end].trim().to_string());
        }
    }
    names
}

#[test]
fn rich_schema_generator_never_produces_duplicate_enum_names() {
    let mut runner = TestRunner::deterministic();
    let strategy = rich_schema_sql_strategy("s_fixed".to_string());

    for _ in 0..2000 {
        let value = strategy.new_tree(&mut runner).unwrap().current();
        let enum_names = extract_create_type_enum_names(&value);
        let mut seen = HashSet::new();
        for name in &enum_names {
            assert!(
                seen.insert(name.clone()),
                "generator produced duplicate CREATE TYPE name {name} in single schema.\n\nSQL:\n{value}"
            );
        }
    }
}

#[test]
fn rich_schema_generator_never_produces_duplicate_table_names() {
    let mut runner = TestRunner::deterministic();
    let strategy = rich_schema_sql_strategy("s_fixed".to_string());

    for _ in 0..2000 {
        let value = strategy.new_tree(&mut runner).unwrap().current();
        let table_names = extract_create_table_names(&value);
        let mut seen = HashSet::new();
        for name in &table_names {
            assert!(
                seen.insert(name.clone()),
                "generator produced duplicate CREATE TABLE name {name} in single schema.\n\nSQL:\n{value}"
            );
        }
    }
}
