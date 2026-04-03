use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use pgmold::diff::{compute_diff, planner::plan_migration, MigrationOp};
use pgmold::model::{Column, Index, IndexType, PgType, Table};
use pgmold::parser::parse_sql_string;
use pgmold::pg::sqlgen::generate_sql;
use std::collections::BTreeMap;

fn generate_schema_sql(table_count: usize) -> String {
    let mut sql = String::new();

    for i in 0..table_count {
        sql.push_str(&format!(
            "CREATE TABLE public.table_{i} (\n  id SERIAL PRIMARY KEY,\n  name TEXT NOT NULL,\n  email VARCHAR(255),\n  created_at TIMESTAMP NOT NULL DEFAULT NOW(),\n  updated_at TIMESTAMP,\n  status TEXT DEFAULT 'active'\n);\n\n"
        ));

        sql.push_str(&format!(
            "CREATE INDEX table_{i}_email_idx ON public.table_{i} (email);\n\n"
        ));

        // Add FK to previous table for tables beyond the first
        if i > 0 {
            let prev = i - 1;
            sql.push_str(&format!(
                "ALTER TABLE public.table_{i} ADD COLUMN ref_id INTEGER;\n\
                 ALTER TABLE public.table_{i} ADD CONSTRAINT table_{i}_ref_fkey FOREIGN KEY (ref_id) REFERENCES public.table_{prev} (id);\n\n"
            ));
        }
    }

    sql
}

fn build_table(index: usize) -> Table {
    let mut columns = BTreeMap::new();
    columns.insert(
        "id".to_string(),
        Column {
            name: "id".to_string(),
            data_type: PgType::Integer,
            nullable: false,
            default: None,
            comment: None,
        },
    );
    columns.insert(
        "name".to_string(),
        Column {
            name: "name".to_string(),
            data_type: PgType::Text,
            nullable: false,
            default: None,
            comment: None,
        },
    );
    columns.insert(
        "email".to_string(),
        Column {
            name: "email".to_string(),
            data_type: PgType::Text,
            nullable: true,
            default: None,
            comment: None,
        },
    );

    Table {
        schema: "public".to_string(),
        name: format!("table_{index}"),
        columns,
        primary_key: None,
        indexes: vec![Index {
            name: format!("table_{index}_email_idx"),
            columns: vec!["email".to_string()],
            unique: false,
            index_type: IndexType::BTree,
            predicate: None,
            is_constraint: false,
        }],
        foreign_keys: Vec::new(),
        check_constraints: Vec::new(),
        comment: None,
        row_level_security: false,
        force_row_level_security: false,
        policies: Vec::new(),
        partition_by: None,
        owner: None,
        grants: Vec::new(),
    }
}

fn build_ops(count: usize) -> Vec<MigrationOp> {
    (0..count)
        .map(|i| MigrationOp::CreateTable(build_table(i)))
        .collect()
}

fn bench_parse(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("parse");

    for (label, count) in [("small", 10), ("medium", 100), ("large", 500)] {
        let sql = generate_schema_sql(count);
        group.bench_with_input(BenchmarkId::new("schema", label), &sql, |bencher, sql| {
            bencher.iter(|| parse_sql_string(sql).unwrap());
        });
    }

    group.finish();
}

fn bench_diff(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("diff");

    for (label, count) in [("small", 10), ("medium", 100)] {
        let sql = generate_schema_sql(count);
        let schema = parse_sql_string(&sql).unwrap();
        group.bench_with_input(
            BenchmarkId::new("identical", label),
            &schema,
            |bencher, schema| {
                bencher.iter(|| compute_diff(schema, schema));
            },
        );
    }

    for (label, table_count, modified_count) in [("small", 10usize, 3usize), ("medium", 100, 10)] {
        let base_sql = generate_schema_sql(table_count);
        let from = parse_sql_string(&base_sql).unwrap();

        // Build the "to" schema by adding extra columns to some tables
        let mut modified_sql = base_sql.clone();
        for i in 0..modified_count {
            modified_sql.push_str(&format!(
                "ALTER TABLE public.table_{i} ADD COLUMN extra_col_{i} TEXT;\n"
            ));
        }
        let to = parse_sql_string(&modified_sql).unwrap();

        group.bench_with_input(
            BenchmarkId::new("changes", label),
            &(&from, &to),
            |bencher, (from, to)| {
                bencher.iter(|| compute_diff(from, to));
            },
        );
    }

    group.finish();
}

fn bench_plan(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("plan_migration");

    for (label, count) in [("small", 10), ("medium", 50)] {
        let ops = build_ops(count);
        group.bench_with_input(
            BenchmarkId::new("migration", label),
            &ops,
            |bencher, ops| {
                bencher.iter(|| plan_migration(ops.clone()));
            },
        );
    }

    group.finish();
}

fn bench_generate_sql(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("generate_sql");

    for (label, count) in [("small", 10), ("medium", 50)] {
        let ops = build_ops(count);
        let planned = plan_migration(ops);
        group.bench_with_input(BenchmarkId::new("ops", label), &planned, |bencher, ops| {
            bencher.iter(|| generate_sql(ops));
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_parse,
    bench_diff,
    bench_plan,
    bench_generate_sql
);
criterion_main!(benches);
