//! Phase 0 triage harness for pgmold-84 (parser coverage epic).
//!
//! For each open parser-coverage ticket, probes three layers:
//!   1. Does upstream sqlparser parse the raw DDL?
//!   2. Does pgmold's `parse_sql_string` return Ok (after preprocess)?
//!   3. Does the resulting Schema contain any mutations, or is the
//!      statement silently stripped / ignored by the match arms?
//!
//! The combination yields a bucket per ticket:
//!   - `ready`     - sqlparser parses, pgmold parses, effect observed.
//!   - `stripped`  - preprocess regex removed the statement.
//!   - `arm_gap`   - sqlparser parses but pgmold has no match arm / no effect.
//!   - `pgmold_bug`- sqlparser parses, pgmold errors (should not happen).
//!   - `needs_fork`- sqlparser rejects the DDL; upstream grammar work required.
//!
//! Runs under `#[ignore]` so it stays out of the main integration job; a
//! dedicated CI job invokes it with `--ignored --nocapture` and surfaces
//! the report in the run log.

use pgmold::model::Schema;
use pgmold::parser::parse_sql_string;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

struct Case {
    ticket: &'static str,
    label: &'static str,
    ddl: &'static str,
}

const CASES: &[Case] = &[
    Case {
        ticket: "pgmold-85",
        label: "CREATE AGGREGATE",
        ddl: "CREATE AGGREGATE public.sum_ints (int) (sfunc = int4pl, stype = int, initcond = '0');",
    },
    Case {
        ticket: "pgmold-89",
        label: "CREATE CAST",
        ddl: "CREATE CAST (text AS int) WITH FUNCTION public.to_int(text) AS ASSIGNMENT;",
    },
    Case {
        ticket: "pgmold-91",
        label: "CREATE CONVERSION",
        ddl: "CREATE CONVERSION public.myconv FOR 'LATIN1' TO 'UTF8' FROM iso8859_1_to_utf8;",
    },
    Case {
        ticket: "pgmold-92",
        label: "CREATE LANGUAGE",
        ddl: "CREATE LANGUAGE plperl;",
    },
    Case {
        ticket: "pgmold-93",
        label: "CREATE RULE",
        ddl: "CREATE RULE t_del AS ON DELETE TO public.t DO INSTEAD NOTHING;",
    },
    Case {
        ticket: "pgmold-94",
        label: "CREATE TEXT SEARCH CONFIG",
        ddl: "CREATE TEXT SEARCH CONFIGURATION public.simple_cfg (COPY = pg_catalog.simple);",
    },
    Case {
        ticket: "pgmold-95",
        label: "CREATE TEXT SEARCH DICTIONARY",
        ddl: "CREATE TEXT SEARCH DICTIONARY public.simple_dict (TEMPLATE = simple);",
    },
    Case {
        ticket: "pgmold-96",
        label: "CREATE TEXT SEARCH PARSER",
        ddl: "CREATE TEXT SEARCH PARSER public.my_parser (START = prsd_start, GETTOKEN = prsd_nexttoken, END = prsd_end, LEXTYPES = prsd_lextype);",
    },
    Case {
        ticket: "pgmold-97",
        label: "CREATE TEXT SEARCH TEMPLATE",
        ddl: "CREATE TEXT SEARCH TEMPLATE public.my_template (LEXIZE = dsimple_lexize);",
    },
    Case {
        ticket: "pgmold-98",
        label: "CREATE FOREIGN TABLE",
        ddl: "CREATE FOREIGN TABLE public.ft (id int) SERVER my_server;",
    },
    Case {
        ticket: "pgmold-100",
        label: "CREATE FOREIGN DATA WRAPPER",
        ddl: "CREATE FOREIGN DATA WRAPPER my_fdw HANDLER postgres_fdw_handler;",
    },
    Case {
        ticket: "pgmold-101",
        label: "CREATE PUBLICATION",
        ddl: "CREATE PUBLICATION mypub FOR TABLE public.t;",
    },
    Case {
        ticket: "pgmold-102",
        label: "CREATE SUBSCRIPTION",
        ddl: "CREATE SUBSCRIPTION mysub CONNECTION 'host=example' PUBLICATION mypub;",
    },
    Case {
        ticket: "pgmold-103",
        label: "CREATE STATISTICS",
        ddl: "CREATE STATISTICS public.s ON a, b FROM public.t;",
    },
    Case {
        ticket: "pgmold-104",
        label: "CREATE ACCESS METHOD",
        ddl: "CREATE ACCESS METHOD my_am TYPE INDEX HANDLER bthandler;",
    },
    Case {
        ticket: "pgmold-105",
        label: "CREATE EVENT TRIGGER",
        ddl: "CREATE EVENT TRIGGER myet ON ddl_command_start EXECUTE FUNCTION public.handler();",
    },
    Case {
        ticket: "pgmold-106",
        label: "CREATE TRANSFORM",
        ddl: "CREATE TRANSFORM FOR int LANGUAGE sql (FROM SQL WITH FUNCTION f1(internal), TO SQL WITH FUNCTION f2(int));",
    },
    Case {
        ticket: "pgmold-109",
        label: "ALTER TABLE SET TABLESPACE",
        ddl: "ALTER TABLE public.t SET TABLESPACE my_tablespace;",
    },
    Case {
        ticket: "pgmold-113",
        label: "ALTER DOMAIN ADD CONSTRAINT",
        ddl: "ALTER DOMAIN public.d ADD CONSTRAINT d_chk CHECK (VALUE > 0);",
    },
    Case {
        ticket: "pgmold-115",
        label: "SECURITY LABEL",
        ddl: "SECURITY LABEL FOR selinux ON TABLE public.t IS 'system_u:object_r:sepgsql_table_t:s0';",
    },
    Case {
        ticket: "pgmold-119",
        label: "CREATE USER MAPPING",
        ddl: "CREATE USER MAPPING FOR postgres SERVER my_server OPTIONS (\"user\" 'bob');",
    },
    Case {
        ticket: "pgmold-120",
        label: "CREATE TABLESPACE",
        ddl: "CREATE TABLESPACE my_ts LOCATION '/mnt/data';",
    },
    Case {
        ticket: "pgmold-124",
        label: "ALTER TRIGGER",
        ddl: "ALTER TRIGGER my_trigger ON public.t RENAME TO new_trigger;",
    },
    Case {
        ticket: "pgmold-126",
        label: "ALTER EXTENSION",
        ddl: "ALTER EXTENSION postgis UPDATE TO '3.4';",
    },
    Case {
        ticket: "pgmold-130",
        label: "ALTER PROCEDURE",
        ddl: "ALTER PROCEDURE public.p(int) SET search_path TO public;",
    },
];

fn sqlparser_direct(ddl: &str) -> Result<usize, String> {
    let dialect = PostgreSqlDialect {};
    Parser::parse_sql(&dialect, ddl)
        .map(|statements| statements.len())
        .map_err(|error| format!("{error}"))
}

fn schema_object_count(schema: &Schema) -> usize {
    schema.schemas.len()
        + schema.extensions.len()
        + schema.tables.len()
        + schema.enums.len()
        + schema.domains.len()
        + schema.functions.len()
        + schema.views.len()
        + schema.triggers.len()
        + schema.sequences.len()
        + schema.partitions.len()
        + schema.pending_policies.len()
        + schema.pending_owners.len()
        + schema.pending_grants.len()
        + schema.pending_revokes.len()
        + schema.pending_comments.len()
}

#[derive(Debug)]
enum Bucket {
    Ready,
    Stripped,
    ArmGap,
    PgmoldBug,
    NeedsFork,
}

impl Bucket {
    fn classify(sqlparser_ok: bool, pgmold_ok: bool, objects_landed: bool) -> Self {
        match (sqlparser_ok, pgmold_ok, objects_landed) {
            (true, true, true) => Bucket::Ready,
            (true, true, false) => Bucket::ArmGap,
            (true, false, _) => Bucket::PgmoldBug,
            (false, true, _) => Bucket::Stripped,
            (false, false, _) => Bucket::NeedsFork,
        }
    }

    fn label(&self) -> &'static str {
        match self {
            Bucket::Ready => "ready",
            Bucket::Stripped => "stripped",
            Bucket::ArmGap => "arm_gap",
            Bucket::PgmoldBug => "pgmold_bug",
            Bucket::NeedsFork => "needs_fork",
        }
    }
}

fn truncate(message: &str, limit: usize) -> String {
    if message.len() <= limit {
        message.to_string()
    } else {
        let mut cut = limit;
        while !message.is_char_boundary(cut) && cut > 0 {
            cut -= 1;
        }
        format!("{}...", &message[..cut])
    }
}

#[test]
#[ignore]
fn phase_0_triage_report() {
    let mut rows: Vec<String> = Vec::new();
    let mut bucket_counts = std::collections::BTreeMap::<&'static str, usize>::new();

    for case in CASES {
        let (sqlparser_ok, statement_count, sqlparser_error) = match sqlparser_direct(case.ddl) {
            Ok(count) => (true, count, String::new()),
            Err(error) => (false, 0, error),
        };

        let (pgmold_ok, objects_landed, pgmold_error) = match parse_sql_string(case.ddl) {
            Ok(schema) => (true, schema_object_count(&schema) > 0, String::new()),
            Err(error) => (false, false, format!("{error}")),
        };

        let bucket = Bucket::classify(sqlparser_ok, pgmold_ok, objects_landed);
        *bucket_counts.entry(bucket.label()).or_insert(0) += 1;

        let detail = if !sqlparser_error.is_empty() {
            format!("sqlparser: {}", truncate(&sqlparser_error, 160))
        } else if !pgmold_error.is_empty() {
            format!("pgmold: {}", truncate(&pgmold_error, 160))
        } else {
            format!("statements={statement_count} objects_landed={objects_landed}")
        };

        rows.push(format!(
            "{ticket:12} {bucket:11} {label:32} {detail}",
            ticket = case.ticket,
            bucket = bucket.label(),
            label = case.label,
        ));
    }

    println!();
    println!("=== pgmold-84 Phase 0 Parser Coverage Triage ===");
    println!();
    for row in &rows {
        println!("{row}");
    }
    println!();
    println!("=== Bucket totals ===");
    for (bucket, count) in &bucket_counts {
        println!("{bucket:11} {count}");
    }
    println!();
}
