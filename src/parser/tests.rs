use super::*;
use std::collections::BTreeMap;

use super::tables::detect_serial_type;

#[test]
fn parse_extension() {
    let sql = r#"
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pgcrypto;
"#;

    let schema = parse_sql_string(sql).expect("Should parse");

    assert_eq!(schema.extensions.len(), 2);
    assert!(schema.extensions.contains_key("uuid-ossp"));
    assert!(schema.extensions.contains_key("pgcrypto"));

    let uuid_ext = &schema.extensions["uuid-ossp"];
    assert_eq!(uuid_ext.name, "uuid-ossp");
}

#[test]
fn plpgsql_extension_skipped_during_parse() {
    let sql = r#"
CREATE EXTENSION IF NOT EXISTS plpgsql;
CREATE EXTENSION IF NOT EXISTS "plpgsql";
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
"#;

    let schema = parse_sql_string(sql).expect("Should parse");

    assert_eq!(schema.extensions.len(), 1);
    assert!(schema.extensions.contains_key("uuid-ossp"));
    assert!(!schema.extensions.contains_key("plpgsql"));
}

#[test]
fn parse_create_schema() {
    let sql = r#"
CREATE SCHEMA IF NOT EXISTS "myschema";
CREATE SCHEMA auth;
"#;

    let schema = parse_sql_string(sql).expect("Should parse");

    assert_eq!(schema.schemas.len(), 2);
    assert!(schema.schemas.contains_key("myschema"));
    assert!(schema.schemas.contains_key("auth"));

    let myschema = &schema.schemas["myschema"];
    assert_eq!(myschema.name, "myschema");
}

#[test]
fn parse_simple_view() {
    let sql = r#"
CREATE TABLE users (
id BIGINT NOT NULL PRIMARY KEY,
email VARCHAR(255) NOT NULL,
active BOOLEAN NOT NULL DEFAULT true
);

CREATE VIEW active_users AS
SELECT id, email FROM users WHERE active = true;
"#;

    let schema = parse_sql_string(sql).expect("Should parse");

    assert_eq!(schema.views.len(), 1);
    assert!(schema.views.contains_key("public.active_users"));

    let view = &schema.views["public.active_users"];
    assert_eq!(view.name, "active_users");
    assert!(!view.materialized);
    assert!(view.query.contains("SELECT"));
}

#[test]
fn parse_materialized_view() {
    let sql = r#"
CREATE TABLE orders (
id BIGINT NOT NULL PRIMARY KEY,
amount BIGINT NOT NULL,
created_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE MATERIALIZED VIEW order_totals AS
SELECT DATE(created_at) as day, SUM(amount) as total
FROM orders
GROUP BY DATE(created_at);
"#;

    let schema = parse_sql_string(sql).expect("Should parse");

    assert_eq!(schema.views.len(), 1);
    assert!(schema.views.contains_key("public.order_totals"));

    let view = &schema.views["public.order_totals"];
    assert_eq!(view.name, "order_totals");
    assert!(view.materialized);
}

#[test]
fn parse_simple_schema() {
    let sql = r#"
CREATE TYPE user_role AS ENUM ('admin', 'user', 'guest');

CREATE TABLE users (
id BIGINT NOT NULL,
email VARCHAR(255) NOT NULL,
role user_role NOT NULL DEFAULT 'guest',
created_at TIMESTAMP WITH TIME ZONE NOT NULL,
PRIMARY KEY (id)
);

CREATE UNIQUE INDEX users_email_idx ON users (email);

CREATE TABLE posts (
id BIGINT NOT NULL,
user_id BIGINT NOT NULL,
title TEXT NOT NULL,
content TEXT,
PRIMARY KEY (id),
CONSTRAINT posts_user_id_fkey FOREIGN KEY (user_id)
    REFERENCES users (id) ON DELETE CASCADE
);

CREATE INDEX posts_user_id_idx ON posts (user_id);
"#;

    let schema = parse_sql_string(sql).expect("Should parse");

    assert_eq!(schema.enums.len(), 1);
    assert!(schema.enums.contains_key("public.user_role"));
    assert_eq!(
        schema.enums["public.user_role"].values,
        vec!["admin", "user", "guest"]
    );

    assert_eq!(schema.tables.len(), 2);
    assert!(schema.tables.contains_key("public.users"));
    assert!(schema.tables.contains_key("public.posts"));

    let users = &schema.tables["public.users"];
    assert_eq!(users.columns.len(), 4);
    assert!(users.primary_key.is_some());
    assert_eq!(users.primary_key.as_ref().unwrap().columns, vec!["id"]);
    assert_eq!(users.indexes.len(), 1);
    assert!(users.indexes[0].unique);

    let posts = &schema.tables["public.posts"];
    assert_eq!(posts.columns.len(), 4);
    assert_eq!(posts.foreign_keys.len(), 1);
    assert_eq!(posts.foreign_keys[0].name, "posts_user_id_fkey");
    assert_eq!(posts.foreign_keys[0].on_delete, ReferentialAction::Cascade);
}

#[test]
fn parse_check_constraint() {
    let sql = r#"
CREATE TABLE products (
id BIGINT NOT NULL PRIMARY KEY,
price BIGINT NOT NULL,
quantity INTEGER NOT NULL,
CONSTRAINT price_positive CHECK (price > 0),
CONSTRAINT quantity_non_negative CHECK (quantity >= 0)
);
"#;

    let schema = parse_sql_string(sql).expect("Should parse");

    let products = &schema.tables["public.products"];
    assert_eq!(products.check_constraints.len(), 2);

    let price_check = products
        .check_constraints
        .iter()
        .find(|c| c.name == "price_positive")
        .expect("price_positive constraint should exist");
    assert_eq!(price_check.expression, "price > 0");

    let quantity_check = products
        .check_constraints
        .iter()
        .find(|c| c.name == "quantity_non_negative")
        .expect("quantity_non_negative constraint should exist");
    assert_eq!(quantity_check.expression, "quantity >= 0");
}

#[test]
fn parses_qualified_table_name() {
    let sql = "CREATE TABLE auth.users (id INTEGER PRIMARY KEY);";
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("auth.users").unwrap();
    assert_eq!(table.schema, "auth");
    assert_eq!(table.name, "users");
}

#[test]
fn parses_unqualified_table_defaults_to_public() {
    let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY);";
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.users").unwrap();
    assert_eq!(table.schema, "public");
    assert_eq!(table.name, "users");
    assert!(
        table.primary_key.is_some(),
        "PRIMARY KEY should be detected"
    );
    assert_eq!(
        table.primary_key.as_ref().unwrap().columns,
        vec!["id".to_string()]
    );
}

#[test]
fn parses_cross_schema_foreign_key() {
    let sql = r#"
        CREATE TABLE public.orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            FOREIGN KEY (user_id) REFERENCES auth.users(id)
        );
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.orders").unwrap();
    let fk = &table.foreign_keys[0];
    assert_eq!(fk.referenced_schema, "auth");
    assert_eq!(fk.referenced_table, "users");
}

#[test]
fn parses_qualified_view_name() {
    let sql =
        "CREATE VIEW reporting.active_users AS SELECT * FROM public.users WHERE active = true;";
    let schema = parse_sql_string(sql).unwrap();
    let view = schema.views.get("reporting.active_users").unwrap();
    assert_eq!(view.schema, "reporting");
    assert_eq!(view.name, "active_users");
}

#[test]
fn parses_qualified_function_name() {
    let sql = r#"
        CREATE FUNCTION utils.add_one(x INTEGER) RETURNS INTEGER
        LANGUAGE SQL AS $$ SELECT x + 1 $$;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let func = schema.functions.get("utils.add_one(integer)").unwrap();
    assert_eq!(func.schema, "utils");
    assert_eq!(func.name, "add_one");
}

#[test]
fn parses_function_with_set_search_path() {
    let sql = r#"
        CREATE OR REPLACE FUNCTION auth.custom_access_token_hook(event jsonb)
        RETURNS jsonb
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = auth, pg_temp, public
        AS $$
        BEGIN
            RETURN event;
        END;
        $$;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let func = schema
        .functions
        .get("auth.custom_access_token_hook(jsonb)")
        .unwrap();
    assert_eq!(func.schema, "auth");
    assert_eq!(func.name, "custom_access_token_hook");
    assert_eq!(func.language, "plpgsql");
    assert_eq!(func.security, SecurityType::Definer);
    assert_eq!(func.config_params.len(), 1);
    assert_eq!(func.config_params[0].0, "search_path");
    assert_eq!(func.config_params[0].1, "auth, pg_temp, public");
}

#[test]
fn parses_function_with_set_from_current() {
    let sql = r#"
        CREATE FUNCTION public.test_func() RETURNS void
        LANGUAGE plpgsql
        SET timezone FROM CURRENT
        AS $$ BEGIN END; $$;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let func = schema.functions.get("public.test_func()").unwrap();
    assert_eq!(func.config_params.len(), 1);
    assert_eq!(func.config_params[0].0, "timezone");
    assert_eq!(func.config_params[0].1, "FROM CURRENT");
}

#[test]
fn parses_function_with_security_invoker() {
    let sql = r#"
        CREATE FUNCTION public.safe_func() RETURNS INTEGER
        LANGUAGE sql SECURITY INVOKER
        AS $$ SELECT 1 $$;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let func = schema.functions.get("public.safe_func()").unwrap();
    assert_eq!(func.security, SecurityType::Invoker);
}

#[test]
fn parses_function_without_security_defaults_to_invoker() {
    let sql = r#"
        CREATE FUNCTION public.default_func() RETURNS INTEGER
        LANGUAGE sql AS $$ SELECT 1 $$;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let func = schema.functions.get("public.default_func()").unwrap();
    assert_eq!(func.security, SecurityType::Invoker);
}

#[test]
fn parses_alter_function_owner_to() {
    let sql = r#"
        CREATE FUNCTION auth.hook() RETURNS void LANGUAGE sql AS $$ SELECT 1 $$;
        ALTER FUNCTION auth.hook() OWNER TO supabase_auth_admin;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let func = schema.functions.get("auth.hook()").unwrap();
    assert_eq!(func.owner, Some("supabase_auth_admin".to_string()));
}

#[test]
fn parses_alter_type_owner_to() {
    let sql = r#"
        CREATE TYPE user_role AS ENUM ('admin', 'user', 'guest');
        ALTER TYPE user_role OWNER TO enum_owner;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let enum_type = schema.enums.get("public.user_role").unwrap();
    assert_eq!(enum_type.owner, Some("enum_owner".to_string()));
}

#[test]
fn parses_alter_domain_owner_to() {
    let sql = r#"
        CREATE DOMAIN email AS TEXT;
        ALTER DOMAIN email OWNER TO domain_owner;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let domain = schema.domains.get("public.email").unwrap();
    assert_eq!(domain.owner, Some("domain_owner".to_string()));
}

#[test]
fn parses_alter_table_owner_to() {
    let sql = r#"
        CREATE TABLE users (id INTEGER PRIMARY KEY);
        ALTER TABLE users OWNER TO table_owner;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.users").unwrap();
    assert_eq!(table.owner, Some("table_owner".to_string()));
}

#[test]
fn parses_alter_view_owner_to() {
    let sql = r#"
        CREATE TABLE base (id INTEGER);
        CREATE VIEW user_view AS SELECT id FROM base;
        ALTER VIEW user_view OWNER TO view_owner;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let view = schema.views.get("public.user_view").unwrap();
    assert_eq!(view.owner, Some("view_owner".to_string()));
}

#[test]
fn parses_alter_sequence_owner_to() {
    let sql = r#"
        CREATE SEQUENCE user_id_seq;
        ALTER SEQUENCE user_id_seq OWNER TO seq_owner;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let sequence = schema.sequences.get("public.user_id_seq").unwrap();
    assert_eq!(sequence.owner, Some("seq_owner".to_string()));
}

#[test]
fn owner_roundtrip_preserves_table_owner() {
    use crate::dump::generate_dump;
    let sql = r#"
        CREATE TABLE users (id BIGINT PRIMARY KEY);
        ALTER TABLE users OWNER TO test_owner;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    assert_eq!(
        schema.tables.get("public.users").unwrap().owner,
        Some("test_owner".to_string())
    );

    let dump = generate_dump(&schema, None);
    let reparsed = parse_sql_string(&dump).unwrap();
    assert_eq!(
        reparsed.tables.get("public.users").unwrap().owner,
        Some("test_owner".to_string()),
        "Owner should be preserved after roundtrip"
    );
}

#[test]
fn parses_qualified_enum_name() {
    let sql = "CREATE TYPE auth.role AS ENUM ('admin', 'user');";
    let schema = parse_sql_string(sql).unwrap();
    let enum_type = schema.enums.get("auth.role").unwrap();
    assert_eq!(enum_type.schema, "auth");
    assert_eq!(enum_type.name, "role");
}

#[test]
fn parses_simple_trigger() {
    let sql = r#"
CREATE FUNCTION audit_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
RETURN NEW;
END;
$$;

CREATE TRIGGER audit_trigger
AFTER INSERT ON users
FOR EACH ROW
EXECUTE FUNCTION audit_fn();
"#;
    let schema = parse_sql_string(sql).unwrap();
    assert_eq!(schema.triggers.len(), 1);

    let trigger = schema.triggers.get("public.users.audit_trigger").unwrap();
    assert_eq!(trigger.name, "audit_trigger");
    assert_eq!(trigger.target_schema, "public");
    assert_eq!(trigger.target_name, "users");
    assert_eq!(trigger.timing, TriggerTiming::After);
    assert_eq!(trigger.events, vec![TriggerEvent::Insert]);
    assert!(trigger.for_each_row);
    assert_eq!(trigger.function_name, "audit_fn");
}

#[test]
fn parses_trigger_with_update_of_columns() {
    let sql = r#"
CREATE FUNCTION notify_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;

CREATE TRIGGER notify_email_change
BEFORE UPDATE OF email, name ON users
FOR EACH ROW
EXECUTE FUNCTION notify_fn();
"#;
    let schema = parse_sql_string(sql).unwrap();
    let trigger = schema
        .triggers
        .get("public.users.notify_email_change")
        .unwrap();

    assert_eq!(trigger.timing, TriggerTiming::Before);
    assert_eq!(trigger.events, vec![TriggerEvent::Update]);
    assert_eq!(trigger.update_columns, vec!["email", "name"]);
}

#[test]
fn parses_trigger_with_multiple_events() {
    let sql = r#"
CREATE FUNCTION log_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;

CREATE TRIGGER log_changes
AFTER INSERT OR UPDATE OR DELETE ON orders
FOR EACH ROW
EXECUTE FUNCTION log_fn();
"#;
    let schema = parse_sql_string(sql).unwrap();
    let trigger = schema.triggers.get("public.orders.log_changes").unwrap();

    assert_eq!(trigger.events.len(), 3);
    assert!(trigger.events.contains(&TriggerEvent::Insert));
    assert!(trigger.events.contains(&TriggerEvent::Update));
    assert!(trigger.events.contains(&TriggerEvent::Delete));
}

#[test]
fn parses_trigger_with_when_clause() {
    let sql = r#"
CREATE FUNCTION check_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;

CREATE TRIGGER check_amount
BEFORE INSERT ON orders
FOR EACH ROW
WHEN (NEW.amount > 1000)
EXECUTE FUNCTION check_fn();
"#;
    let schema = parse_sql_string(sql).unwrap();
    let trigger = schema.triggers.get("public.orders.check_amount").unwrap();

    assert!(trigger.when_clause.is_some());
    assert!(trigger.when_clause.as_ref().unwrap().contains("amount"));
}

#[test]
fn parses_trigger_for_each_statement() {
    let sql = r#"
CREATE FUNCTION batch_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END; $$;

CREATE TRIGGER batch_notify
AFTER INSERT ON events
FOR EACH STATEMENT
EXECUTE FUNCTION batch_fn();
"#;
    let schema = parse_sql_string(sql).unwrap();
    let trigger = schema.triggers.get("public.events.batch_notify").unwrap();

    assert!(!trigger.for_each_row);
}

#[test]
fn parses_instead_of_trigger_on_view() {
    let sql = r#"
CREATE VIEW active_users AS SELECT * FROM users WHERE active = true;

CREATE FUNCTION insert_active_user_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
INSERT INTO users (name, active) VALUES (NEW.name, true);
RETURN NEW;
END;
$$;

CREATE TRIGGER insert_active_user
INSTEAD OF INSERT ON active_users
FOR EACH ROW
EXECUTE FUNCTION insert_active_user_fn();
"#;
    let schema = parse_sql_string(sql).unwrap();
    assert_eq!(schema.triggers.len(), 1);

    let trigger = schema
        .triggers
        .get("public.active_users.insert_active_user")
        .unwrap();
    assert_eq!(trigger.name, "insert_active_user");
    assert_eq!(trigger.target_schema, "public");
    assert_eq!(trigger.target_name, "active_users");
    assert_eq!(trigger.timing, TriggerTiming::InsteadOf);
    assert_eq!(trigger.events, vec![TriggerEvent::Insert]);
    assert!(trigger.for_each_row);
    assert!(trigger.when_clause.is_none());
    assert_eq!(trigger.function_name, "insert_active_user_fn");
}

#[test]
fn instead_of_trigger_rejects_for_each_statement() {
    let sql = r#"
CREATE VIEW active_users AS SELECT * FROM users WHERE active = true;

CREATE FUNCTION insert_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;

CREATE TRIGGER bad_trigger
INSTEAD OF INSERT ON active_users
FOR EACH STATEMENT
EXECUTE FUNCTION insert_fn();
"#;
    let result = parse_sql_string(sql);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("must be FOR EACH ROW"), "Error: {err}");
}

#[test]
fn instead_of_trigger_rejects_when_clause() {
    let sql = r#"
CREATE VIEW active_users AS SELECT * FROM users WHERE active = true;

CREATE FUNCTION insert_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;

CREATE TRIGGER bad_trigger
INSTEAD OF INSERT ON active_users
FOR EACH ROW
WHEN (NEW.id > 0)
EXECUTE FUNCTION insert_fn();
"#;
    let result = parse_sql_string(sql);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("cannot have a WHEN clause"), "Error: {err}");
}

#[test]
fn parse_create_sequence_minimal() {
    let sql = "CREATE SEQUENCE users_id_seq;";
    let schema = parse_sql_string(sql).unwrap();
    assert!(schema.sequences.contains_key("public.users_id_seq"));
    let seq = schema.sequences.get("public.users_id_seq").unwrap();
    assert_eq!(seq.name, "users_id_seq");
    assert_eq!(seq.schema, "public");
}

#[test]
fn parse_create_sequence_with_schema() {
    let sql = "CREATE SEQUENCE auth.counter_seq;";
    let schema = parse_sql_string(sql).unwrap();
    assert!(schema.sequences.contains_key("auth.counter_seq"));
}

#[test]
fn parse_create_sequence_with_data_type() {
    let sql = "CREATE SEQUENCE myschema.counter_seq AS bigint;";
    let schema = parse_sql_string(sql).unwrap();
    let seq = schema.sequences.get("myschema.counter_seq").unwrap();
    assert_eq!(seq.data_type, SequenceDataType::BigInt);
}

#[test]
fn parse_create_sequence_with_start() {
    let sql = "CREATE SEQUENCE myschema.counter_seq START WITH 100;";
    let schema = parse_sql_string(sql).unwrap();
    let seq = schema.sequences.get("myschema.counter_seq").unwrap();
    assert_eq!(seq.start, Some(100));
}

#[test]
fn parse_create_sequence_with_increment() {
    let sql = "CREATE SEQUENCE myschema.counter_seq INCREMENT BY 5;";
    let schema = parse_sql_string(sql).unwrap();
    let seq = schema.sequences.get("myschema.counter_seq").unwrap();
    assert_eq!(seq.increment, Some(5));
}

#[test]
fn parse_create_sequence_owned_by() {
    let sql = "CREATE SEQUENCE public.users_id_seq OWNED BY public.users.id;";
    let schema = parse_sql_string(sql).unwrap();
    let seq = schema.sequences.get("public.users_id_seq").unwrap();
    let owner = seq.owned_by.as_ref().unwrap();
    assert_eq!(owner.table_schema, "public");
    assert_eq!(owner.table_name, "users");
    assert_eq!(owner.column_name, "id");
}

#[test]
fn parse_create_sequence_with_negative_start() {
    let sql = "CREATE SEQUENCE test.desc_seq START WITH -1;";
    let schema = parse_sql_string(sql).unwrap();
    let seq = schema.sequences.get("test.desc_seq").unwrap();
    assert_eq!(seq.start, Some(-1));
}

#[test]
fn parse_create_sequence_with_negative_increment() {
    let sql = "CREATE SEQUENCE test.desc_seq INCREMENT BY -1;";
    let schema = parse_sql_string(sql).unwrap();
    let seq = schema.sequences.get("test.desc_seq").unwrap();
    assert_eq!(seq.increment, Some(-1));
}

#[test]
fn parse_create_sequence_with_negative_minvalue() {
    let sql = "CREATE SEQUENCE test.desc_seq MINVALUE -1000;";
    let schema = parse_sql_string(sql).unwrap();
    let seq = schema.sequences.get("test.desc_seq").unwrap();
    assert_eq!(seq.min_value, Some(-1000));
}

#[test]
fn parse_create_sequence_descending_defaults() {
    let sql = "CREATE SEQUENCE public.desc_seq INCREMENT BY -1;";
    let schema = parse_sql_string(sql).unwrap();
    let seq = schema.sequences.get("public.desc_seq").unwrap();
    assert_eq!(seq.increment, Some(-1));
    assert_eq!(seq.min_value, Some(-2147483648));
    assert_eq!(seq.max_value, Some(-1));
    assert_eq!(seq.start, Some(-1));
}

#[test]
fn parse_sequence_postgresql_order() {
    // PostgreSQL order: INCREMENT BY before START WITH
    let sql = "CREATE SEQUENCE seq INCREMENT BY 1 START WITH 1;";
    let result = parse_sql_string(sql);
    assert!(result.is_ok(), "PostgreSQL order should work: {result:?}");
}

#[test]
fn parse_alter_sequence_not_supported() {
    // sqlparser doesn't support ALTER SEQUENCE
    let sql = r#"ALTER SEQUENCE "public"."seq" OWNED BY "public"."users"."id";"#;
    let result = parse_sql_string(sql);
    // ALTER SEQUENCE is preprocessed out, so should parse OK (empty schema)
    assert!(result.is_ok());
}

#[test]
fn parse_create_sequence_full_options_with_owned_by() {
    // Full sequence with all options including OWNED BY inline
    let sql = r#"CREATE SEQUENCE "public"."user_id_seq" AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 OWNED BY "public"."users"."id";"#;
    let result = parse_sql_string(sql);
    assert!(
        result.is_ok(),
        "Full CREATE SEQUENCE should parse: {result:?}"
    );
    let schema = result.unwrap();
    let seq = schema.sequences.get("public.user_id_seq").unwrap();
    assert!(seq.owned_by.is_some());
}

#[test]
fn is_serial_type_detection() {
    use sqlparser::ast::DataType;
    use sqlparser::ast::Ident;
    use sqlparser::ast::ObjectName;
    use sqlparser::ast::ObjectNamePart;

    // SERIAL
    let serial = DataType::Custom(
        ObjectName(vec![ObjectNamePart::Identifier(Ident::new("serial"))]),
        vec![],
    );
    assert_eq!(detect_serial_type(&serial), Some(SequenceDataType::Integer));

    // BIGSERIAL
    let bigserial = DataType::Custom(
        ObjectName(vec![ObjectNamePart::Identifier(Ident::new("bigserial"))]),
        vec![],
    );
    assert_eq!(
        detect_serial_type(&bigserial),
        Some(SequenceDataType::BigInt)
    );

    // SMALLSERIAL
    let smallserial = DataType::Custom(
        ObjectName(vec![ObjectNamePart::Identifier(Ident::new("smallserial"))]),
        vec![],
    );
    assert_eq!(
        detect_serial_type(&smallserial),
        Some(SequenceDataType::SmallInt)
    );

    // Not serial
    let integer = DataType::Integer(None);
    assert_eq!(detect_serial_type(&integer), None);
}

#[test]
fn parse_serial_column_creates_sequence() {
    let sql = "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);";
    let schema = parse_sql_string(sql).unwrap();

    // Table should exist with integer column
    assert!(schema.tables.contains_key("public.users"));
    let table = schema.tables.get("public.users").unwrap();
    let id_col = table.columns.get("id").unwrap();
    assert_eq!(id_col.data_type, PgType::Integer);
    assert_eq!(
        id_col.default,
        Some("nextval('users_id_seq'::regclass)".to_string())
    );

    // Sequence should exist
    assert!(schema.sequences.contains_key("public.users_id_seq"));
    let seq = schema.sequences.get("public.users_id_seq").unwrap();
    assert_eq!(seq.data_type, SequenceDataType::Integer);
    assert!(seq.owned_by.is_some());
    let owner = seq.owned_by.as_ref().unwrap();
    assert_eq!(owner.table_schema, "public");
    assert_eq!(owner.table_name, "users");
    assert_eq!(owner.column_name, "id");
}

#[test]
fn parse_serial_ignores_explicit_default() {
    let sql = "CREATE TABLE test (id SERIAL DEFAULT 999);";
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.test").unwrap();
    let col = table.columns.get("id").unwrap();
    assert_eq!(
        col.default,
        Some("nextval('test_id_seq'::regclass)".to_string())
    );
}

#[test]
fn parse_bigserial_column() {
    let sql = "CREATE TABLE events (id BIGSERIAL PRIMARY KEY);";
    let schema = parse_sql_string(sql).unwrap();

    let table = schema.tables.get("public.events").unwrap();
    let id_col = table.columns.get("id").unwrap();
    assert_eq!(id_col.data_type, PgType::BigInt);

    let seq = schema.sequences.get("public.events_id_seq").unwrap();
    assert_eq!(seq.data_type, SequenceDataType::BigInt);
}

#[test]
fn parse_smallserial_column() {
    let sql = "CREATE TABLE counters (id SMALLSERIAL PRIMARY KEY);";
    let schema = parse_sql_string(sql).unwrap();

    let table = schema.tables.get("public.counters").unwrap();
    let id_col = table.columns.get("id").unwrap();
    assert_eq!(id_col.data_type, PgType::SmallInt);

    let seq = schema.sequences.get("public.counters_id_seq").unwrap();
    assert_eq!(seq.data_type, SequenceDataType::SmallInt);
}

#[test]
fn parse_serial_with_schema() {
    let sql = "CREATE TABLE auth.users (id SERIAL PRIMARY KEY, name TEXT);";
    let schema = parse_sql_string(sql).unwrap();

    assert!(schema.tables.contains_key("auth.users"));
    let table = schema.tables.get("auth.users").unwrap();
    let id_col = table.columns.get("id").unwrap();
    assert_eq!(
        id_col.default,
        Some("nextval('auth.users_id_seq'::regclass)".to_string())
    );

    assert!(schema.sequences.contains_key("auth.users_id_seq"));
    let seq = schema.sequences.get("auth.users_id_seq").unwrap();
    assert_eq!(seq.schema, "auth");
    let owner = seq.owned_by.as_ref().unwrap();
    assert_eq!(owner.table_schema, "auth");
}

#[test]
fn serial_default_omits_public_schema_prefix() {
    // Bug: Parser creates nextval('public.users_id_seq'::regclass) but PostgreSQL
    // information_schema returns nextval('users_id_seq'::regclass) for public schema.
    let sql = "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);";
    let schema = parse_sql_string(sql).unwrap();

    let table = schema.tables.get("public.users").unwrap();
    let id_col = table.columns.get("id").unwrap();

    // Should NOT have 'public.' prefix for public schema
    assert_eq!(
        id_col.default,
        Some("nextval('users_id_seq'::regclass)".to_string())
    );
}

#[test]
fn serial_default_keeps_non_public_schema_prefix() {
    // For non-public schemas, the schema prefix SHOULD be kept
    let sql = "CREATE TABLE auth.users (id SERIAL PRIMARY KEY, name TEXT);";
    let schema = parse_sql_string(sql).unwrap();

    let table = schema.tables.get("auth.users").unwrap();
    let id_col = table.columns.get("id").unwrap();

    // Should KEEP 'auth.' prefix for non-public schema
    assert_eq!(
        id_col.default,
        Some("nextval('auth.users_id_seq'::regclass)".to_string())
    );
}

#[test]
fn trigger_enabled_by_default() {
    let sql = r#"
CREATE FUNCTION audit_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
CREATE TRIGGER audit_trigger AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION audit_fn();
"#;
    let schema = parse_sql_string(sql).unwrap();
    let trigger = schema.triggers.get("public.users.audit_trigger").unwrap();
    assert_eq!(trigger.enabled, TriggerEnabled::Origin);
}

#[test]
fn parses_disable_trigger() {
    let sql = r#"
CREATE FUNCTION audit_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
CREATE TRIGGER audit_trigger AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION audit_fn();
ALTER TABLE users DISABLE TRIGGER audit_trigger;
"#;
    let schema = parse_sql_string(sql).unwrap();
    let trigger = schema.triggers.get("public.users.audit_trigger").unwrap();
    assert_eq!(trigger.enabled, TriggerEnabled::Disabled);
}

#[test]
fn parses_enable_trigger() {
    let sql = r#"
CREATE FUNCTION audit_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
CREATE TRIGGER audit_trigger AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION audit_fn();
ALTER TABLE users DISABLE TRIGGER audit_trigger;
ALTER TABLE users ENABLE TRIGGER audit_trigger;
"#;
    let schema = parse_sql_string(sql).unwrap();
    let trigger = schema.triggers.get("public.users.audit_trigger").unwrap();
    assert_eq!(trigger.enabled, TriggerEnabled::Origin);
}

#[test]
fn parses_enable_replica_trigger() {
    let sql = r#"
CREATE FUNCTION audit_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
CREATE TRIGGER audit_trigger AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION audit_fn();
ALTER TABLE users ENABLE REPLICA TRIGGER audit_trigger;
"#;
    let schema = parse_sql_string(sql).unwrap();
    let trigger = schema.triggers.get("public.users.audit_trigger").unwrap();
    assert_eq!(trigger.enabled, TriggerEnabled::Replica);
}

#[test]
fn parses_enable_always_trigger() {
    let sql = r#"
CREATE FUNCTION audit_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
CREATE TRIGGER audit_trigger AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION audit_fn();
ALTER TABLE users ENABLE ALWAYS TRIGGER audit_trigger;
"#;
    let schema = parse_sql_string(sql).unwrap();
    let trigger = schema.triggers.get("public.users.audit_trigger").unwrap();
    assert_eq!(trigger.enabled, TriggerEnabled::Always);
}

#[test]
fn parses_disable_trigger_with_schema() {
    let sql = r#"
CREATE FUNCTION audit_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
CREATE TRIGGER audit_trigger AFTER INSERT ON myschema.users FOR EACH ROW EXECUTE FUNCTION audit_fn();
ALTER TABLE myschema.users DISABLE TRIGGER audit_trigger;
"#;
    let schema = parse_sql_string(sql).unwrap();
    let trigger = schema.triggers.get("myschema.users.audit_trigger").unwrap();
    assert_eq!(trigger.enabled, TriggerEnabled::Disabled);
}

#[test]
fn parses_trigger_with_old_table() {
    let sql = r#"
CREATE FUNCTION audit_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN OLD; END; $$;
CREATE TRIGGER audit_deletes
AFTER DELETE ON users
REFERENCING OLD TABLE AS deleted_rows
FOR EACH STATEMENT
EXECUTE FUNCTION audit_fn();
"#;
    let schema = parse_sql_string(sql).unwrap();
    let trigger = schema.triggers.get("public.users.audit_deletes").unwrap();
    assert_eq!(trigger.old_table_name, Some("deleted_rows".to_string()));
    assert_eq!(trigger.new_table_name, None);
}

#[test]
fn parses_trigger_with_new_table() {
    let sql = r#"
CREATE FUNCTION audit_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
CREATE TRIGGER audit_inserts
AFTER INSERT ON users
REFERENCING NEW TABLE AS inserted_rows
FOR EACH STATEMENT
EXECUTE FUNCTION audit_fn();
"#;
    let schema = parse_sql_string(sql).unwrap();
    let trigger = schema.triggers.get("public.users.audit_inserts").unwrap();
    assert_eq!(trigger.old_table_name, None);
    assert_eq!(trigger.new_table_name, Some("inserted_rows".to_string()));
}

#[test]
fn parses_trigger_with_both_transition_tables() {
    let sql = r#"
CREATE FUNCTION audit_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
CREATE TRIGGER audit_updates
AFTER UPDATE ON users
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION audit_fn();
"#;
    let schema = parse_sql_string(sql).unwrap();
    let trigger = schema.triggers.get("public.users.audit_updates").unwrap();
    assert_eq!(trigger.old_table_name, Some("old_rows".to_string()));
    assert_eq!(trigger.new_table_name, Some("new_rows".to_string()));
}

#[test]
fn rejects_referencing_on_before_trigger() {
    let sql = r#"
CREATE FUNCTION audit_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
CREATE TRIGGER bad_trigger
BEFORE INSERT ON users
REFERENCING NEW TABLE AS new_rows
FOR EACH ROW
EXECUTE FUNCTION audit_fn();
"#;
    let result = parse_sql_string(sql);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("REFERENCING") && err.contains("AFTER"));
}

#[test]
fn rejects_referencing_on_instead_of_trigger() {
    let sql = r#"
CREATE VIEW user_view AS SELECT id, name FROM users;
CREATE FUNCTION insert_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
CREATE TRIGGER bad_trigger
INSTEAD OF INSERT ON user_view
REFERENCING NEW TABLE AS new_rows
FOR EACH ROW
EXECUTE FUNCTION insert_fn();
"#;
    let result = parse_sql_string(sql);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("REFERENCING") || err.contains("INSTEAD OF"));
}

#[test]
fn rejects_old_table_on_insert_only_trigger() {
    let sql = r#"
CREATE FUNCTION audit_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
CREATE TRIGGER bad_trigger
AFTER INSERT ON users
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT
EXECUTE FUNCTION audit_fn();
"#;
    let result = parse_sql_string(sql);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("OLD TABLE")
            && (err.contains("INSERT") || err.contains("UPDATE") || err.contains("DELETE"))
    );
}

#[test]
fn rejects_new_table_on_delete_only_trigger() {
    let sql = r#"
CREATE FUNCTION audit_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN OLD; END; $$;
CREATE TRIGGER bad_trigger
AFTER DELETE ON users
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION audit_fn();
"#;
    let result = parse_sql_string(sql);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("NEW TABLE")
            && (err.contains("INSERT") || err.contains("UPDATE") || err.contains("DELETE"))
    );
}

#[test]
fn parses_partition_by_range() {
    let sql = r#"
CREATE TABLE measurement (
city_id INT NOT NULL,
logdate DATE NOT NULL,
peaktemp INT,
unitsales INT
) PARTITION BY RANGE (logdate);
"#;
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.measurement").unwrap();

    let partition_by = table
        .partition_by
        .as_ref()
        .expect("partition_by should be set");
    assert_eq!(partition_by.strategy, PartitionStrategy::Range);
    assert_eq!(partition_by.columns, vec!["logdate".to_string()]);
}

#[test]
fn parses_partition_by_list() {
    let sql = r#"
CREATE TABLE customers (
id INT NOT NULL,
status TEXT NOT NULL,
name TEXT
) PARTITION BY LIST (status);
"#;
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.customers").unwrap();

    let partition_by = table
        .partition_by
        .as_ref()
        .expect("partition_by should be set");
    assert_eq!(partition_by.strategy, PartitionStrategy::List);
    assert_eq!(partition_by.columns, vec!["status".to_string()]);
}

#[test]
fn parses_partition_by_hash() {
    let sql = r#"
CREATE TABLE orders (
id INT NOT NULL,
customer_id INT NOT NULL,
created_at TIMESTAMP NOT NULL
) PARTITION BY HASH (id);
"#;
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.orders").unwrap();

    let partition_by = table
        .partition_by
        .as_ref()
        .expect("partition_by should be set");
    assert_eq!(partition_by.strategy, PartitionStrategy::Hash);
    assert_eq!(partition_by.columns, vec!["id".to_string()]);
}

#[test]
fn parses_partition_by_multiple_columns() {
    let sql = r#"
CREATE TABLE events (
region TEXT NOT NULL,
event_date DATE NOT NULL,
event_id INT NOT NULL
) PARTITION BY RANGE (region, event_date);
"#;
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.events").unwrap();

    let partition_by = table
        .partition_by
        .as_ref()
        .expect("partition_by should be set");
    assert_eq!(partition_by.strategy, PartitionStrategy::Range);
    assert_eq!(
        partition_by.columns,
        vec!["region".to_string(), "event_date".to_string()]
    );
}

#[test]
fn parses_range_partition() {
    let sql = r#"
CREATE TABLE measurement (
city_id INT NOT NULL,
logdate DATE NOT NULL
) PARTITION BY RANGE (logdate);

CREATE TABLE measurement_2024 PARTITION OF measurement
FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
"#;
    let schema = parse_sql_string(sql).unwrap();

    let partition = schema
        .partitions
        .get("public.measurement_2024")
        .expect("partition should exist");
    assert_eq!(partition.parent_schema, "public");
    assert_eq!(partition.parent_name, "measurement");
    match &partition.bound {
        PartitionBound::Range { from, to } => {
            assert_eq!(from, &vec!["'2024-01-01'".to_string()]);
            assert_eq!(to, &vec!["'2025-01-01'".to_string()]);
        }
        _ => panic!("Expected Range bound"),
    }
}

#[test]
fn parses_list_partition() {
    let sql = r#"
CREATE TABLE customers (
id INT NOT NULL,
status TEXT NOT NULL
) PARTITION BY LIST (status);

CREATE TABLE customers_active PARTITION OF customers
FOR VALUES IN ('active', 'pending');
"#;
    let schema = parse_sql_string(sql).unwrap();

    let partition = schema
        .partitions
        .get("public.customers_active")
        .expect("partition should exist");
    match &partition.bound {
        PartitionBound::List { values } => {
            assert_eq!(
                values,
                &vec!["'active'".to_string(), "'pending'".to_string()]
            );
        }
        _ => panic!("Expected List bound"),
    }
}

#[test]
fn parses_hash_partition() {
    let sql = r#"
CREATE TABLE orders (
id INT NOT NULL
) PARTITION BY HASH (id);

CREATE TABLE orders_part1 PARTITION OF orders
FOR VALUES WITH (MODULUS 4, REMAINDER 0);
"#;
    let schema = parse_sql_string(sql).unwrap();

    let partition = schema
        .partitions
        .get("public.orders_part1")
        .expect("partition should exist");
    match &partition.bound {
        PartitionBound::Hash { modulus, remainder } => {
            assert_eq!(*modulus, 4);
            assert_eq!(*remainder, 0);
        }
        _ => panic!("Expected Hash bound"),
    }
}

#[test]
fn parses_default_partition() {
    let sql = r#"
CREATE TABLE logs (
id INT NOT NULL,
level TEXT NOT NULL
) PARTITION BY LIST (level);

CREATE TABLE logs_other PARTITION OF logs DEFAULT;
"#;
    let schema = parse_sql_string(sql).unwrap();

    let partition = schema
        .partitions
        .get("public.logs_other")
        .expect("partition should exist");
    assert_eq!(partition.bound, PartitionBound::Default);
}

#[test]
fn parses_partition_with_schema() {
    let sql = r#"
CREATE TABLE analytics.events (
id INT NOT NULL,
occurred_at DATE NOT NULL
) PARTITION BY RANGE (occurred_at);

CREATE TABLE analytics.events_2024 PARTITION OF analytics.events
FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
"#;
    let schema = parse_sql_string(sql).unwrap();

    let table = schema.tables.get("analytics.events").unwrap();
    assert!(table.partition_by.is_some());

    let partition = schema
        .partitions
        .get("analytics.events_2024")
        .expect("partition should exist");
    assert_eq!(partition.schema, "analytics");
    assert_eq!(partition.parent_schema, "analytics");
    assert_eq!(partition.parent_name, "events");
}

#[test]
fn parses_simple_domain() {
    let sql = "CREATE DOMAIN email_address AS TEXT;";

    let schema = parse_sql_string(sql).expect("Should parse");

    assert_eq!(schema.domains.len(), 1);
    assert!(schema.domains.contains_key("public.email_address"));

    let domain = &schema.domains["public.email_address"];
    assert_eq!(domain.name, "email_address");
    assert_eq!(domain.schema, "public");
    assert!(!domain.not_null);
    assert!(domain.default.is_none());
    assert!(domain.check_constraints.is_empty());
}

#[test]
fn parses_domain_with_check_constraint() {
    let sql = "CREATE DOMAIN email_address AS TEXT CHECK (VALUE ~ '@');";

    let schema = parse_sql_string(sql).expect("Should parse");

    let domain = &schema.domains["public.email_address"];
    assert_eq!(domain.check_constraints.len(), 1);
    assert!(domain.check_constraints[0].expression.contains("@"));
}

#[test]
fn parses_domain_with_named_constraint() {
    let sql =
        "CREATE DOMAIN positive_int AS INTEGER CONSTRAINT must_be_positive CHECK (VALUE > 0);";

    let schema = parse_sql_string(sql).expect("Should parse");

    let domain = &schema.domains["public.positive_int"];
    assert_eq!(domain.data_type, PgType::Integer);
    assert_eq!(domain.check_constraints.len(), 1);
    assert_eq!(
        domain.check_constraints[0].name.as_deref(),
        Some("must_be_positive")
    );
}

#[test]
fn parses_domain_with_default() {
    let sql = "CREATE DOMAIN status AS TEXT DEFAULT 'pending';";

    let schema = parse_sql_string(sql).expect("Should parse");

    let domain = &schema.domains["public.status"];
    assert_eq!(domain.default.as_deref(), Some("'pending'"));
}

#[test]
fn parses_domain_with_collation() {
    let sql = r#"CREATE DOMAIN case_insensitive AS TEXT COLLATE "en_US";"#;

    let schema = parse_sql_string(sql).expect("Should parse");

    let domain = &schema.domains["public.case_insensitive"];
    assert!(domain.collation.is_some());
}

#[test]
fn parses_domain_full_syntax() {
    let sql = r#"
CREATE DOMAIN us_postal_code AS TEXT
COLLATE "en_US"
DEFAULT '00000'
CONSTRAINT valid_format CHECK (VALUE ~ '^\d{5}(-\d{4})?$');
"#;

    let schema = parse_sql_string(sql).expect("Should parse");

    let domain = &schema.domains["public.us_postal_code"];
    assert_eq!(domain.name, "us_postal_code");
    assert_eq!(domain.data_type, PgType::Text);
    assert!(domain.collation.is_some());
    assert_eq!(domain.default.as_deref(), Some("'00000'"));
    assert_eq!(domain.check_constraints.len(), 1);
    assert_eq!(
        domain.check_constraints[0].name.as_deref(),
        Some("valid_format")
    );
}

#[test]
fn parses_domain_with_schema() {
    let sql = "CREATE DOMAIN auth.email AS TEXT CHECK (VALUE ~ '@');";

    let schema = parse_sql_string(sql).expect("Should parse");

    assert!(schema.domains.contains_key("auth.email"));
    let domain = &schema.domains["auth.email"];
    assert_eq!(domain.schema, "auth");
    assert_eq!(domain.name, "email");
}

#[test]
fn domain_round_trip_with_check_constraint() {
    use crate::dump::generate_dump;

    let mut schema = Schema::new();
    schema.domains.insert(
        "public.email_address".to_string(),
        Domain {
            schema: "public".to_string(),
            name: "email_address".to_string(),
            data_type: PgType::Text,
            default: None,
            not_null: false,
            owner: None,
            grants: Vec::new(),
            collation: None,
            check_constraints: vec![DomainConstraint {
                name: None,
                expression: "VALUE ~ '@'".to_string(),
            }],
        },
    );

    let fingerprint_before = schema.fingerprint();
    let sql = generate_dump(&schema, None);
    let parsed = parse_sql_string(&sql).expect("Should parse generated SQL");
    let fingerprint_after = parsed.fingerprint();

    assert_eq!(
        fingerprint_before, fingerprint_after,
        "Domain should round-trip correctly"
    );
    assert_eq!(parsed.domains.len(), 1);
    let parsed_domain = &parsed.domains["public.email_address"];
    assert_eq!(parsed_domain.data_type, PgType::Text);
    assert_eq!(parsed_domain.check_constraints.len(), 1);
}

#[test]
fn domain_round_trip_with_table_using_domain() {
    use crate::dump::generate_dump;

    let mut schema = Schema::new();
    schema.domains.insert(
        "public.email_address".to_string(),
        Domain {
            schema: "public".to_string(),
            name: "email_address".to_string(),
            data_type: PgType::Text,
            default: None,
            not_null: false,
            owner: None,
            grants: Vec::new(),
            collation: None,
            check_constraints: vec![DomainConstraint {
                name: None,
                expression: "VALUE ~ '@'".to_string(),
            }],
        },
    );

    let mut users_columns = BTreeMap::new();
    users_columns.insert(
        "id".to_string(),
        Column {
            name: "id".to_string(),
            data_type: PgType::BigInt,
            nullable: false,
            default: None,
            comment: None,
        },
    );
    users_columns.insert(
        "email".to_string(),
        Column {
            name: "email".to_string(),
            data_type: PgType::CustomEnum("public.email_address".to_string()),
            nullable: false,
            default: None,
            comment: None,
        },
    );

    schema.tables.insert(
        "public.users".to_string(),
        Table {
            schema: "public".to_string(),
            name: "users".to_string(),
            columns: users_columns,
            primary_key: Some(PrimaryKey {
                columns: vec!["id".to_string()],
            }),
            indexes: Vec::new(),
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

    let fingerprint_before = schema.fingerprint();
    let sql = generate_dump(&schema, None);
    let parsed = parse_sql_string(&sql).expect("Should parse generated SQL");
    let fingerprint_after = parsed.fingerprint();

    assert_eq!(
        fingerprint_before, fingerprint_after,
        "Domain and table should round-trip correctly"
    );
}

#[test]
fn parses_policy_with_quoted_role_names() {
    let sql = r#"
        CREATE TABLE users (id BIGINT PRIMARY KEY);
        ALTER TABLE users ENABLE ROW LEVEL SECURITY;
        CREATE POLICY admin_policy ON users FOR ALL TO "authenticated" USING (true);
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.users").unwrap();
    let policy = &table.policies[0];

    assert_eq!(policy.roles.len(), 1);
    assert_eq!(
        policy.roles[0], "authenticated",
        "Role name should not have quotes"
    );
}

#[test]
fn parses_policy_before_table_in_same_file() {
    // Bug fix: policies should work regardless of statement order
    let sql = r#"
        CREATE POLICY admin_policy ON users FOR ALL TO "authenticated" USING (true);
        CREATE TABLE users (id BIGINT PRIMARY KEY);
        ALTER TABLE users ENABLE ROW LEVEL SECURITY;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.users").unwrap();

    assert_eq!(
        table.policies.len(),
        1,
        "Policy should be associated with table"
    );
    assert_eq!(table.policies[0].name, "admin_policy");
    assert_eq!(table.policies[0].roles, vec!["authenticated"]);
}

#[test]
fn parses_multiple_policies_different_order() {
    // Mix of policies before and after table definition
    let sql = r#"
        CREATE POLICY first_policy ON users FOR SELECT USING (true);
        CREATE TABLE users (id BIGINT PRIMARY KEY, role TEXT);
        CREATE POLICY second_policy ON users FOR INSERT WITH CHECK (role = 'admin');
        ALTER TABLE users ENABLE ROW LEVEL SECURITY;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.users").unwrap();

    assert_eq!(
        table.policies.len(),
        2,
        "Both policies should be associated"
    );
    // Policies are sorted by name
    let names: Vec<&str> = table.policies.iter().map(|p| p.name.as_str()).collect();
    assert!(names.contains(&"first_policy"));
    assert!(names.contains(&"second_policy"));
}

#[test]
fn policy_references_nonexistent_table_errors() {
    let sql = r#"
        CREATE POLICY orphan_policy ON nonexistent_table FOR ALL USING (true);
    "#;
    let result = parse_sql_string(sql);
    // The policy references a non-existent table, which should result in pending_policies
    // being non-empty, but parse_sql_string uses finalize_partial which doesn't error
    let schema = result.unwrap();
    assert!(
        schema.pending_policies.len() == 1,
        "Orphaned policy should remain in pending"
    );
    assert_eq!(schema.pending_policies[0].name, "orphan_policy");
}

#[test]
fn parses_check_constraint_from_alter_table() {
    let sql = r#"
        CREATE TABLE products (id BIGINT PRIMARY KEY, price INTEGER);
        ALTER TABLE products ADD CONSTRAINT price_positive CHECK (price > 0);
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.products").unwrap();

    assert_eq!(table.check_constraints.len(), 1);
    assert_eq!(table.check_constraints[0].name, "price_positive");
    assert!(table.check_constraints[0].expression.contains("price > 0"));
}

#[test]
fn parses_function_with_quoted_parameter_names() {
    let sql = r#"
        CREATE FUNCTION auth.is_org_admin("p_role_name" text, "p_enterprise_id" uuid)
        RETURNS boolean LANGUAGE sql AS $$ SELECT true $$;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let func = schema
        .functions
        .get("auth.is_org_admin(text, uuid)")
        .unwrap();

    assert_eq!(func.arguments[0].name, Some("p_role_name".to_string()));
    assert_eq!(func.arguments[1].name, Some("p_enterprise_id".to_string()));
}

#[test]
fn type_casts_normalized_to_lowercase() {
    let sql = r#"
        CREATE TABLE users (
            id BIGINT,
            role TEXT DEFAULT 'admin'::TEXT
        );
        CREATE POLICY user_policy ON users
            FOR ALL
            USING (role = 'admin'::TEXT)
            WITH CHECK (role = 'user'::VARCHAR);
        ALTER TABLE users ADD CONSTRAINT role_check CHECK (role IN ('admin'::TEXT, 'user'::TEXT));
    "#;
    let schema = parse_sql_string(sql).unwrap();

    let table = schema.tables.get("public.users").unwrap();

    let role_col = table.columns.get("role").unwrap();
    assert_eq!(
        role_col.default,
        Some("'admin'::text".to_string()),
        "Column default type casts should be lowercase"
    );

    let policy = &table.policies[0];
    assert_eq!(
        policy.using_expr,
        Some("role = 'admin'::text".to_string()),
        "Policy USING expression type casts should be lowercase"
    );
    assert_eq!(
        policy.check_expr,
        Some("role = 'user'::varchar".to_string()),
        "Policy CHECK expression type casts should be lowercase"
    );

    let check = &table.check_constraints[0];
    assert!(
        check.expression.contains("'admin'::text"),
        "Check constraint expression type casts should be lowercase: {}",
        check.expression
    );
}

#[test]
fn parses_trigger_on_cross_schema_table_with_qualified_function() {
    // Bug: triggers on non-public schema tables are not parsed correctly
    // when the function is also in a non-public schema
    let sql = r#"
CREATE FUNCTION auth.on_auth_user_created() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
CREATE TRIGGER "on_auth_user_created" AFTER INSERT ON "auth"."users" FOR EACH ROW EXECUTE FUNCTION "auth"."on_auth_user_created"();
"#;
    let schema = parse_sql_string(sql).unwrap();

    assert_eq!(schema.triggers.len(), 1, "Should parse exactly one trigger");
    let trigger = schema
        .triggers
        .get("auth.users.on_auth_user_created")
        .expect("Trigger should be keyed as auth.users.on_auth_user_created");
    assert_eq!(trigger.name, "on_auth_user_created");
    assert_eq!(trigger.target_schema, "auth");
    assert_eq!(trigger.target_name, "users");
    assert_eq!(trigger.function_schema, "auth");
    assert_eq!(trigger.function_name, "on_auth_user_created");
}

#[test]
fn parse_vector_types() {
    let sql = r#"
CREATE TABLE embeddings (
id BIGINT NOT NULL PRIMARY KEY,
embedding_default vector,
embedding_1536 vector(1536),
embedding_qualified public.vector(768)
);
"#;

    let schema = parse_sql_string(sql).expect("Should parse");

    let embeddings = &schema.tables["public.embeddings"];
    assert_eq!(embeddings.columns.len(), 4);

    let embedding_default = &embeddings.columns["embedding_default"];
    assert_eq!(embedding_default.data_type, PgType::Vector(None));

    let embedding_1536 = &embeddings.columns["embedding_1536"];
    assert_eq!(embedding_1536.data_type, PgType::Vector(Some(1536)));

    let embedding_qualified = &embeddings.columns["embedding_qualified"];
    assert_eq!(embedding_qualified.data_type, PgType::Vector(Some(768)));
}

#[test]
fn real_parses_correctly() {
    let sql = r#"
CREATE TABLE test (
"value" REAL
);
"#;
    let schema = parse_sql_string(sql).expect("Should parse REAL");
    let table = &schema.tables["public.test"];
    assert_eq!(table.columns["value"].data_type, PgType::Real);
}

#[test]
fn float4_parses_to_real() {
    let sql = r#"
CREATE TABLE test (
"value" FLOAT4
);
"#;
    let schema = parse_sql_string(sql).expect("Should parse FLOAT4");
    let table = &schema.tables["public.test"];
    assert_eq!(table.columns["value"].data_type, PgType::Real);
}

#[test]
fn float8_parses_to_double_precision() {
    let sql = r#"
CREATE TABLE test (
"value" FLOAT8
);
"#;
    let schema = parse_sql_string(sql).expect("Should parse FLOAT8");
    let table = &schema.tables["public.test"];
    assert_eq!(table.columns["value"].data_type, PgType::DoublePrecision);
}

#[test]
fn double_precision_parses_correctly() {
    let sql = r#"
CREATE TABLE "mrv"."Procurement" (
"id" TEXT NOT NULL,
"procurementAmount" DOUBLE PRECISION,
CONSTRAINT "Procurement_pkey" PRIMARY KEY ("id"),
CONSTRAINT "procurement_amount_positive" CHECK ("procurementAmount" > 0)
);
"#;

    let schema = parse_sql_string(sql).expect("Should parse DOUBLE PRECISION");

    let table = &schema.tables["mrv.Procurement"];
    let procurement_amount = &table.columns["procurementAmount"];

    assert_eq!(
        procurement_amount.data_type,
        PgType::DoublePrecision,
        "DOUBLE PRECISION should parse to PgType::DoublePrecision, not {:?}",
        procurement_amount.data_type
    );
}

#[test]
fn timestamptz_alias_parses_to_timestamptz_type() {
    let sql = r#"
CREATE TABLE "mrv"."Cultivation" (
"id" TEXT NOT NULL,
"plantingDate" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
"createdAt" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
"updatedAt" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
CONSTRAINT "Cultivation_pkey" PRIMARY KEY ("id")
);
"#;

    let schema = parse_sql_string(sql).expect("Should parse TIMESTAMPTZ");

    let table = &schema.tables["mrv.Cultivation"];
    let created_at = &table.columns["createdAt"];
    let updated_at = &table.columns["updatedAt"];
    let planting_date = &table.columns["plantingDate"];

    assert_eq!(
        created_at.data_type,
        PgType::TimestampTz,
        "TIMESTAMPTZ should parse to PgType::TimestampTz"
    );
    assert_eq!(
        updated_at.data_type,
        PgType::TimestampTz,
        "TIMESTAMPTZ should parse to PgType::TimestampTz"
    );
    assert_eq!(
        planting_date.data_type,
        PgType::Timestamp,
        "TIMESTAMP without time zone should parse to PgType::Timestamp"
    );
}

#[test]
fn parses_grant_on_table() {
    let sql = r#"
        CREATE TABLE users (id INTEGER PRIMARY KEY);
        GRANT SELECT, INSERT ON users TO app_user;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.users").unwrap();
    assert_eq!(table.grants.len(), 1);
    let grant = &table.grants[0];
    assert_eq!(grant.grantee, "app_user");
    assert!(grant.privileges.contains(&Privilege::Select));
    assert!(grant.privileges.contains(&Privilege::Insert));
    assert!(!grant.with_grant_option);
}

#[test]
fn parses_grant_with_table_keyword() {
    let sql = r#"
        CREATE TABLE products (id INTEGER PRIMARY KEY);
        GRANT SELECT ON TABLE products TO readonly_user;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.products").unwrap();
    assert_eq!(table.grants.len(), 1);
    assert_eq!(table.grants[0].grantee, "readonly_user");
    assert!(table.grants[0].privileges.contains(&Privilege::Select));
}

#[test]
fn parses_grant_with_schema_qualified_name() {
    let sql = r#"
        CREATE SCHEMA auth;
        CREATE TABLE auth.accounts (id INTEGER PRIMARY KEY);
        GRANT SELECT, UPDATE ON TABLE auth.accounts TO app_admin;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("auth.accounts").unwrap();
    assert_eq!(table.grants.len(), 1);
    assert_eq!(table.grants[0].grantee, "app_admin");
    assert!(table.grants[0].privileges.contains(&Privilege::Select));
    assert!(table.grants[0].privileges.contains(&Privilege::Update));
}

#[test]
fn parses_grant_on_view() {
    let sql = r#"
        CREATE TABLE base (id INTEGER);
        CREATE VIEW user_view AS SELECT id FROM base;
        GRANT SELECT ON VIEW user_view TO readonly;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let view = schema.views.get("public.user_view").unwrap();
    assert_eq!(view.grants.len(), 1);
    assert_eq!(view.grants[0].grantee, "readonly");
    assert!(view.grants[0].privileges.contains(&Privilege::Select));
}

#[test]
fn parses_grant_on_sequence() {
    let sql = r#"
        CREATE SEQUENCE user_id_seq;
        GRANT USAGE ON SEQUENCE user_id_seq TO app_user;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let sequence = schema.sequences.get("public.user_id_seq").unwrap();
    assert_eq!(sequence.grants.len(), 1);
    assert_eq!(sequence.grants[0].grantee, "app_user");
    assert!(sequence.grants[0].privileges.contains(&Privilege::Usage));
}

#[test]
fn parses_grant_on_function() {
    let sql = r#"
        CREATE FUNCTION add_numbers(a integer, b integer) RETURNS integer
        LANGUAGE sql AS $$ SELECT a + b $$;
        GRANT EXECUTE ON FUNCTION add_numbers(integer, integer) TO app_user;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let func = schema
        .functions
        .get("public.add_numbers(integer, integer)")
        .unwrap();
    assert_eq!(func.grants.len(), 1);
    assert_eq!(func.grants[0].grantee, "app_user");
    assert!(func.grants[0].privileges.contains(&Privilege::Execute));
}

#[test]
fn parses_grant_on_schema() {
    let sql = r#"
        CREATE SCHEMA test_schema;
        GRANT USAGE ON SCHEMA test_schema TO app_user;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let pg_schema = schema.schemas.get("test_schema").unwrap();
    assert_eq!(pg_schema.grants.len(), 1);
    assert_eq!(pg_schema.grants[0].grantee, "app_user");
    assert!(pg_schema.grants[0].privileges.contains(&Privilege::Usage));
}

#[test]
fn grant_all_on_schema_expands_to_usage_create() {
    let sql = r#"
        CREATE SCHEMA app;
        GRANT ALL ON SCHEMA app TO admin_user;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let pg_schema = schema.schemas.get("app").unwrap();
    assert_eq!(pg_schema.grants.len(), 1);
    assert_eq!(pg_schema.grants[0].grantee, "admin_user");
    assert!(pg_schema.grants[0].privileges.contains(&Privilege::Usage));
    assert!(pg_schema.grants[0].privileges.contains(&Privilege::Create));
    assert_eq!(pg_schema.grants[0].privileges.len(), 2);
}

#[test]
fn grant_all_on_sequence_expands_to_usage_select_update() {
    let sql = r#"
        CREATE SEQUENCE counter_seq;
        GRANT ALL ON SEQUENCE counter_seq TO app_user;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let seq = schema.sequences.get("public.counter_seq").unwrap();
    assert_eq!(seq.grants.len(), 1);
    assert!(seq.grants[0].privileges.contains(&Privilege::Usage));
    assert!(seq.grants[0].privileges.contains(&Privilege::Select));
    assert!(seq.grants[0].privileges.contains(&Privilege::Update));
    assert_eq!(seq.grants[0].privileges.len(), 3);
}

#[test]
fn grant_all_on_function_expands_to_execute() {
    let sql = r#"
        CREATE FUNCTION add(a integer, b integer) RETURNS integer LANGUAGE sql AS $$ SELECT a + b $$;
        GRANT ALL ON FUNCTION add(integer, integer) TO app_user;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let func = schema
        .functions
        .get("public.add(integer, integer)")
        .unwrap();
    assert_eq!(func.grants.len(), 1);
    assert!(func.grants[0].privileges.contains(&Privilege::Execute));
    assert_eq!(func.grants[0].privileges.len(), 1);
}

#[test]
fn grant_all_on_type_expands_to_usage() {
    let sql = r#"
        CREATE TYPE status AS ENUM ('active', 'inactive');
        GRANT ALL ON TYPE status TO app_user;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let enum_type = schema.enums.get("public.status").unwrap();
    assert_eq!(enum_type.grants.len(), 1);
    assert!(enum_type.grants[0].privileges.contains(&Privilege::Usage));
    assert_eq!(enum_type.grants[0].privileges.len(), 1);
}

#[test]
fn grant_all_on_table_expands_to_table_privileges() {
    let sql = r#"
        CREATE TABLE items (id INTEGER PRIMARY KEY);
        GRANT ALL ON TABLE items TO app_user;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.items").unwrap();
    assert_eq!(table.grants.len(), 1);
    assert_eq!(table.grants[0].privileges.len(), 7);
    assert!(table.grants[0].privileges.contains(&Privilege::Select));
    assert!(table.grants[0].privileges.contains(&Privilege::Insert));
    assert!(table.grants[0].privileges.contains(&Privilege::Update));
    assert!(table.grants[0].privileges.contains(&Privilege::Delete));
    assert!(table.grants[0].privileges.contains(&Privilege::Truncate));
    assert!(table.grants[0].privileges.contains(&Privilege::References));
    assert!(table.grants[0].privileges.contains(&Privilege::Trigger));
}

#[test]
fn parse_grant_all_on_all_tables_in_schema() {
    let sql = r#"
        CREATE TABLE mrv.orders (id INTEGER PRIMARY KEY);
        CREATE TABLE mrv.items (id INTEGER PRIMARY KEY);
        GRANT ALL ON ALL TABLES IN SCHEMA "mrv" TO service_role;
    "#;
    let schema = parse_sql_string(sql).unwrap();

    let orders_table = schema.tables.get("mrv.orders").unwrap();
    let items_table = schema.tables.get("mrv.items").unwrap();

    assert!(!orders_table.grants.is_empty());
    assert!(!items_table.grants.is_empty());
    assert_eq!(orders_table.grants[0].grantee, "service_role");
    assert_eq!(items_table.grants[0].grantee, "service_role");
    assert!(orders_table.grants[0]
        .privileges
        .contains(&Privilege::Select));
    assert!(orders_table.grants[0]
        .privileges
        .contains(&Privilege::Insert));
}

#[test]
fn grant_all_on_all_tables_includes_views() {
    let sql = r#"
        CREATE TABLE mrv.orders (id INTEGER PRIMARY KEY);
        CREATE VIEW mrv.summary AS SELECT count(*) FROM mrv.orders;
        GRANT ALL ON ALL TABLES IN SCHEMA "mrv" TO service_role;
    "#;
    let schema = parse_sql_string(sql).unwrap();

    let table = schema.tables.get("mrv.orders").unwrap();
    let view = schema.views.get("mrv.summary").unwrap();

    assert!(!table.grants.is_empty());
    assert!(!view.grants.is_empty());
    assert_eq!(view.grants[0].grantee, "service_role");
    assert!(view.grants[0].privileges.contains(&Privilege::Select));
}

#[test]
fn do_blocks_stripped_during_parse() {
    let sql = r#"
        DO $$
        BEGIN
            EXECUTE format('GRANT ALL ON SCHEMA public TO %I', current_user);
        END $$;

        CREATE TABLE users (id INTEGER PRIMARY KEY);
    "#;
    let schema = parse_sql_string(sql).unwrap();
    assert!(schema.tables.contains_key("public.users"));
}

#[test]
fn do_blocks_with_custom_tag_stripped() {
    let sql = r#"
        DO $do$
        BEGIN
            RAISE NOTICE 'hello';
        END $do$;

        CREATE TABLE items (id INTEGER PRIMARY KEY);
    "#;
    let schema = parse_sql_string(sql).unwrap();
    assert!(schema.tables.contains_key("public.items"));
}

#[test]
fn multiple_do_blocks_stripped() {
    let sql = r#"
        DO $$ BEGIN EXECUTE 'SELECT 1'; END $$;
        CREATE TABLE t1 (id INTEGER PRIMARY KEY);
        DO $$ BEGIN EXECUTE 'SELECT 2'; END $$;
        CREATE TABLE t2 (id INTEGER PRIMARY KEY);
    "#;
    let schema = parse_sql_string(sql).unwrap();
    assert!(schema.tables.contains_key("public.t1"));
    assert!(schema.tables.contains_key("public.t2"));
}

#[test]
fn do_blocks_with_language_stripped() {
    let sql = r#"
        DO LANGUAGE plpgsql $$
        BEGIN
            RAISE NOTICE 'test';
        END $$;

        CREATE TABLE t (id INTEGER PRIMARY KEY);
    "#;
    let schema = parse_sql_string(sql).unwrap();
    assert!(schema.tables.contains_key("public.t"));
}

#[test]
fn comment_on_with_semicolons_in_text_stripped() {
    let sql = r#"
        CREATE FUNCTION foo() RETURNS void LANGUAGE sql AS $$ SELECT 1; $$;
        COMMENT ON FUNCTION foo() IS 'Returns a; b; c';
        CREATE TABLE t (id INTEGER PRIMARY KEY);
    "#;
    let schema = parse_sql_string(sql).unwrap();
    assert!(schema.tables.contains_key("public.t"));
    assert!(schema.functions.contains_key("public.foo()"));
}

#[test]
fn comment_on_function_stripped_during_parse() {
    let sql = r#"
        CREATE FUNCTION add(a integer, b integer) RETURNS integer LANGUAGE sql AS $$ SELECT a + b $$;
        COMMENT ON FUNCTION add(integer, integer) IS 'Adds two numbers';
    "#;
    let schema = parse_sql_string(sql).unwrap();
    assert!(schema
        .functions
        .contains_key("public.add(integer, integer)"));
}

#[test]
fn comment_on_type_stripped_during_parse() {
    let sql = r#"
        CREATE TYPE status AS ENUM ('active', 'inactive');
        COMMENT ON TYPE status IS 'Status enum';
    "#;
    let schema = parse_sql_string(sql).unwrap();
    assert!(schema.enums.contains_key("public.status"));
}

#[test]
fn comment_on_schema_stripped_during_parse() {
    let sql = r#"
        CREATE SCHEMA myschema;
        COMMENT ON SCHEMA myschema IS 'My schema description';
        CREATE TABLE myschema.items (id INTEGER PRIMARY KEY);
    "#;
    let schema = parse_sql_string(sql).unwrap();
    assert!(schema.tables.contains_key("myschema.items"));
}

#[test]
fn parses_grant_on_enum_type() {
    let sql = r#"
        CREATE TYPE user_role AS ENUM ('admin', 'user');
        GRANT USAGE ON TYPE user_role TO app_user;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let enum_type = schema.enums.get("public.user_role").unwrap();
    assert_eq!(enum_type.grants.len(), 1);
    assert_eq!(enum_type.grants[0].grantee, "app_user");
    assert!(enum_type.grants[0].privileges.contains(&Privilege::Usage));
}

#[test]
fn parses_grant_with_grant_option() {
    let sql = r#"
        CREATE TABLE users (id INTEGER PRIMARY KEY);
        GRANT SELECT ON users TO app_user WITH GRANT OPTION;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.users").unwrap();
    assert_eq!(table.grants.len(), 1);
    let grant = &table.grants[0];
    assert_eq!(grant.grantee, "app_user");
    assert!(grant.with_grant_option);
}

#[test]
fn parses_grant_to_public() {
    let sql = r#"
        CREATE TABLE users (id INTEGER PRIMARY KEY);
        GRANT SELECT ON users TO PUBLIC;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.users").unwrap();
    assert_eq!(table.grants.len(), 1);
    assert_eq!(table.grants[0].grantee, "PUBLIC");
}

#[test]
fn parses_multiple_grants_on_same_object() {
    let sql = r#"
        CREATE TABLE users (id INTEGER PRIMARY KEY);
        GRANT SELECT ON users TO user1;
        GRANT INSERT, UPDATE ON users TO user2;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.users").unwrap();
    assert_eq!(table.grants.len(), 2);
    assert_eq!(table.grants[0].grantee, "user1");
    assert_eq!(table.grants[1].grantee, "user2");
}

#[test]
fn parses_grant_on_domain() {
    let sql = r#"
        CREATE DOMAIN email AS TEXT;
        GRANT USAGE ON TYPE email TO app_user;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let domain = schema.domains.get("public.email").unwrap();
    assert_eq!(domain.grants.len(), 1);
    assert_eq!(domain.grants[0].grantee, "app_user");
    assert!(domain.grants[0].privileges.contains(&Privilege::Usage));
}

#[test]
fn parses_grant_to_quoted_grantee_with_hyphen() {
    let sql = r#"
        CREATE TABLE users (id INTEGER PRIMARY KEY);
        GRANT SELECT ON users TO "app-user";
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.users").unwrap();
    assert_eq!(table.grants.len(), 1);
    assert_eq!(table.grants[0].grantee, "app-user");
}

#[test]
fn parses_grant_to_quoted_grantee_with_dot() {
    let sql = r#"
        CREATE TABLE users (id INTEGER PRIMARY KEY);
        GRANT SELECT ON users TO "service.account";
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.users").unwrap();
    assert_eq!(table.grants.len(), 1);
    assert_eq!(table.grants[0].grantee, "service.account");
}

#[test]
fn parses_grant_to_quoted_grantee_with_space() {
    let sql = r#"
        CREATE TABLE users (id INTEGER PRIMARY KEY);
        GRANT SELECT ON users TO "my user role";
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.users").unwrap();
    assert_eq!(table.grants.len(), 1);
    assert_eq!(table.grants[0].grantee, "my user role");
}

#[test]
fn parses_revoke_from_quoted_grantee_with_special_chars() {
    let sql = r#"
        CREATE TABLE users (id INTEGER PRIMARY KEY);
        GRANT SELECT ON users TO "app-user";
        REVOKE SELECT ON users FROM "app-user";
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.users").unwrap();
    assert_eq!(table.grants.len(), 0);
}

#[test]
fn parses_revoke_all_privileges() {
    let sql = r#"
        CREATE TABLE users (id INTEGER PRIMARY KEY);
        GRANT SELECT, INSERT ON users TO app_user;
        REVOKE SELECT, INSERT ON users FROM app_user;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.users").unwrap();
    assert_eq!(table.grants.len(), 0);
}

#[test]
fn parses_revoke_partial_privileges() {
    let sql = r#"
        CREATE TABLE users (id INTEGER PRIMARY KEY);
        GRANT SELECT, INSERT, UPDATE ON users TO app_user;
        REVOKE INSERT ON users FROM app_user;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.users").unwrap();
    assert_eq!(table.grants.len(), 1);
    let grant = &table.grants[0];
    assert_eq!(grant.grantee, "app_user");
    assert!(grant.privileges.contains(&Privilege::Select));
    assert!(!grant.privileges.contains(&Privilege::Insert));
    assert!(grant.privileges.contains(&Privilege::Update));
}

#[test]
fn parses_revoke_grant_option_for() {
    let sql = r#"
        CREATE TABLE users (id INTEGER PRIMARY KEY);
        GRANT SELECT ON users TO app_user WITH GRANT OPTION;
        REVOKE GRANT OPTION FOR SELECT ON users FROM app_user;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.users").unwrap();
    assert_eq!(table.grants.len(), 1);
    let grant = &table.grants[0];
    assert_eq!(grant.grantee, "app_user");
    assert!(grant.privileges.contains(&Privilege::Select));
    assert!(!grant.with_grant_option);
}

#[test]
fn parses_revoke_from_public() {
    let sql = r#"
        CREATE TABLE users (id INTEGER PRIMARY KEY);
        GRANT SELECT ON users TO PUBLIC;
        REVOKE SELECT ON users FROM PUBLIC;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.users").unwrap();
    assert_eq!(table.grants.len(), 0);
}

#[test]
fn parses_revoke_on_table_keyword() {
    let sql = r#"
        CREATE TABLE users (id INTEGER PRIMARY KEY);
        GRANT SELECT ON TABLE users TO app_user;
        REVOKE SELECT ON TABLE users FROM app_user;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.users").unwrap();
    assert_eq!(table.grants.len(), 0);
}

#[test]
fn parses_revoke_on_function() {
    let sql = r#"
        CREATE FUNCTION get_user(user_id integer) RETURNS text AS $$ SELECT 'user'; $$ LANGUAGE sql;
        GRANT EXECUTE ON FUNCTION get_user(integer) TO app_user;
        REVOKE EXECUTE ON FUNCTION get_user(integer) FROM app_user;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let func = schema.functions.get("public.get_user(integer)").unwrap();
    assert_eq!(func.grants.len(), 0);
}

#[test]
fn parses_revoke_on_sequence() {
    let sql = r#"
        CREATE SEQUENCE user_id_seq;
        GRANT USAGE ON SEQUENCE user_id_seq TO app_user;
        REVOKE USAGE ON SEQUENCE user_id_seq FROM app_user;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let seq = schema.sequences.get("public.user_id_seq").unwrap();
    assert_eq!(seq.grants.len(), 0);
}

#[test]
fn parses_revoke_on_schema() {
    let sql = r#"
        CREATE SCHEMA app;
        GRANT USAGE ON SCHEMA app TO app_user;
        REVOKE USAGE ON SCHEMA app FROM app_user;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let pg_schema = schema.schemas.get("app").unwrap();
    assert_eq!(pg_schema.grants.len(), 0);
}

#[test]
fn parses_revoke_on_type() {
    let sql = r#"
        CREATE TYPE status AS ENUM ('active', 'inactive');
        GRANT USAGE ON TYPE status TO app_user;
        REVOKE USAGE ON TYPE status FROM app_user;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let enum_type = schema.enums.get("public.status").unwrap();
    assert_eq!(enum_type.grants.len(), 0);
}

#[test]
fn parses_revoke_preserves_other_grantees() {
    let sql = r#"
        CREATE TABLE users (id INTEGER PRIMARY KEY);
        GRANT SELECT ON users TO user1;
        GRANT SELECT ON users TO user2;
        REVOKE SELECT ON users FROM user1;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.users").unwrap();
    assert_eq!(table.grants.len(), 1);
    assert_eq!(table.grants[0].grantee, "user2");
}

#[test]
fn parses_unique_constraint_in_create_table() {
    let sql = r#"
        CREATE TABLE users (
            id BIGINT PRIMARY KEY,
            email TEXT NOT NULL,
            CONSTRAINT users_email_unique UNIQUE (email)
        );
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.users").unwrap();

    let unique_idx = table
        .indexes
        .iter()
        .find(|idx| idx.name == "users_email_unique")
        .expect("UNIQUE constraint should be parsed as an index");

    assert!(unique_idx.unique, "Index should be marked as unique");
    assert_eq!(unique_idx.columns, vec!["email"]);
}

#[test]
fn parses_unique_constraint_from_alter_table() {
    let sql = r#"
        CREATE TABLE "auth"."mfa_amr_claims" (
            "id" uuid NOT NULL PRIMARY KEY,
            "session_id" uuid NOT NULL,
            "authentication_method" TEXT NOT NULL
        );
        ALTER TABLE "auth"."mfa_amr_claims" ADD CONSTRAINT
            "mfa_amr_claims_session_id_authentication_method_pkey"
            UNIQUE ("session_id", "authentication_method");
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("auth.mfa_amr_claims").unwrap();

    let unique_idx = table
        .indexes
        .iter()
        .find(|idx| idx.name == "mfa_amr_claims_session_id_authentication_method_pkey")
        .expect("UNIQUE constraint from ALTER TABLE should be parsed as an index");

    assert!(unique_idx.unique, "Index should be marked as unique");
    assert_eq!(
        unique_idx.columns,
        vec!["session_id", "authentication_method"]
    );
}

#[test]
fn parse_alter_table_add_column() {
    let sql = r#"
CREATE TABLE users (
id SERIAL PRIMARY KEY,
email VARCHAR(255) NOT NULL
);

ALTER TABLE users ADD COLUMN name TEXT;
ALTER TABLE users ADD COLUMN active BOOLEAN NOT NULL DEFAULT true;
"#;

    let schema = parse_sql_string(sql).expect("Should parse");

    let table = schema
        .tables
        .get("public.users")
        .expect("Table should exist");
    assert_eq!(table.columns.len(), 4); // id, email, name, active

    let name_col = table.columns.get("name").expect("name column should exist");
    assert_eq!(name_col.name, "name");
    assert!(name_col.nullable);
    assert!(name_col.default.is_none());

    let active_col = table
        .columns
        .get("active")
        .expect("active column should exist");
    assert_eq!(active_col.name, "active");
    assert!(!active_col.nullable);
    assert_eq!(active_col.default.as_deref(), Some("true"));
}

#[test]
fn parse_alter_table_add_column_serial() {
    let sql = r#"
CREATE TABLE items (
id SERIAL PRIMARY KEY
);

ALTER TABLE items ADD COLUMN version SERIAL;
"#;

    let schema = parse_sql_string(sql).expect("Should parse");

    let table = schema
        .tables
        .get("public.items")
        .expect("Table should exist");
    assert_eq!(table.columns.len(), 2); // id, version

    let version_col = table
        .columns
        .get("version")
        .expect("version column should exist");
    assert_eq!(version_col.name, "version");
    // SERIAL columns have nextval default
    assert!(version_col.default.as_ref().unwrap().contains("nextval"));

    // Should have created the sequence
    assert!(schema.sequences.contains_key("public.items_version_seq"));
}

#[test]
fn parse_alter_table_drop_column() {
    let sql = r#"
CREATE TABLE users (
id SERIAL PRIMARY KEY,
email VARCHAR(255) NOT NULL,
name TEXT,
deprecated_field TEXT
);

ALTER TABLE users DROP COLUMN deprecated_field;
ALTER TABLE users DROP COLUMN name;
"#;

    let schema = parse_sql_string(sql).expect("Should parse");

    let table = schema
        .tables
        .get("public.users")
        .expect("Table should exist");
    assert_eq!(table.columns.len(), 2); // id, email

    assert!(table.columns.contains_key("id"));
    assert!(table.columns.contains_key("email"));
    assert!(!table.columns.contains_key("name"));
    assert!(!table.columns.contains_key("deprecated_field"));
}

#[test]
fn parse_alter_table_add_and_drop_column() {
    let sql = r#"
CREATE TABLE products (
id SERIAL PRIMARY KEY,
name TEXT NOT NULL,
old_price NUMERIC
);

ALTER TABLE products DROP COLUMN old_price;
ALTER TABLE products ADD COLUMN price NUMERIC(10, 2) NOT NULL DEFAULT 0;
ALTER TABLE products ADD COLUMN description TEXT;
"#;

    let schema = parse_sql_string(sql).expect("Should parse");

    let table = schema
        .tables
        .get("public.products")
        .expect("Table should exist");
    assert_eq!(table.columns.len(), 4); // id, name, price, description

    assert!(!table.columns.contains_key("old_price"));
    assert!(table.columns.contains_key("price"));
    assert!(table.columns.contains_key("description"));

    let price_col = table
        .columns
        .get("price")
        .expect("price column should exist");
    assert!(!price_col.nullable);
    assert_eq!(price_col.default.as_deref(), Some("0"));
}

#[test]
fn parse_drop_table() {
    let sql = r#"
CREATE TABLE users (id SERIAL PRIMARY KEY, email TEXT);
CREATE TABLE posts (id SERIAL PRIMARY KEY, title TEXT);
DROP TABLE users;
"#;
    let schema = parse_sql_string(sql).expect("Should parse");

    assert!(!schema.tables.contains_key("public.users"));
    assert!(schema.tables.contains_key("public.posts"));
}

#[test]
fn parse_drop_table_if_exists() {
    let sql = r#"
CREATE TABLE users (id SERIAL PRIMARY KEY);
DROP TABLE IF EXISTS nonexistent;
DROP TABLE IF EXISTS users;
"#;
    let schema = parse_sql_string(sql).expect("Should parse");
    assert!(!schema.tables.contains_key("public.users"));
}

#[test]
fn parse_drop_table_qualified() {
    let sql = r#"
CREATE SCHEMA auth;
CREATE TABLE auth.users (id SERIAL PRIMARY KEY);
DROP TABLE auth.users;
"#;
    let schema = parse_sql_string(sql).expect("Should parse");
    assert!(!schema.tables.contains_key("auth.users"));
}

#[test]
fn parse_drop_view() {
    let sql = r#"
CREATE TABLE users (id SERIAL PRIMARY KEY, email TEXT);
CREATE VIEW active_users AS SELECT * FROM users;
DROP VIEW active_users;
"#;
    let schema = parse_sql_string(sql).expect("Should parse");

    assert!(schema.tables.contains_key("public.users"));
    assert!(!schema.views.contains_key("public.active_users"));
}

#[test]
fn parse_drop_index() {
    let sql = r#"
CREATE TABLE users (id SERIAL PRIMARY KEY, email TEXT);
CREATE INDEX users_email_idx ON users (email);
DROP INDEX users_email_idx;
"#;
    let schema = parse_sql_string(sql).expect("Should parse");

    let table = &schema.tables["public.users"];
    assert!(table.indexes.iter().all(|i| i.name != "users_email_idx"));
}

#[test]
fn parse_drop_sequence() {
    let sql = r#"
CREATE SEQUENCE user_id_seq;
CREATE SEQUENCE post_id_seq;
DROP SEQUENCE user_id_seq;
"#;
    let schema = parse_sql_string(sql).expect("Should parse");

    assert!(!schema.sequences.contains_key("public.user_id_seq"));
    assert!(schema.sequences.contains_key("public.post_id_seq"));
}

#[test]
fn parse_drop_type() {
    let sql = r#"
CREATE TYPE status AS ENUM ('active', 'inactive');
CREATE TYPE role AS ENUM ('admin', 'user');
DROP TYPE status;
"#;
    let schema = parse_sql_string(sql).expect("Should parse");

    assert!(!schema.enums.contains_key("public.status"));
    assert!(schema.enums.contains_key("public.role"));
}

#[test]
fn parse_drop_function() {
    let sql = r#"
CREATE FUNCTION add_one(x INTEGER) RETURNS INTEGER LANGUAGE sql AS 'SELECT x + 1';
CREATE FUNCTION add_two(x INTEGER) RETURNS INTEGER LANGUAGE sql AS 'SELECT x + 2';
DROP FUNCTION add_one(INTEGER);
"#;
    let schema = parse_sql_string(sql).expect("Should parse");

    assert!(!schema.functions.keys().any(|k| k.contains("add_one")));
    assert!(schema.functions.keys().any(|k| k.contains("add_two")));
}

#[test]
fn parse_drop_trigger() {
    let sql = r#"
CREATE TABLE users (id SERIAL PRIMARY KEY);
CREATE FUNCTION log_changes() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
CREATE TRIGGER users_audit AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION log_changes();
DROP TRIGGER users_audit ON users;
"#;
    let schema = parse_sql_string(sql).expect("Should parse");
    assert!(!schema.triggers.contains_key("public.users.users_audit"));
}

#[test]
fn parse_drop_policy() {
    let sql = r#"
CREATE TABLE users (id SERIAL PRIMARY KEY);
ALTER TABLE users ENABLE ROW LEVEL SECURITY;
CREATE POLICY users_policy ON users FOR ALL USING (true);
DROP POLICY users_policy ON users;
"#;
    let schema = parse_sql_string(sql).expect("Should parse");

    let table = &schema.tables["public.users"];
    assert!(table.policies.is_empty());
}

#[test]
fn parse_drop_domain() {
    let sql = r#"
CREATE DOMAIN email_address AS TEXT CHECK (VALUE ~ '@');
CREATE DOMAIN positive_int AS INTEGER CHECK (VALUE > 0);
DROP DOMAIN email_address;
"#;
    let schema = parse_sql_string(sql).expect("Should parse");

    assert!(!schema.domains.contains_key("public.email_address"));
    assert!(schema.domains.contains_key("public.positive_int"));
}

#[test]
fn parse_drop_extension() {
    let sql = r#"
CREATE EXTENSION pgcrypto;
CREATE EXTENSION uuid_ossp;
DROP EXTENSION pgcrypto;
"#;
    let schema = parse_sql_string(sql).expect("Should parse");

    assert!(!schema.extensions.contains_key("pgcrypto"));
    assert!(
        schema.extensions.contains_key("uuid_ossp") || schema.extensions.contains_key("uuid-ossp")
    );
}

#[test]
fn parse_alter_table_rename_table() {
    let sql = r#"
CREATE TABLE users (
id SERIAL PRIMARY KEY,
email TEXT NOT NULL
);

ALTER TABLE users RENAME TO customers;
"#;

    let schema = parse_sql_string(sql).expect("Should parse");

    assert!(!schema.tables.contains_key("public.users"));
    assert!(schema.tables.contains_key("public.customers"));

    let table = schema
        .tables
        .get("public.customers")
        .expect("Table should exist");
    assert_eq!(table.columns.len(), 2);
    assert!(table.columns.contains_key("id"));
    assert!(table.columns.contains_key("email"));
}

#[test]
fn parse_alter_table_rename_column() {
    let sql = r#"
CREATE TABLE users (
id SERIAL PRIMARY KEY,
email TEXT NOT NULL,
username TEXT
);

ALTER TABLE users RENAME COLUMN username TO display_name;
"#;

    let schema = parse_sql_string(sql).expect("Should parse");

    let table = schema
        .tables
        .get("public.users")
        .expect("Table should exist");
    assert_eq!(table.columns.len(), 3);
    assert!(table.columns.contains_key("id"));
    assert!(table.columns.contains_key("email"));
    assert!(!table.columns.contains_key("username"));
    assert!(table.columns.contains_key("display_name"));
}

#[test]
fn parse_alter_table_rename_constraint() {
    let sql = r#"
CREATE TABLE users (
id SERIAL PRIMARY KEY,
email TEXT NOT NULL
);
CREATE INDEX users_email_idx ON users (email);

ALTER TABLE users RENAME CONSTRAINT users_email_idx TO users_email_index;
"#;

    let schema = parse_sql_string(sql).expect("Should parse");

    let table = schema
        .tables
        .get("public.users")
        .expect("Table should exist");

    assert!(table.indexes.iter().all(|i| i.name != "users_email_idx"));
    assert!(table.indexes.iter().any(|i| i.name == "users_email_index"));
}

#[test]
fn parses_alter_default_privileges_for_role_in_schema() {
    let sql = r#"
        ALTER DEFAULT PRIVILEGES FOR ROLE admin IN SCHEMA public
        GRANT SELECT, INSERT ON TABLES TO app_user;
    "#;

    let schema = parse_sql_string(sql).unwrap();
    assert_eq!(schema.default_privileges.len(), 1);

    let dp = &schema.default_privileges[0];
    assert_eq!(dp.target_role, "admin");
    assert_eq!(dp.schema, Some("public".to_string()));
    assert_eq!(dp.object_type, DefaultPrivilegeObjectType::Tables);
    assert_eq!(dp.grantee, "app_user");
    assert!(dp.privileges.contains(&Privilege::Select));
    assert!(dp.privileges.contains(&Privilege::Insert));
    assert!(!dp.with_grant_option);
}

#[test]
fn parses_alter_default_privileges_global() {
    let sql = r#"
        ALTER DEFAULT PRIVILEGES FOR ROLE admin
        GRANT ALL ON SEQUENCES TO app_user WITH GRANT OPTION;
    "#;

    let schema = parse_sql_string(sql).unwrap();
    assert_eq!(schema.default_privileges.len(), 1);

    let dp = &schema.default_privileges[0];
    assert_eq!(dp.target_role, "admin");
    assert_eq!(dp.schema, None);
    assert_eq!(dp.object_type, DefaultPrivilegeObjectType::Sequences);
    assert!(dp.with_grant_option);
}

#[test]
fn parses_alter_default_privileges_implicit_role() {
    let sql = r#"
        ALTER DEFAULT PRIVILEGES IN SCHEMA api
        GRANT EXECUTE ON FUNCTIONS TO service_role;
    "#;

    let schema = parse_sql_string(sql).unwrap();
    assert_eq!(schema.default_privileges.len(), 1);

    let dp = &schema.default_privileges[0];
    assert_eq!(dp.target_role, "CURRENT_ROLE");
    assert_eq!(dp.schema, Some("api".to_string()));
    assert_eq!(dp.object_type, DefaultPrivilegeObjectType::Functions);
}

#[test]
fn parses_alter_default_privileges_revoke() {
    let sql = r#"
        ALTER DEFAULT PRIVILEGES FOR ROLE admin IN SCHEMA public
        REVOKE SELECT ON TABLES FROM app_user;
    "#;

    let schema = parse_sql_string(sql).unwrap();
    assert_eq!(schema.default_privileges.len(), 0);
}

#[test]
fn parse_drop_schema() {
    let sql = r#"
CREATE SCHEMA staging;
CREATE SCHEMA production;
DROP SCHEMA staging;
"#;
    let schema = parse_sql_string(sql).expect("Should parse");

    assert!(!schema.schemas.contains_key("staging"));
    assert!(schema.schemas.contains_key("production"));
}

#[test]
fn dml_statements_skipped_gracefully() {
    let sql = r#"
CREATE TABLE users (
id SERIAL PRIMARY KEY,
email TEXT NOT NULL UNIQUE
);
INSERT INTO users (email) VALUES ('test@example.com') ON CONFLICT DO NOTHING;
INSERT INTO users (email) VALUES ('a@b.com')
ON CONFLICT (email) DO UPDATE SET email = EXCLUDED.email;
UPDATE users SET email = 'bob@example.com' WHERE id = 1;
DELETE FROM users WHERE id = 1;
"#;
    let schema = parse_sql_string(sql).unwrap();
    assert!(schema.tables.contains_key("public.users"));
}
