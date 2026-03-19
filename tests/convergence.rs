mod common;
use common::*;

async fn apply_and_assert_convergence(
    connection: &PgConnection,
    target_schema: &Schema,
    schemas: &[&str],
) {
    let schema_names: Vec<String> = schemas.iter().map(|s| s.to_string()).collect();

    let empty = introspect_schema(connection, &schema_names, false)
        .await
        .unwrap();

    let ops = compute_diff(&empty, target_schema);
    let planned = plan_migration(ops);
    let sql = generate_sql(&planned);

    for stmt in &sql {
        sqlx::query(stmt)
            .execute(connection.pool())
            .await
            .unwrap_or_else(|e| panic!("Failed to execute: {stmt}\nError: {e}"));
    }

    let after = introspect_schema(connection, &schema_names, false)
        .await
        .unwrap();

    let second_diff = compute_diff(&after, target_schema);

    assert!(
        second_diff.is_empty(),
        "Expected zero ops after apply, but got {} op(s): {:?}",
        second_diff.len(),
        second_diff
    );
}

#[tokio::test]
async fn table_with_columns_and_pk() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let schema_sql = r#"
        CREATE TABLE public.users (
            id          BIGSERIAL           NOT NULL,
            email       VARCHAR(255)        NOT NULL,
            username    TEXT                NOT NULL,
            is_active   BOOLEAN             NOT NULL DEFAULT TRUE,
            score       INTEGER             NOT NULL DEFAULT 0,
            metadata    JSONB,
            created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
            PRIMARY KEY (id)
        );
    "#;

    let target = parse_sql_string(schema_sql).unwrap();
    apply_and_assert_convergence(&connection, &target, &["public"]).await;
}

#[tokio::test]
async fn table_with_indexes() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let schema_sql = r#"
        CREATE TABLE public.products (
            id          BIGSERIAL NOT NULL,
            sku         TEXT      NOT NULL,
            name        TEXT      NOT NULL,
            price       NUMERIC(12, 2) NOT NULL,
            category    TEXT,
            is_deleted  BOOLEAN   NOT NULL DEFAULT FALSE,
            PRIMARY KEY (id)
        );

        CREATE UNIQUE INDEX products_sku_idx ON public.products (sku);
        CREATE INDEX products_category_idx ON public.products (category);
        CREATE INDEX products_active_idx ON public.products (id) WHERE (is_deleted = FALSE);
    "#;

    let target = parse_sql_string(schema_sql).unwrap();
    apply_and_assert_convergence(&connection, &target, &["public"]).await;
}

#[tokio::test]
async fn table_with_foreign_keys() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let schema_sql = r#"
        CREATE TABLE public.authors (
            id   BIGSERIAL NOT NULL,
            name TEXT      NOT NULL,
            PRIMARY KEY (id)
        );

        CREATE TABLE public.articles (
            id        BIGSERIAL NOT NULL,
            author_id BIGINT    NOT NULL,
            title     TEXT      NOT NULL,
            body      TEXT,
            PRIMARY KEY (id),
            CONSTRAINT articles_author_id_fkey
                FOREIGN KEY (author_id) REFERENCES public.authors (id) ON DELETE CASCADE
        );
    "#;

    let target = parse_sql_string(schema_sql).unwrap();
    apply_and_assert_convergence(&connection, &target, &["public"]).await;
}

#[tokio::test]
async fn table_with_check_constraints() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let schema_sql = r#"
        CREATE TABLE public.orders (
            id       BIGSERIAL      NOT NULL,
            quantity INTEGER        NOT NULL,
            total    NUMERIC(12, 2) NOT NULL,
            status   TEXT           NOT NULL DEFAULT 'pending',
            PRIMARY KEY (id),
            CONSTRAINT orders_quantity_positive CHECK (quantity > 0),
            CONSTRAINT orders_total_non_negative CHECK (total >= 0)
        );
    "#;

    let target = parse_sql_string(schema_sql).unwrap();
    apply_and_assert_convergence(&connection, &target, &["public"]).await;
}

#[tokio::test]
async fn enum_type() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let schema_sql = r#"
        CREATE TYPE public.order_status AS ENUM (
            'pending',
            'processing',
            'shipped',
            'delivered',
            'cancelled',
            'refunded'
        );
    "#;

    let target = parse_sql_string(schema_sql).unwrap();
    apply_and_assert_convergence(&connection, &target, &["public"]).await;
}

#[tokio::test]
async fn function_plpgsql() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let schema_sql = r#"
        CREATE FUNCTION public.get_user_display_name(
            p_user_id BIGINT,
            p_fallback TEXT DEFAULT 'Unknown'
        )
        RETURNS TEXT
        LANGUAGE plpgsql
        STABLE
        SECURITY DEFINER
        SET search_path = public
        AS $$
        DECLARE
            v_name TEXT;
        BEGIN
            SELECT username INTO v_name FROM public.profile WHERE id = p_user_id;
            IF v_name IS NULL THEN
                RETURN p_fallback;
            END IF;
            RETURN v_name;
        END;
        $$;
    "#;

    let target = parse_sql_string(schema_sql).unwrap();
    apply_and_assert_convergence(&connection, &target, &["public"]).await;
}

#[tokio::test]
async fn function_sql() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let schema_sql = r#"
        CREATE FUNCTION public.calculate_discount(p_price NUMERIC, p_rate NUMERIC)
        RETURNS NUMERIC
        LANGUAGE sql
        IMMUTABLE
        AS $$
            SELECT p_price * (1 - p_rate);
        $$;
    "#;

    let target = parse_sql_string(schema_sql).unwrap();
    apply_and_assert_convergence(&connection, &target, &["public"]).await;
}

#[tokio::test]
async fn view() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let schema_sql = r#"
        CREATE TABLE public.employees (
            id         BIGSERIAL NOT NULL,
            first_name TEXT      NOT NULL,
            last_name  TEXT      NOT NULL,
            department TEXT,
            salary     NUMERIC(10, 2),
            hired_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
            PRIMARY KEY (id)
        );

        CREATE VIEW public.active_employees_view AS
        SELECT
            id,
            first_name || ' ' || last_name AS full_name,
            department,
            salary
        FROM public.employees
        WHERE hired_at IS NOT NULL;
    "#;

    let target = parse_sql_string(schema_sql).unwrap();
    apply_and_assert_convergence(&connection, &target, &["public"]).await;
}

#[tokio::test]
async fn materialized_view() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let schema_sql = r#"
        CREATE TABLE public.events (
            id         BIGSERIAL NOT NULL,
            event_type TEXT      NOT NULL,
            user_id    BIGINT    NOT NULL,
            occurred_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
            PRIMARY KEY (id)
        );

        CREATE MATERIALIZED VIEW public.event_counts_mv AS
        SELECT
            id,
            event_type,
            user_id,
            occurred_at
        FROM public.events
        WHERE user_id > 0;
    "#;

    let target = parse_sql_string(schema_sql).unwrap();
    apply_and_assert_convergence(&connection, &target, &["public"]).await;
}

#[tokio::test]
async fn trigger() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let schema_sql = r#"
        CREATE TABLE public.audit_log (
            id         BIGSERIAL NOT NULL,
            table_name TEXT      NOT NULL,
            operation  TEXT      NOT NULL,
            changed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
            PRIMARY KEY (id)
        );

        CREATE TABLE public.customers (
            id    BIGSERIAL NOT NULL,
            email TEXT      NOT NULL,
            name  TEXT      NOT NULL,
            PRIMARY KEY (id)
        );

        CREATE FUNCTION public.customers_audit_fn()
        RETURNS TRIGGER
        LANGUAGE plpgsql
        AS $$
        BEGIN
            INSERT INTO public.audit_log (table_name, operation)
            VALUES ('customers', TG_OP);
            RETURN NEW;
        END;
        $$;

        CREATE TRIGGER customers_audit_trigger
        AFTER INSERT OR UPDATE ON public.customers
        FOR EACH ROW
        EXECUTE FUNCTION public.customers_audit_fn();
    "#;

    let target = parse_sql_string(schema_sql).unwrap();
    apply_and_assert_convergence(&connection, &target, &["public"]).await;
}

#[tokio::test]
async fn sequence() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let schema_sql = r#"
        CREATE SEQUENCE public.invoice_number_seq
            INCREMENT BY 1
            CACHE 10;
    "#;

    let target = parse_sql_string(schema_sql).unwrap();
    apply_and_assert_convergence(&connection, &target, &["public"]).await;
}

#[tokio::test]
async fn policy() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE ROLE app_user NOLOGIN")
        .execute(connection.pool())
        .await
        .unwrap();

    let schema_sql = r#"
        CREATE TABLE public.documents (
            id      BIGSERIAL NOT NULL,
            owner   TEXT      NOT NULL DEFAULT current_user,
            content TEXT      NOT NULL,
            PRIMARY KEY (id)
        );

        ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY;

        CREATE POLICY documents_owner_select ON public.documents
        FOR SELECT
        TO public
        USING (owner = current_user);

        CREATE POLICY documents_owner_insert ON public.documents
        FOR INSERT
        TO public
        WITH CHECK (owner = current_user);
    "#;

    let target = parse_sql_string(schema_sql).unwrap();
    apply_and_assert_convergence(&connection, &target, &["public"]).await;
}

#[tokio::test]
async fn domain() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let schema_sql = r#"
        CREATE DOMAIN public.positive_integer AS INTEGER
            CONSTRAINT positive_integer_check CHECK (VALUE > 0);
    "#;

    let target = parse_sql_string(schema_sql).unwrap();
    apply_and_assert_convergence(&connection, &target, &["public"]).await;
}

#[tokio::test]
async fn partition() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let schema_sql = r#"
        CREATE TABLE public.transactions (
            id              BIGSERIAL NOT NULL,
            amount          NUMERIC(12, 2) NOT NULL,
            transaction_date DATE NOT NULL,
            description     TEXT
        ) PARTITION BY RANGE (transaction_date);

        CREATE TABLE public.transactions_2024 PARTITION OF public.transactions
            FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

        CREATE TABLE public.transactions_2025 PARTITION OF public.transactions
            FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
    "#;

    let target = parse_sql_string(schema_sql).unwrap();
    apply_and_assert_convergence(&connection, &target, &["public"]).await;
}

#[tokio::test]
async fn grants() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE ROLE readonly_role NOLOGIN")
        .execute(connection.pool())
        .await
        .unwrap();

    let schema_sql = r#"
        CREATE TABLE public.reports (
            id      BIGSERIAL NOT NULL,
            title   TEXT      NOT NULL,
            content TEXT,
            PRIMARY KEY (id)
        );

        GRANT SELECT, INSERT ON TABLE public.reports TO readonly_role;
    "#;

    let target = parse_sql_string(schema_sql).unwrap();

    let empty = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let ops = pgmold::diff::compute_diff_with_flags(
        &empty,
        &target,
        false,
        true,
        &std::collections::HashSet::new(),
    );
    let planned = plan_migration(ops);
    let sql = generate_sql(&planned);

    for stmt in &sql {
        sqlx::query(stmt)
            .execute(connection.pool())
            .await
            .unwrap_or_else(|e| panic!("Failed to execute: {stmt}\nError: {e}"));
    }

    let after = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let second_diff = pgmold::diff::compute_diff_with_flags(
        &after,
        &target,
        false,
        true,
        &std::collections::HashSet::new(),
    );
    let grant_ops: Vec<_> = second_diff
        .iter()
        .filter(|op| {
            matches!(
                op,
                MigrationOp::GrantPrivileges { .. } | MigrationOp::RevokePrivileges { .. }
            )
        })
        .collect();

    assert!(
        grant_ops.is_empty(),
        "Expected zero grant ops after apply, but got {} op(s): {:?}",
        grant_ops.len(),
        grant_ops
    );
}

#[tokio::test]
async fn default_privileges() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE ROLE db_admin NOLOGIN")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE ROLE app_reader NOLOGIN")
        .execute(connection.pool())
        .await
        .unwrap();

    let schema_sql = r#"
        ALTER DEFAULT PRIVILEGES FOR ROLE db_admin IN SCHEMA public
        GRANT SELECT ON TABLES TO app_reader;
    "#;

    let target = parse_sql_string(schema_sql).unwrap();

    let empty = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let ops = compute_diff(&empty, &target);
    let planned = plan_migration(ops);
    let sql = generate_sql(&planned);

    for stmt in &sql {
        sqlx::query(stmt)
            .execute(connection.pool())
            .await
            .unwrap_or_else(|e| panic!("Failed to execute: {stmt}\nError: {e}"));
    }

    let after = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let second_diff = compute_diff(&after, &target);
    let adp_ops: Vec<_> = second_diff
        .iter()
        .filter(|op| matches!(op, MigrationOp::AlterDefaultPrivileges { .. }))
        .collect();

    assert!(
        adp_ops.is_empty(),
        "Expected zero AlterDefaultPrivileges ops after apply, but got {} op(s): {:?}",
        adp_ops.len(),
        adp_ops
    );
}

#[tokio::test]
async fn multi_schema() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE SCHEMA auth")
        .execute(connection.pool())
        .await
        .unwrap();
    sqlx::query("CREATE SCHEMA api")
        .execute(connection.pool())
        .await
        .unwrap();

    let schema_sql = r#"
        CREATE SCHEMA IF NOT EXISTS auth;
        CREATE SCHEMA IF NOT EXISTS api;

        CREATE TABLE auth.principals (
            id       BIGSERIAL NOT NULL,
            email    TEXT      NOT NULL UNIQUE,
            password TEXT      NOT NULL,
            PRIMARY KEY (id)
        );

        CREATE TABLE api.profiles (
            id           BIGSERIAL NOT NULL,
            principal_id BIGINT    NOT NULL,
            display_name TEXT      NOT NULL,
            bio          TEXT,
            PRIMARY KEY (id),
            CONSTRAINT profiles_principal_id_fkey
                FOREIGN KEY (principal_id) REFERENCES auth.principals (id) ON DELETE CASCADE
        );

        CREATE INDEX profiles_principal_idx ON api.profiles (principal_id);
    "#;

    let target = parse_sql_string(schema_sql).unwrap();
    apply_and_assert_convergence(&connection, &target, &["auth", "api"]).await;
}

#[tokio::test]
async fn complex_combined() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE ROLE web_user NOLOGIN")
        .execute(connection.pool())
        .await
        .unwrap();

    let schema_sql = r#"
        CREATE TYPE public.task_status AS ENUM (
            'todo',
            'in_progress',
            'done',
            'cancelled'
        );

        CREATE TABLE public.workspaces (
            id   BIGSERIAL NOT NULL,
            name TEXT      NOT NULL,
            PRIMARY KEY (id)
        );

        CREATE TABLE public.tasks (
            id           BIGSERIAL         NOT NULL,
            workspace_id BIGINT            NOT NULL,
            title        TEXT              NOT NULL,
            status       public.task_status NOT NULL DEFAULT 'todo',
            assignee     TEXT,
            due_date     DATE,
            metadata     JSONB,
            created_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
            PRIMARY KEY (id),
            CONSTRAINT tasks_workspace_id_fkey
                FOREIGN KEY (workspace_id) REFERENCES public.workspaces (id) ON DELETE CASCADE,
            CONSTRAINT tasks_title_non_empty CHECK (char_length(title) > 0)
        );

        CREATE INDEX tasks_workspace_idx ON public.tasks (workspace_id);
        CREATE INDEX tasks_status_idx ON public.tasks (status);
        CREATE INDEX tasks_open_idx ON public.tasks (workspace_id, due_date) WHERE (status <> 'done' AND status <> 'cancelled');

        CREATE FUNCTION public.count_tasks_by_status(
            p_workspace_id BIGINT,
            p_status       TEXT
        )
        RETURNS BIGINT
        LANGUAGE plpgsql
        STABLE
        AS $$
        DECLARE
            v_count BIGINT;
        BEGIN
            SELECT COUNT(*) INTO v_count
            FROM public.tasks
            WHERE workspace_id = p_workspace_id
              AND status::TEXT = p_status;
            RETURN v_count;
        END;
        $$;

        CREATE VIEW public.open_tasks_view AS
        SELECT
            t.id,
            t.title,
            t.status,
            t.assignee,
            t.due_date,
            w.name AS workspace_name
        FROM public.tasks t
        JOIN public.workspaces w ON w.id = t.workspace_id
        WHERE t.status <> 'done' AND t.status <> 'cancelled';

        CREATE FUNCTION public.tasks_audit_fn()
        RETURNS TRIGGER
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RETURN NEW;
        END;
        $$;

        CREATE TRIGGER tasks_audit_trigger
        AFTER INSERT OR UPDATE ON public.tasks
        FOR EACH ROW
        EXECUTE FUNCTION public.tasks_audit_fn();

        ALTER TABLE public.tasks ENABLE ROW LEVEL SECURITY;

        CREATE POLICY tasks_workspace_access ON public.tasks
        FOR ALL
        TO public
        USING (workspace_id IS NOT NULL);
    "#;

    let target = parse_sql_string(schema_sql).unwrap();
    apply_and_assert_convergence(&connection, &target, &["public"]).await;
}
