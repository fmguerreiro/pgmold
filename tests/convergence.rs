mod common;
use common::*;

async fn apply_and_assert_convergence(
    connection: &PgConnection,
    target: &Schema,
    schemas: &[&str],
) {
    let schema_names: Vec<String> = schemas.iter().map(|s| s.to_string()).collect();

    let empty = introspect_schema(connection, &schema_names, false)
        .await
        .unwrap();

    let sql = generate_sql(&plan_migration(compute_diff(&empty, target)));

    for stmt in &sql {
        sqlx::query(stmt)
            .execute(connection.pool())
            .await
            .unwrap_or_else(|e| panic!("Failed to execute: {stmt}\nError: {e}"));
    }

    let after = introspect_schema(connection, &schema_names, false)
        .await
        .unwrap();

    let second_diff = compute_diff(&after, target);

    assert!(
        second_diff.is_empty(),
        "Expected zero ops after apply, but got {} op(s): {:?}",
        second_diff.len(),
        second_diff
    );
}

async fn assert_convergence_public(schema_sql: &str) {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();
    let target = parse_sql_string(schema_sql).unwrap();
    apply_and_assert_convergence(&connection, &target, &["public"]).await;
}

#[tokio::test]
async fn table_with_columns_and_pk() {
    assert_convergence_public(
        r#"
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
        "#,
    )
    .await;
}

#[tokio::test]
async fn table_with_indexes() {
    assert_convergence_public(
        r#"
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
        "#,
    )
    .await;
}

#[tokio::test]
async fn table_with_foreign_keys() {
    assert_convergence_public(
        r#"
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
        "#,
    )
    .await;
}

#[tokio::test]
async fn table_with_check_constraints() {
    assert_convergence_public(
        r#"
        CREATE TABLE public.orders (
            id       BIGSERIAL      NOT NULL,
            quantity INTEGER        NOT NULL,
            total    NUMERIC(12, 2) NOT NULL,
            status   TEXT           NOT NULL DEFAULT 'pending',
            PRIMARY KEY (id),
            CONSTRAINT orders_quantity_positive CHECK (quantity > 0),
            CONSTRAINT orders_total_non_negative CHECK (total >= 0)
        );
        "#,
    )
    .await;
}

#[tokio::test]
async fn enum_type() {
    assert_convergence_public(
        r#"
        CREATE TYPE public.order_status AS ENUM (
            'pending',
            'processing',
            'shipped',
            'delivered',
            'cancelled',
            'refunded'
        );
        "#,
    )
    .await;
}

#[tokio::test]
async fn function_plpgsql() {
    assert_convergence_public(
        r#"
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
            -- references non-existent table; plpgsql validates at call time, not creation
            SELECT username INTO v_name FROM public.profile WHERE id = p_user_id;
            IF v_name IS NULL THEN
                RETURN p_fallback;
            END IF;
            RETURN v_name;
        END;
        $$;
        "#,
    )
    .await;
}

#[tokio::test]
async fn function_sql() {
    assert_convergence_public(
        r#"
        CREATE FUNCTION public.calculate_discount(p_price NUMERIC, p_rate NUMERIC)
        RETURNS NUMERIC
        LANGUAGE sql
        IMMUTABLE
        AS $$
            SELECT p_price * (1 - p_rate);
        $$;
        "#,
    )
    .await;
}

#[tokio::test]
async fn view() {
    assert_convergence_public(
        r#"
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
        "#,
    )
    .await;
}

#[tokio::test]
async fn materialized_view() {
    assert_convergence_public(
        r#"
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
        "#,
    )
    .await;
}

#[tokio::test]
async fn trigger() {
    assert_convergence_public(
        r#"
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
        "#,
    )
    .await;
}

#[tokio::test]
async fn sequence() {
    assert_convergence_public(
        r#"
        CREATE SEQUENCE public.invoice_number_seq
            INCREMENT BY 1
            CACHE 10;
        "#,
    )
    .await;
}

#[tokio::test]
async fn policy() {
    assert_convergence_public(
        r#"
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
        "#,
    )
    .await;
}

#[tokio::test]
async fn domain() {
    assert_convergence_public(
        r#"
        CREATE DOMAIN public.positive_integer AS INTEGER
            CONSTRAINT positive_integer_check CHECK (VALUE > 0);
        "#,
    )
    .await;
}

#[tokio::test]
async fn partition() {
    assert_convergence_public(
        r#"
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
        "#,
    )
    .await;
}

#[tokio::test]
async fn grants() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE ROLE readonly_role NOLOGIN")
        .execute(connection.pool())
        .await
        .unwrap();

    let target = parse_sql_string(
        r#"
        CREATE TABLE public.reports (
            id      BIGSERIAL NOT NULL,
            title   TEXT      NOT NULL,
            content TEXT,
            PRIMARY KEY (id)
        );

        GRANT SELECT, INSERT ON TABLE public.reports TO readonly_role;
        "#,
    )
    .unwrap();

    let no_dropped_roles = std::collections::HashSet::new();
    let diff_with_grants = |from: &Schema, to: &Schema| -> Vec<MigrationOp> {
        pgmold::diff::compute_diff_with_flags(from, to, false, true, &no_dropped_roles)
    };

    let empty = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let ops = diff_with_grants(&empty, &target);
    let sql = generate_sql(&plan_migration(ops));

    for stmt in &sql {
        sqlx::query(stmt)
            .execute(connection.pool())
            .await
            .unwrap_or_else(|e| panic!("Failed to execute: {stmt}\nError: {e}"));
    }

    let after = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let remaining_grant_ops: Vec<_> = diff_with_grants(&after, &target)
        .into_iter()
        .filter(|op| {
            matches!(
                op,
                MigrationOp::GrantPrivileges { .. } | MigrationOp::RevokePrivileges { .. }
            )
        })
        .collect();

    assert!(
        remaining_grant_ops.is_empty(),
        "Expected zero grant ops after apply, but got {} op(s): {:?}",
        remaining_grant_ops.len(),
        remaining_grant_ops
    );
}

#[tokio::test]
async fn default_privileges() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    for role in ["db_admin", "app_reader"] {
        sqlx::query(&format!("CREATE ROLE {role} NOLOGIN"))
            .execute(connection.pool())
            .await
            .unwrap();
    }

    let target = parse_sql_string(
        r#"
        ALTER DEFAULT PRIVILEGES FOR ROLE db_admin IN SCHEMA public
        GRANT SELECT ON TABLES TO app_reader;
        "#,
    )
    .unwrap();

    let schema_names = vec!["public".to_string()];

    let empty = introspect_schema(&connection, &schema_names, false)
        .await
        .unwrap();

    let sql = generate_sql(&plan_migration(compute_diff(&empty, &target)));

    for stmt in &sql {
        sqlx::query(stmt)
            .execute(connection.pool())
            .await
            .unwrap_or_else(|e| panic!("Failed to execute: {stmt}\nError: {e}"));
    }

    let after = introspect_schema(&connection, &schema_names, false)
        .await
        .unwrap();

    let remaining: Vec<_> = compute_diff(&after, &target)
        .into_iter()
        .filter(|op| matches!(op, MigrationOp::AlterDefaultPrivileges { .. }))
        .collect();

    assert!(
        remaining.is_empty(),
        "Expected zero AlterDefaultPrivileges ops after apply, but got {} op(s): {:?}",
        remaining.len(),
        remaining
    );
}

#[tokio::test]
async fn multi_schema() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    for schema in ["auth", "api"] {
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(connection.pool())
            .await
            .unwrap();
    }

    let target = parse_sql_string(
        r#"
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
        "#,
    )
    .unwrap();

    apply_and_assert_convergence(&connection, &target, &["auth", "api"]).await;
}

#[tokio::test]
async fn complex_combined() {
    assert_convergence_public(
        r#"
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
        "#,
    )
    .await;
}
