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
        CREATE INDEX products_name_lower_idx ON public.products (lower(name));
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
        WHERE hired_at IS NOT NULL
        ORDER BY salary DESC;
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
            event_type,
            COUNT(*) AS event_count
        FROM public.events
        GROUP BY event_type;
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
            START WITH 1000
            MINVALUE 1
            MAXVALUE 999999999
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

        GRANT SELECT ON TABLE public.reports TO readonly_role;
        GRANT INSERT ON TABLE public.reports TO readonly_role;
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
        CREATE INDEX tasks_open_idx ON public.tasks (workspace_id, due_date) WHERE (status NOT IN ('done', 'cancelled'));

        CREATE FUNCTION public.count_tasks_by_status(
            p_workspace_id BIGINT,
            p_status       public.task_status
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
              AND status = p_status;
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
        WHERE t.status NOT IN ('done', 'cancelled');

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

#[tokio::test]
async fn function_with_default_null_typed_params() {
    assert_convergence_public(
        r#"
        CREATE FUNCTION public.create_project(
            p_name text,
            p_owner_id uuid DEFAULT NULL,
            p_parent_id integer DEFAULT NULL,
            p_metadata jsonb DEFAULT NULL
        )
        RETURNS void
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RETURN;
        END;
        $$;
        "#,
    )
    .await;
}

#[tokio::test]
async fn function_with_mixed_defaults() {
    assert_convergence_public(
        r#"
        CREATE FUNCTION public.upsert_record(
            p_id uuid,
            p_name text DEFAULT 'unnamed',
            p_count integer DEFAULT 0,
            p_active boolean DEFAULT true,
            p_tags text[] DEFAULT NULL,
            p_config jsonb DEFAULT '{}'::jsonb
        )
        RETURNS void
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RETURN;
        END;
        $$;
        "#,
    )
    .await;
}

#[tokio::test]
async fn view_after_column_drop_recreate() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let initial_sql = r#"
        CREATE TABLE public.items (
            id serial PRIMARY KEY,
            name text NOT NULL,
            description text,
            price numeric(10,2) NOT NULL
        );

        CREATE VIEW public.item_summary AS
        SELECT id, name, price FROM public.items WHERE price > 0;
    "#;

    let target = parse_sql_string(initial_sql).unwrap();
    apply_and_assert_convergence(&connection, &target, &["public"]).await;

    let after_drop_sql = r#"
        CREATE TABLE public.items (
            id serial PRIMARY KEY,
            name text NOT NULL,
            price numeric(10,2) NOT NULL
        );

        CREATE VIEW public.item_summary AS
        SELECT id, name, price FROM public.items WHERE price > 0;
    "#;

    let after_target = parse_sql_string(after_drop_sql).unwrap();
    let current = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let ops = compute_diff(&current, &after_target);
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

    let residual = compute_diff(&after, &after_target);
    assert!(
        residual.is_empty(),
        "Expected zero ops after drop column + view recreate, got {} op(s): {:?}",
        residual.len(),
        residual
    );
}

#[tokio::test]
async fn view_with_explicit_casts_and_coalesce() {
    assert_convergence_public(
        r#"
        CREATE TABLE public.products (
            id serial PRIMARY KEY,
            name text,
            weight numeric,
            category text
        );

        CREATE VIEW public.product_display AS
        SELECT
            id,
            COALESCE(name, 'N/A') AS display_name,
            COALESCE(weight, 0) AS display_weight,
            COALESCE(category, 'uncategorized') AS display_category
        FROM public.products;
        "#,
    )
    .await;
}

#[tokio::test]
async fn function_non_public_schema_with_null_defaults() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE SCHEMA mrv")
        .execute(connection.pool())
        .await
        .unwrap();

    let schema_sql = r#"
        CREATE SCHEMA mrv;

        CREATE FUNCTION mrv.vcs_project_create(
            p_name text,
            p_organization_id uuid DEFAULT NULL,
            p_parent_project_id uuid DEFAULT NULL,
            p_description text DEFAULT NULL,
            p_settings jsonb DEFAULT NULL
        )
        RETURNS uuid
        LANGUAGE plpgsql SECURITY DEFINER
        SET search_path = mrv, public
        AS $$
        DECLARE
            v_id uuid;
        BEGIN
            v_id := gen_random_uuid();
            RETURN v_id;
        END;
        $$;
    "#;

    let target = parse_sql_string(schema_sql).unwrap();
    apply_and_assert_convergence(&connection, &target, &["mrv"]).await;
}

#[tokio::test]
async fn function_with_complex_defaults() {
    assert_convergence_public(
        r#"
        CREATE FUNCTION public.process_batch(
            p_items text[] DEFAULT ARRAY[]::text[],
            p_config jsonb DEFAULT '{"enabled": true}'::jsonb,
            p_limit integer DEFAULT 100,
            p_offset integer DEFAULT 0,
            p_created_after timestamptz DEFAULT now()
        )
        RETURNS void
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RETURN;
        END;
        $$;
        "#,
    )
    .await;
}

#[tokio::test]
async fn view_with_case_and_subquery() {
    assert_convergence_public(
        r#"
        CREATE TABLE public.orders (
            id serial PRIMARY KEY,
            customer_id integer NOT NULL,
            status text NOT NULL DEFAULT 'pending',
            total numeric(12,2) NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now()
        );

        CREATE TABLE public.customers (
            id serial PRIMARY KEY,
            name text NOT NULL,
            tier text DEFAULT 'standard'
        );

        CREATE VIEW public.order_summary AS
        SELECT
            o.id,
            o.total,
            c.name AS customer_name,
            CASE
                WHEN o.total > 1000 THEN 'high'
                WHEN o.total > 100 THEN 'medium'
                ELSE 'low'
            END AS value_tier,
            CASE WHEN o.status = 'completed' THEN true ELSE false END AS is_complete,
            (SELECT COUNT(*) FROM public.orders o2 WHERE o2.customer_id = o.customer_id) AS customer_order_count
        FROM public.orders o
        JOIN public.customers c ON c.id = o.customer_id
        WHERE o.status <> 'cancelled';
        "#,
    )
    .await;
}

#[tokio::test]
async fn view_with_string_concat_and_type_casts() {
    assert_convergence_public(
        r#"
        CREATE TABLE public.users (
            id serial PRIMARY KEY,
            first_name text NOT NULL,
            last_name text NOT NULL,
            age integer,
            score numeric(5,2),
            created_at timestamptz DEFAULT now()
        );

        CREATE VIEW public.user_display AS
        SELECT
            id,
            first_name || ' ' || last_name AS full_name,
            COALESCE(age::text, 'unknown') AS age_display,
            COALESCE(score, 0.0) AS display_score,
            created_at::date AS signup_date
        FROM public.users;
        "#,
    )
    .await;
}

#[tokio::test]
async fn function_returns_table_with_defaults() {
    assert_convergence_public(
        r#"
        CREATE FUNCTION public.search_items(
            p_query text DEFAULT NULL,
            p_limit integer DEFAULT 50,
            p_include_inactive boolean DEFAULT false
        )
        RETURNS TABLE(id integer, name text, score numeric)
        LANGUAGE plpgsql STABLE
        AS $$
        BEGIN
            RETURN QUERY SELECT 1, 'test'::text, 1.0::numeric;
        END;
        $$;
        "#,
    )
    .await;
}

#[tokio::test]
async fn function_with_interval_and_timestamp_defaults() {
    assert_convergence_public(
        r#"
        CREATE FUNCTION public.get_recent_events(
            p_window interval DEFAULT interval '90 days',
            p_since timestamptz DEFAULT now(),
            p_status text DEFAULT 'active'
        )
        RETURNS void
        LANGUAGE plpgsql STABLE
        AS $$
        BEGIN
            RETURN;
        END;
        $$;
        "#,
    )
    .await;
}

#[tokio::test]
async fn view_with_window_functions() {
    assert_convergence_public(
        r#"
        CREATE TABLE public.sales (
            id serial PRIMARY KEY,
            product_id integer NOT NULL,
            amount numeric(12,2) NOT NULL,
            sold_at timestamptz NOT NULL DEFAULT now()
        );

        CREATE VIEW public.sales_ranked AS
        SELECT
            id,
            product_id,
            amount,
            sold_at,
            ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sold_at DESC) AS row_num,
            SUM(amount) OVER (PARTITION BY product_id) AS product_total
        FROM public.sales;
        "#,
    )
    .await;
}

#[tokio::test]
async fn function_with_out_params_and_defaults() {
    assert_convergence_public(
        r#"
        CREATE FUNCTION public.get_user_info(
            p_user_id integer,
            OUT o_name text,
            OUT o_email text
        )
        RETURNS RECORD
        LANGUAGE plpgsql STABLE
        AS $$
        BEGIN
            o_name := 'test';
            o_email := 'test@test.com';
        END;
        $$;
        "#,
    )
    .await;
}

#[tokio::test]
async fn function_sql_with_type_casts_in_body() {
    assert_convergence_public(
        r#"
        CREATE FUNCTION public.format_value(p_val numeric DEFAULT 0)
        RETURNS text
        LANGUAGE sql IMMUTABLE
        AS $$
            SELECT p_val::text || ' units';
        $$;
        "#,
    )
    .await;
}

#[tokio::test]
async fn function_with_returns_table_aliases() {
    assert_convergence_public(
        r#"
        CREATE FUNCTION public.get_stats(
            p_department text DEFAULT NULL
        )
        RETURNS TABLE(
            department_name text,
            employee_count bigint,
            avg_salary numeric
        )
        LANGUAGE plpgsql STABLE
        AS $$
        BEGIN
            RETURN QUERY
            SELECT 'eng'::text, 10::bigint, 95000.00::numeric;
        END;
        $$;
        "#,
    )
    .await;
}

#[tokio::test]
async fn function_with_inout_params() {
    assert_convergence_public(
        r#"
        CREATE FUNCTION public.increment_counter(
            INOUT counter integer,
            step integer DEFAULT 1
        )
        RETURNS integer
        LANGUAGE plpgsql
        AS $$
        BEGIN
            counter := counter + step;
        END;
        $$;
        "#,
    )
    .await;
}

#[tokio::test]
async fn view_with_window_frame_bounds() {
    assert_convergence_public(
        r#"
        CREATE TABLE public.metrics (
            id serial PRIMARY KEY,
            value numeric NOT NULL,
            recorded_at timestamptz NOT NULL DEFAULT now()
        );

        CREATE VIEW public.metrics_moving_avg AS
        SELECT
            id,
            value,
            recorded_at,
            AVG(value) OVER (ORDER BY recorded_at ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg
        FROM public.metrics;
        "#,
    )
    .await;
}

#[tokio::test]
async fn view_with_cte() {
    assert_convergence_public(
        r#"
        CREATE TABLE public.employees (
            id serial PRIMARY KEY,
            name text NOT NULL,
            manager_id integer,
            department text NOT NULL,
            salary numeric(10,2) NOT NULL
        );

        CREATE VIEW public.department_stats AS
        WITH dept_totals AS (
            SELECT
                department,
                COUNT(*) AS employee_count,
                AVG(salary) AS avg_salary
            FROM public.employees
            GROUP BY department
        )
        SELECT
            department,
            employee_count,
            avg_salary
        FROM dept_totals
        WHERE employee_count > 0;
        "#,
    )
    .await;
}

#[tokio::test]
async fn view_with_union() {
    assert_convergence_public(
        r#"
        CREATE TABLE public.active_users (
            id serial PRIMARY KEY,
            name text NOT NULL,
            email text NOT NULL
        );

        CREATE TABLE public.archived_users (
            id serial PRIMARY KEY,
            name text NOT NULL,
            email text NOT NULL,
            archived_at timestamptz NOT NULL DEFAULT now()
        );

        CREATE VIEW public.all_users AS
        SELECT id, name, email, 'active' AS status FROM public.active_users
        UNION ALL
        SELECT id, name, email, 'archived' AS status FROM public.archived_users;
        "#,
    )
    .await;
}

#[tokio::test]
async fn unique_constraint_inline() {
    assert_convergence_public(
        r#"
        CREATE TABLE public.users (
            id          BIGSERIAL NOT NULL,
            email       TEXT      NOT NULL,
            username    TEXT      NOT NULL,
            PRIMARY KEY (id),
            CONSTRAINT users_email_unique UNIQUE (email)
        );
        "#,
    )
    .await;
}

const UNIQUE_CONSTRAINT_ALTER_TABLE_SQL: &str = r#"
    CREATE SCHEMA IF NOT EXISTS auth;

    CREATE TABLE auth.mfa_amr_claims (
        id                      UUID NOT NULL PRIMARY KEY,
        session_id              UUID NOT NULL,
        authentication_method   TEXT NOT NULL
    );
    ALTER TABLE auth.mfa_amr_claims
        ADD CONSTRAINT mfa_amr_claims_session_id_authentication_method_pkey
        UNIQUE (session_id, authentication_method);
"#;

#[tokio::test]
async fn unique_constraint_via_alter_table() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE SCHEMA auth")
        .execute(connection.pool())
        .await
        .unwrap();

    let target = parse_sql_string(UNIQUE_CONSTRAINT_ALTER_TABLE_SQL).unwrap();
    apply_and_assert_convergence(&connection, &target, &["auth"]).await;
}

#[tokio::test]
async fn unique_constraint_dump_round_trip() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE SCHEMA auth")
        .execute(connection.pool())
        .await
        .unwrap();

    let target = parse_sql_string(UNIQUE_CONSTRAINT_ALTER_TABLE_SQL).unwrap();
    apply_and_assert_convergence(&connection, &target, &["auth"]).await;

    let db_schema = introspect_schema(&connection, &["auth".to_string()], false)
        .await
        .unwrap();
    let dump_output = generate_dump(&db_schema, None);
    let reparsed = parse_sql_string(&dump_output).unwrap();

    let diff = compute_diff(&db_schema, &reparsed);
    assert!(
        diff.is_empty(),
        "Dump round-trip should produce zero diff. Got {} op(s): {:?}\nDump output:\n{}",
        diff.len(),
        diff,
        dump_output
    );

    let reparsed_table = reparsed.tables.get("auth.mfa_amr_claims").unwrap();
    let constraint_index = reparsed_table
        .indexes
        .iter()
        .find(|i| i.name == "mfa_amr_claims_session_id_authentication_method_pkey")
        .expect("Dump round-trip should preserve the unique constraint");
    assert!(
        constraint_index.is_constraint,
        "Dump round-trip should preserve is_constraint=true, got is_constraint=false"
    );
    assert!(
        !dump_output.contains("CREATE UNIQUE INDEX"),
        "Dump should emit ALTER TABLE ADD CONSTRAINT, not CREATE UNIQUE INDEX.\nDump output:\n{}",
        dump_output
    );
}
