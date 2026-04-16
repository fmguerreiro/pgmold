-- Source: hand-crafted for pgmold
-- Commit: n/a
-- License: Apache-2.0
-- Stresses: materialized views, views with CTEs, plpgsql functions with complex logic, SQL functions

CREATE TABLE public.tenants (
    id          BIGSERIAL NOT NULL,
    slug        TEXT      NOT NULL,
    name        TEXT      NOT NULL,
    plan        TEXT      NOT NULL DEFAULT 'free',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id),
    CONSTRAINT tenants_slug_unique UNIQUE (slug)
);

CREATE TABLE public.users (
    id          BIGSERIAL NOT NULL,
    tenant_id   BIGINT    NOT NULL,
    email       TEXT      NOT NULL,
    role        TEXT      NOT NULL DEFAULT 'member',
    last_seen   TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id),
    CONSTRAINT users_tenant_email_unique UNIQUE (tenant_id, email),
    CONSTRAINT users_tenant_id_fkey
        FOREIGN KEY (tenant_id) REFERENCES public.tenants (id) ON DELETE CASCADE
);

CREATE TABLE public.resource_usage (
    id          BIGSERIAL NOT NULL,
    tenant_id   BIGINT    NOT NULL,
    resource    TEXT      NOT NULL,
    quantity    BIGINT    NOT NULL DEFAULT 0,
    period      DATE      NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (id),
    CONSTRAINT resource_usage_tenant_resource_period_unique UNIQUE (tenant_id, resource, period),
    CONSTRAINT resource_usage_tenant_id_fkey
        FOREIGN KEY (tenant_id) REFERENCES public.tenants (id) ON DELETE CASCADE
);

CREATE VIEW public.tenant_user_counts AS
WITH counts AS (
    SELECT
        tenant_id,
        COUNT(*) AS total_users,
        COUNT(*) FILTER (WHERE last_seen > NOW() - INTERVAL '30 days') AS active_users
    FROM public.users
    GROUP BY tenant_id
)
SELECT
    t.id AS tenant_id,
    t.slug,
    t.plan,
    COALESCE(c.total_users, 0) AS total_users,
    COALESCE(c.active_users, 0) AS active_users
FROM public.tenants t
LEFT JOIN counts c ON c.tenant_id = t.id;

CREATE MATERIALIZED VIEW public.monthly_usage_summary AS
SELECT
    tenant_id,
    resource,
    DATE_TRUNC('month', period) AS month,
    SUM(quantity) AS total_quantity
FROM public.resource_usage
GROUP BY tenant_id, resource, DATE_TRUNC('month', period);

CREATE INDEX monthly_usage_summary_tenant_idx ON public.monthly_usage_summary (tenant_id);

CREATE FUNCTION public.tenant_active_users(p_tenant_id BIGINT, p_days INTEGER DEFAULT 30)
RETURNS BIGINT
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_count BIGINT;
BEGIN
    SELECT COUNT(*)
    INTO v_count
    FROM public.users
    WHERE tenant_id = p_tenant_id
      AND last_seen > NOW() - (p_days || ' days')::INTERVAL;
    RETURN COALESCE(v_count, 0);
END;
$$;

CREATE FUNCTION public.slugify(p_text TEXT)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT lower(regexp_replace(trim(p_text), '[^a-zA-Z0-9]+', '-', 'g'));
$$;
