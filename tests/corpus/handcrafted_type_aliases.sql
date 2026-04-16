-- Source: hand-crafted for pgmold
-- Commit: n/a
-- License: Apache-2.0
-- Stresses: PostgreSQL type alias normalization (bool/boolean, int/integer, float8/double precision, text[])

CREATE TABLE public.settings (
    id          BIGSERIAL  NOT NULL,
    key         TEXT       NOT NULL,
    is_enabled  BOOL       NOT NULL DEFAULT TRUE,
    count       INT        NOT NULL DEFAULT 0,
    ratio       FLOAT8     NOT NULL DEFAULT 0.0,
    tags        TEXT[]     NOT NULL DEFAULT '{}',
    metadata    JSONB,
    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id),
    CONSTRAINT settings_key_unique UNIQUE (key)
);

CREATE TABLE public.metrics_raw (
    id          BIGINT     NOT NULL,
    value_int   INT        NOT NULL,
    value_bool  BOOL       NOT NULL DEFAULT FALSE,
    value_float FLOAT8     NOT NULL DEFAULT 0.0,
    labels      TEXT[]     DEFAULT NULL,
    PRIMARY KEY (id)
);

CREATE FUNCTION public.toggle_setting(p_key TEXT)
RETURNS BOOL
LANGUAGE plpgsql
AS $$
DECLARE
    v_result BOOL;
BEGIN
    UPDATE public.settings
    SET is_enabled = NOT is_enabled
    WHERE key = p_key
    RETURNING is_enabled INTO v_result;
    RETURN v_result;
END;
$$;
