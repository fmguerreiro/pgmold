-- Source: hand-crafted for pgmold
-- Commit: n/a
-- License: Apache-2.0
-- Stresses: trigger functions using IS DISTINCT FROM, BEFORE/AFTER triggers, row-level and statement-level

CREATE TABLE public.accounts (
    id          BIGSERIAL   NOT NULL,
    email       TEXT        NOT NULL,
    status      TEXT        NOT NULL DEFAULT 'active',
    balance     NUMERIC(15, 2) NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id),
    CONSTRAINT accounts_email_unique UNIQUE (email),
    CONSTRAINT accounts_balance_nonneg CHECK (balance >= 0)
);

CREATE TABLE public.account_audit (
    id          BIGSERIAL   NOT NULL,
    account_id  BIGINT      NOT NULL,
    field       TEXT        NOT NULL,
    old_value   TEXT,
    new_value   TEXT,
    changed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id)
);

CREATE FUNCTION public.account_audit_fn()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.status IS DISTINCT FROM OLD.status THEN
        INSERT INTO public.account_audit (account_id, field, old_value, new_value)
        VALUES (NEW.id, 'status', OLD.status, NEW.status);
    END IF;

    IF NEW.balance IS DISTINCT FROM OLD.balance THEN
        INSERT INTO public.account_audit (account_id, field, old_value, new_value)
        VALUES (NEW.id, 'balance', OLD.balance::TEXT, NEW.balance::TEXT);
    END IF;

    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$;

CREATE TRIGGER account_changes
BEFORE UPDATE ON public.accounts
FOR EACH ROW
EXECUTE FUNCTION public.account_audit_fn();

CREATE FUNCTION public.account_email_normalize_fn()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.email := lower(NEW.email);
    RETURN NEW;
END;
$$;

CREATE TRIGGER account_email_normalize
BEFORE INSERT OR UPDATE ON public.accounts
FOR EACH ROW
EXECUTE FUNCTION public.account_email_normalize_fn();
