-- IGNORE: pgmold-252 domain type and sequence default convergence failure
-- Source: hand-crafted for pgmold
-- Commit: n/a
-- License: Apache-2.0
-- Stresses: explicit sequences, custom domains, composite unique constraints, deferred constraints

CREATE SEQUENCE public.invoice_seq
    INCREMENT BY 1
    START WITH 10000
    MINVALUE 10000
    MAXVALUE 9999999
    CACHE 20
    NO CYCLE;

CREATE SEQUENCE public.order_seq
    INCREMENT BY 5
    START WITH 100
    MINVALUE 100
    CACHE 1;

CREATE DOMAIN public.email_address AS TEXT
    CONSTRAINT email_address_format CHECK (VALUE ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$');

CREATE DOMAIN public.positive_money AS NUMERIC(15, 2)
    CONSTRAINT positive_money_check CHECK (VALUE > 0);

CREATE TYPE public.currency AS ENUM ('USD', 'EUR', 'GBP', 'JPY');

CREATE TABLE public.invoices (
    id           BIGINT          NOT NULL DEFAULT nextval('public.invoice_seq'),
    invoice_no   TEXT            NOT NULL,
    customer_ref TEXT            NOT NULL,
    currency     public.currency NOT NULL DEFAULT 'USD',
    amount       public.positive_money NOT NULL,
    issued_at    DATE            NOT NULL DEFAULT CURRENT_DATE,
    due_at       DATE            NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT invoices_invoice_no_unique UNIQUE (invoice_no),
    CONSTRAINT invoices_due_after_issued CHECK (due_at >= issued_at)
);

CREATE INDEX invoices_customer_ref_idx ON public.invoices (customer_ref);
CREATE INDEX invoices_issued_at_idx ON public.invoices (issued_at DESC);
