-- Source: hand-crafted for pgmold
-- Commit: n/a
-- License: Apache-2.0
-- Stresses: GENERATED ALWAYS AS ... STORED computed columns

CREATE TABLE public.products (
    id          BIGSERIAL      NOT NULL,
    price_cents INTEGER        NOT NULL,
    tax_rate    NUMERIC(5, 4)  NOT NULL DEFAULT 0.0800,
    price_usd   NUMERIC(10, 2) GENERATED ALWAYS AS (price_cents / 100.0) STORED,
    tax_amount  NUMERIC(10, 2) GENERATED ALWAYS AS (price_cents / 100.0 * tax_rate) STORED,
    PRIMARY KEY (id)
);

CREATE TABLE public.people (
    id          BIGSERIAL NOT NULL,
    first_name  TEXT      NOT NULL,
    last_name   TEXT      NOT NULL,
    full_name   TEXT      GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED,
    PRIMARY KEY (id)
);
