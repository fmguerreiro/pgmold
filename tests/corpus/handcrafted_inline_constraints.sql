-- Source: hand-crafted for pgmold
-- Commit: n/a
-- License: Apache-2.0
-- Stresses: inline column-level UNIQUE, REFERENCES, and CHECK — the class of bug found 2026-04-14

CREATE TABLE public.regions (
    id   BIGSERIAL NOT NULL,
    code TEXT      NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE public.users (
    id         BIGSERIAL   NOT NULL,
    email      TEXT        NOT NULL UNIQUE,
    username   TEXT        NOT NULL,
    region_id  BIGINT      NOT NULL REFERENCES public.regions (id) ON DELETE SET NULL,
    age        INTEGER     CHECK (age >= 0),
    score      NUMERIC     NOT NULL DEFAULT 0 CHECK (score >= 0),
    PRIMARY KEY (id)
);

CREATE TABLE public.posts (
    id         BIGSERIAL NOT NULL,
    author_id  BIGINT    NOT NULL REFERENCES public.users (id) ON DELETE CASCADE,
    slug       TEXT      NOT NULL UNIQUE,
    title      TEXT      NOT NULL CHECK (char_length(title) > 0),
    body       TEXT,
    PRIMARY KEY (id)
);
