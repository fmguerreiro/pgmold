-- Source: hand-crafted for pgmold
-- Commit: n/a
-- License: Apache-2.0
-- Stresses: multi-schema DDL (auth + public), cross-schema foreign keys, sequences, indexes

CREATE SCHEMA IF NOT EXISTS auth;

CREATE TABLE auth.users (
    id              UUID        NOT NULL DEFAULT gen_random_uuid(),
    email           TEXT        NOT NULL,
    encrypted_password TEXT,
    email_confirmed_at TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id),
    CONSTRAINT users_email_unique UNIQUE (email)
);

CREATE TABLE auth.sessions (
    id           UUID        NOT NULL DEFAULT gen_random_uuid(),
    user_id      UUID        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id),
    CONSTRAINT sessions_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES auth.users (id) ON DELETE CASCADE
);

CREATE TABLE auth.refresh_tokens (
    id         BIGSERIAL   NOT NULL,
    token      TEXT        NOT NULL,
    session_id UUID        NOT NULL,
    revoked    BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id),
    CONSTRAINT refresh_tokens_token_unique UNIQUE (token),
    CONSTRAINT refresh_tokens_session_id_fkey
        FOREIGN KEY (session_id) REFERENCES auth.sessions (id) ON DELETE CASCADE
);

CREATE INDEX sessions_user_id_idx ON auth.sessions (user_id);
CREATE INDEX refresh_tokens_session_id_idx ON auth.refresh_tokens (session_id);
CREATE INDEX refresh_tokens_token_idx ON auth.refresh_tokens (token) WHERE revoked = FALSE;

CREATE TABLE public.profiles (
    id          UUID        NOT NULL,
    username    TEXT        NOT NULL,
    avatar_url  TEXT,
    bio         TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id),
    CONSTRAINT profiles_username_unique UNIQUE (username),
    CONSTRAINT profiles_id_fkey
        FOREIGN KEY (id) REFERENCES auth.users (id) ON DELETE CASCADE
);
