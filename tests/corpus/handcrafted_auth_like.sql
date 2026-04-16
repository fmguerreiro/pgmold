-- Source: hand-crafted for pgmold
-- Commit: n/a
-- License: Apache-2.0
-- Stresses: UUID PKs, complex FK chains, multi-schema, ALTER TABLE ADD CONSTRAINT, partial indexes; inspired by Supabase auth schema patterns (Apache-2.0)

CREATE SCHEMA IF NOT EXISTS auth;

CREATE TABLE auth.users (
    instance_id             UUID,
    id                      UUID        NOT NULL,
    aud                     VARCHAR(255),
    role                    VARCHAR(255),
    email                   VARCHAR(255),
    encrypted_password      VARCHAR(255),
    email_confirmed_at      TIMESTAMPTZ,
    invited_at              TIMESTAMPTZ,
    confirmation_token      VARCHAR(255),
    confirmation_sent_at    TIMESTAMPTZ,
    recovery_token          VARCHAR(255),
    recovery_sent_at        TIMESTAMPTZ,
    last_sign_in_at         TIMESTAMPTZ,
    raw_app_meta_data       JSONB,
    raw_user_meta_data      JSONB,
    is_super_admin          BOOLEAN,
    created_at              TIMESTAMPTZ,
    updated_at              TIMESTAMPTZ,
    phone                   TEXT,
    phone_confirmed_at      TIMESTAMPTZ,
    phone_change            TEXT        NOT NULL DEFAULT '',
    phone_change_token      VARCHAR(255) NOT NULL DEFAULT '',
    phone_change_sent_at    TIMESTAMPTZ,
    email_change_token_current VARCHAR(255) NOT NULL DEFAULT '',
    email_change_confirm_status SMALLINT   NOT NULL DEFAULT 0,
    banned_until            TIMESTAMPTZ,
    reauthentication_token  VARCHAR(255) NOT NULL DEFAULT '',
    reauthentication_sent_at TIMESTAMPTZ,
    is_sso_user             BOOLEAN      NOT NULL DEFAULT FALSE,
    deleted_at              TIMESTAMPTZ,
    is_anonymous            BOOLEAN      NOT NULL DEFAULT FALSE,
    PRIMARY KEY (id)
);

CREATE TABLE auth.sessions (
    id              UUID        NOT NULL,
    user_id         UUID        NOT NULL,
    created_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ,
    factor_id       UUID,
    aal             TEXT,
    not_after       TIMESTAMPTZ,
    refreshed_at    TIMESTAMP,
    user_agent      TEXT,
    ip              INET,
    tag             TEXT,
    PRIMARY KEY (id),
    CONSTRAINT sessions_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES auth.users (id) ON DELETE CASCADE
);

CREATE TABLE auth.refresh_tokens (
    instance_id UUID,
    id          BIGSERIAL   NOT NULL,
    token       VARCHAR(255),
    user_id     VARCHAR(255),
    revoked     BOOLEAN,
    created_at  TIMESTAMPTZ,
    updated_at  TIMESTAMPTZ,
    parent      VARCHAR(255),
    session_id  UUID,
    PRIMARY KEY (id)
);

ALTER TABLE auth.refresh_tokens
    ADD CONSTRAINT refresh_tokens_session_id_fkey
    FOREIGN KEY (session_id) REFERENCES auth.sessions (id) ON DELETE CASCADE;

CREATE TABLE auth.mfa_factors (
    id              UUID        NOT NULL,
    user_id         UUID        NOT NULL,
    friendly_name   TEXT,
    factor_type     TEXT        NOT NULL,
    status          TEXT        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL,
    secret          TEXT,
    phone           TEXT,
    last_challenged_at TIMESTAMPTZ,
    PRIMARY KEY (id),
    CONSTRAINT mfa_factors_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES auth.users (id) ON DELETE CASCADE
);

CREATE TABLE auth.mfa_challenges (
    id          UUID        NOT NULL,
    factor_id   UUID        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL,
    verified_at TIMESTAMPTZ,
    ip_address  INET        NOT NULL,
    otp_code    TEXT,
    PRIMARY KEY (id),
    CONSTRAINT mfa_challenges_factor_id_fkey
        FOREIGN KEY (factor_id) REFERENCES auth.mfa_factors (id) ON DELETE CASCADE
);

CREATE TABLE auth.mfa_amr_claims (
    id                      UUID NOT NULL,
    session_id              UUID NOT NULL,
    authentication_method   TEXT NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL,
    updated_at              TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT mfa_amr_claims_session_id_fkey
        FOREIGN KEY (session_id) REFERENCES auth.sessions (id) ON DELETE CASCADE
);

ALTER TABLE auth.mfa_amr_claims
    ADD CONSTRAINT mfa_amr_claims_session_id_authentication_method_pkey
    UNIQUE (session_id, authentication_method);

CREATE TABLE auth.sso_providers (
    id              UUID    NOT NULL,
    resource_id     TEXT,
    created_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ,
    PRIMARY KEY (id)
);

CREATE TABLE auth.sso_domains (
    id              UUID    NOT NULL,
    sso_provider_id UUID    NOT NULL,
    domain          TEXT    NOT NULL,
    created_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ,
    PRIMARY KEY (id),
    CONSTRAINT sso_domains_sso_provider_id_fkey
        FOREIGN KEY (sso_provider_id) REFERENCES auth.sso_providers (id) ON DELETE CASCADE
);

CREATE TABLE auth.saml_providers (
    id                  UUID    NOT NULL,
    sso_provider_id     UUID    NOT NULL,
    entity_id           TEXT    NOT NULL,
    metadata_xml        TEXT    NOT NULL,
    metadata_url        TEXT,
    attribute_mapping   JSONB,
    created_at          TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ,
    name_id_format      TEXT,
    PRIMARY KEY (id),
    CONSTRAINT saml_providers_sso_provider_id_fkey
        FOREIGN KEY (sso_provider_id) REFERENCES auth.sso_providers (id) ON DELETE CASCADE
);

CREATE TABLE auth.flow_state (
    id                      UUID    NOT NULL,
    user_id                 UUID,
    auth_code               TEXT    NOT NULL,
    code_challenge_method   TEXT    NOT NULL,
    code_challenge          TEXT    NOT NULL,
    provider_type           TEXT    NOT NULL,
    provider_access_token   TEXT,
    provider_refresh_token  TEXT,
    created_at              TIMESTAMPTZ,
    updated_at              TIMESTAMPTZ,
    authentication_method   TEXT    NOT NULL,
    auth_code_issued_at     TIMESTAMPTZ,
    PRIMARY KEY (id)
);

CREATE INDEX users_instance_id_idx ON auth.users (instance_id);
CREATE INDEX users_email_partial_key ON auth.users (email) WHERE is_sso_user = FALSE;
CREATE INDEX sessions_user_id_idx ON auth.sessions (user_id);
CREATE INDEX sessions_not_after_idx ON auth.sessions (not_after DESC);
CREATE INDEX refresh_tokens_instance_id_idx ON auth.refresh_tokens (instance_id);
CREATE INDEX refresh_tokens_session_id_revoked_idx ON auth.refresh_tokens (session_id) WHERE revoked = FALSE;
CREATE INDEX mfa_factors_user_id_idx ON auth.mfa_factors (user_id);
CREATE INDEX sso_domains_domain_idx ON auth.sso_domains (lower(domain));
CREATE INDEX saml_providers_sso_provider_id_idx ON auth.saml_providers (sso_provider_id);
CREATE INDEX flow_state_created_at_idx ON auth.flow_state (created_at DESC);
