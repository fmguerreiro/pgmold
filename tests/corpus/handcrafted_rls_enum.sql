-- Source: hand-crafted for pgmold
-- Commit: n/a
-- License: Apache-2.0
-- Stresses: RLS policies with enum casts, multi-role USING/WITH CHECK expressions

CREATE TYPE public.user_role AS ENUM ('admin', 'editor', 'viewer');

CREATE TABLE public.documents (
    id         BIGSERIAL         NOT NULL,
    owner_id   BIGINT            NOT NULL,
    role_required public.user_role NOT NULL DEFAULT 'viewer',
    content    TEXT              NOT NULL,
    is_public  BOOLEAN           NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ       NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id)
);

ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY;

CREATE POLICY documents_public_read ON public.documents
FOR SELECT
TO public
USING (is_public = TRUE);

CREATE POLICY documents_owner_all ON public.documents
FOR ALL
TO public
USING (owner_id = current_setting('app.current_user_id')::BIGINT)
WITH CHECK (owner_id = current_setting('app.current_user_id')::BIGINT);

CREATE POLICY documents_admin_all ON public.documents
FOR ALL
TO public
USING (
    EXISTS (
        SELECT 1
        FROM public.documents d2
        WHERE d2.role_required = 'admin'::public.user_role
          AND d2.owner_id = current_setting('app.current_user_id')::BIGINT
    )
);
