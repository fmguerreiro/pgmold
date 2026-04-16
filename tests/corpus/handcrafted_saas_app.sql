-- Source: hand-crafted for pgmold
-- Commit: n/a
-- License: Apache-2.0
-- Stresses: realistic SaaS schema with enums, FK chains, RLS, triggers, views, partial indexes

CREATE TYPE public.org_plan AS ENUM ('free', 'starter', 'pro', 'enterprise');
CREATE TYPE public.member_role AS ENUM ('owner', 'admin', 'member', 'viewer');
CREATE TYPE public.ticket_status AS ENUM ('open', 'in_progress', 'resolved', 'closed');
CREATE TYPE public.ticket_priority AS ENUM ('low', 'medium', 'high', 'critical');

CREATE TABLE public.organizations (
    id          BIGSERIAL        NOT NULL,
    slug        TEXT             NOT NULL,
    name        TEXT             NOT NULL,
    plan        public.org_plan  NOT NULL DEFAULT 'free',
    created_at  TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id),
    CONSTRAINT organizations_slug_unique UNIQUE (slug)
);

CREATE TABLE public.members (
    id              BIGSERIAL           NOT NULL,
    org_id          BIGINT              NOT NULL,
    email           TEXT                NOT NULL,
    role            public.member_role  NOT NULL DEFAULT 'member',
    invited_at      TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
    accepted_at     TIMESTAMPTZ,
    PRIMARY KEY (id),
    CONSTRAINT members_org_email_unique UNIQUE (org_id, email),
    CONSTRAINT members_org_id_fkey
        FOREIGN KEY (org_id) REFERENCES public.organizations (id) ON DELETE CASCADE
);

CREATE TABLE public.projects (
    id          BIGSERIAL NOT NULL,
    org_id      BIGINT    NOT NULL,
    name        TEXT      NOT NULL,
    description TEXT,
    archived_at TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id),
    CONSTRAINT projects_org_name_unique UNIQUE (org_id, name),
    CONSTRAINT projects_org_id_fkey
        FOREIGN KEY (org_id) REFERENCES public.organizations (id) ON DELETE CASCADE
);

CREATE TABLE public.tickets (
    id          BIGSERIAL              NOT NULL,
    project_id  BIGINT                 NOT NULL,
    reporter_id BIGINT                 NOT NULL,
    assignee_id BIGINT,
    title       TEXT                   NOT NULL,
    body        TEXT,
    status      public.ticket_status   NOT NULL DEFAULT 'open',
    priority    public.ticket_priority NOT NULL DEFAULT 'medium',
    due_at      TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ,
    created_at  TIMESTAMPTZ            NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ            NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id),
    CONSTRAINT tickets_project_id_fkey
        FOREIGN KEY (project_id) REFERENCES public.projects (id) ON DELETE CASCADE,
    CONSTRAINT tickets_reporter_id_fkey
        FOREIGN KEY (reporter_id) REFERENCES public.members (id),
    CONSTRAINT tickets_assignee_id_fkey
        FOREIGN KEY (assignee_id) REFERENCES public.members (id),
    CONSTRAINT tickets_title_non_empty CHECK (char_length(title) > 0)
);

CREATE INDEX tickets_project_id_idx ON public.tickets (project_id);
CREATE INDEX tickets_status_idx ON public.tickets (project_id, status);
CREATE INDEX tickets_assignee_idx ON public.tickets (assignee_id) WHERE assignee_id IS NOT NULL;
CREATE INDEX tickets_open_idx ON public.tickets (project_id, priority, created_at)
    WHERE status IN ('open', 'in_progress');

CREATE TABLE public.comments (
    id          BIGSERIAL NOT NULL,
    ticket_id   BIGINT    NOT NULL,
    author_id   BIGINT    NOT NULL,
    body        TEXT      NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id),
    CONSTRAINT comments_ticket_id_fkey
        FOREIGN KEY (ticket_id) REFERENCES public.tickets (id) ON DELETE CASCADE,
    CONSTRAINT comments_author_id_fkey
        FOREIGN KEY (author_id) REFERENCES public.members (id),
    CONSTRAINT comments_body_non_empty CHECK (char_length(body) > 0)
);

CREATE FUNCTION public.ticket_updated_at_fn()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$;

CREATE TRIGGER ticket_updated_at
BEFORE UPDATE ON public.tickets
FOR EACH ROW
EXECUTE FUNCTION public.ticket_updated_at_fn();

ALTER TABLE public.tickets ENABLE ROW LEVEL SECURITY;

CREATE POLICY tickets_org_access ON public.tickets
FOR ALL
TO public
USING (
    project_id IN (
        SELECT p.id FROM public.projects p
        WHERE p.org_id = current_setting('app.org_id')::BIGINT
    )
);

CREATE VIEW public.open_tickets AS
SELECT
    t.id,
    t.title,
    t.status,
    t.priority,
    t.due_at,
    p.name AS project_name,
    o.slug AS org_slug
FROM public.tickets t
JOIN public.projects p ON p.id = t.project_id
JOIN public.organizations o ON o.id = p.org_id
WHERE t.status IN ('open', 'in_progress')
ORDER BY t.priority DESC, t.created_at;
