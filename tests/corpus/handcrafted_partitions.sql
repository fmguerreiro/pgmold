-- Source: hand-crafted for pgmold
-- Commit: n/a
-- License: Apache-2.0
-- Stresses: range-partitioned tables, list-partitioned tables, indexes on partitioned tables

CREATE TABLE public.events (
    id           BIGSERIAL   NOT NULL,
    event_type   TEXT        NOT NULL,
    user_id      BIGINT      NOT NULL,
    payload      JSONB,
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
) PARTITION BY RANGE (occurred_at);

CREATE TABLE public.events_2024_q1 PARTITION OF public.events
    FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');

CREATE TABLE public.events_2024_q2 PARTITION OF public.events
    FOR VALUES FROM ('2024-04-01') TO ('2024-07-01');

CREATE TABLE public.events_2024_q3 PARTITION OF public.events
    FOR VALUES FROM ('2024-07-01') TO ('2024-10-01');

CREATE TABLE public.events_2024_q4 PARTITION OF public.events
    FOR VALUES FROM ('2024-10-01') TO ('2025-01-01');

CREATE TABLE public.events_2025 PARTITION OF public.events
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

CREATE INDEX events_user_id_idx ON public.events (user_id);
CREATE INDEX events_type_idx ON public.events (event_type);

CREATE TABLE public.notifications (
    id         BIGSERIAL NOT NULL,
    channel    TEXT      NOT NULL,
    message    TEXT      NOT NULL,
    sent_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
) PARTITION BY LIST (channel);

CREATE TABLE public.notifications_email PARTITION OF public.notifications
    FOR VALUES IN ('email');

CREATE TABLE public.notifications_sms PARTITION OF public.notifications
    FOR VALUES IN ('sms');

CREATE TABLE public.notifications_push PARTITION OF public.notifications
    FOR VALUES IN ('push');
