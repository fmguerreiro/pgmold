-- IGNORE: pgmold-251 exclusion constraints (EXCLUDE USING) are not yet supported by the parser
-- Source: hand-crafted for pgmold
-- Commit: n/a
-- License: Apache-2.0
-- Stresses: EXCLUDE USING gist constraints for range overlap prevention

CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE public.room_bookings (
    id          BIGSERIAL NOT NULL,
    room_id     BIGINT    NOT NULL,
    booked_by   TEXT      NOT NULL,
    during      TSTZRANGE NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT room_bookings_no_overlap
        EXCLUDE USING gist (room_id WITH =, during WITH &&)
);
