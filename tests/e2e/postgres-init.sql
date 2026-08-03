-- One relational table, so `profile_table` has a connector that keeps no write
-- history to report on.
CREATE TABLE regions (
    id integer PRIMARY KEY,
    name text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO regions (id, name) VALUES
    (0, 'AFRICA'),
    (1, 'AMERICA'),
    (2, 'ASIA'),
    (3, 'EUROPE'),
    (4, 'MIDDLE EAST');
