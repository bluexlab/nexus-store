CREATE TABLE _migration(
  id bigserial PRIMARY KEY,
  created_at bigint NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW()),
  version bigint NOT NULL,
  CONSTRAINT version CHECK (version >= 1)
);

CREATE UNIQUE INDEX ON _migration USING btree(version);