CREATE TABLE users(
  id bigserial PRIMARY KEY NOT NULL,
  name text NOT NULL,
  created_at bigint NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW()),

  CONSTRAINT name_length CHECK (char_length(name) > 0 AND char_length(name) < 128)
);

CREATE UNIQUE INDEX users_name ON users USING btree(name);
