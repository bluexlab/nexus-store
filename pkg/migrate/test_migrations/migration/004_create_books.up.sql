CREATE TABLE books(
  id bigserial PRIMARY KEY NOT NULL,
  name text NOT NULL,
  created_at bigint NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())
);
