ALTER TABLE users
ADD COLUMN email text;

ALTER TABLE users
ADD CONSTRAINT email_not_null CHECK (email IS NOT NULL);

ALTER TABLE users
ADD CONSTRAINT email_valid CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z]{2,}$');

CREATE UNIQUE INDEX users_email ON users USING btree(email);
