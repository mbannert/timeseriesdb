CREATE TABLE IF NOT EXISTS rls_main(
  id TEXT PRIMARY KEY,
  data INTEGER,
  access TEXT
);

ALTER TABLE rls_main ENABLE ROW LEVEL SECURITY;

CREATE POLICY rls_policy ON rls_main
    USING (pg_has_role(access, 'usage'));

CREATE TABLE IF NOT EXISTS rls_sub(
  fk_id TEXT,
  sub_data INTEGER,
  FOREIGN KEY (fk_id) REFERENCES rls_main(id)
);

INSERT INTO rls_main VALUES
('key1', 1, 'access_public'),
('key2', 2, 'access_public'),
('key3', 3, 'access_public'),
('rkey1', 1, 'access_restricted'),
('rkey2', 2, 'access_restricted');

INSERT INTO rls_sub VALUES
('key1', 11),
('key1', 12),
('key2', 22),
('rkey1', 110);

CREATE ROLE access_public;
CREATE ROLE access_restricted;
GRANT access_public TO access_restricted;

CREATE ROLE johnny_public;
GRANT access_public TO johnny_public;
CREATE ROLE alexandra_maine;
GRANT access_restricted TO alexandra_maine;

GRANT SELECT ON rls_main TO johnny_public;
GRANT SELECT ON rls_sub TO johnny_public;
GRANT SELECT ON rls_main TO alexandra_maine;
GRANT SELECT ON rls_sub TO alexandra_maine;
