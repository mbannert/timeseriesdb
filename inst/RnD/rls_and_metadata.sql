DROP TABLE IF EXISTS rls_super CASCADE;
DROP TABLE IF EXISTS rls_main CASCADE;
DROP TABLE IF EXISTS rls_sub CASCADE;

CREATE TABLE rls_super(
  id TEXT PRIMARY KEY
);

CREATE TABLE rls_main(
  id TEXT PRIMARY KEY,
  set TEXT,
  data INTEGER,
  access TEXT,
  FOREIGN KEY(set) REFERENCES rls_super(id) ON DELETE CASCADE
);

ALTER TABLE rls_main ENABLE ROW LEVEL SECURITY;

CREATE POLICY rls_policy ON rls_main
    FOR ALL
    USING (pg_has_role(access, 'usage'))
    WITH CHECK (pg_has_role(access, 'usage'));

CREATE TABLE rls_sub(
  fk_id TEXT,
  sub_data INTEGER,
  FOREIGN KEY (fk_id) REFERENCES rls_main(id) ON DELETE CASCADE
);

INSERT INTO rls_super VALUES
('set1'),
('set2'),
('set3'),
('set4');


INSERT INTO rls_main VALUES
('key1', 'set1', 1, 'access_public'),
('key2', 'set2', 2, 'access_public'),
('key3', 'set2', 3, 'access_public'),
('rkey1', 'set3', 1, 'access_restricted'),
('rkey2', 'set4', 2, 'access_restricted');

INSERT INTO rls_sub VALUES
('key1', 11),
('key1', 12),
('key2', 22),
('rkey1', 110),
('rkey2', 120);

CREATE ROLE access_public;
CREATE ROLE access_restricted;
GRANT access_public TO access_restricted;

CREATE ROLE johnny_public;
GRANT access_public TO johnny_public;
CREATE ROLE alexandra_maine;
GRANT access_restricted TO alexandra_maine;

GRANT SELECT ON rls_super TO johnny_public;
GRANT SELECT ON rls_main TO johnny_public;
GRANT SELECT ON rls_sub TO johnny_public;
GRANT DELETE ON rls_super TO johnny_public;
GRANT DELETE ON rls_main TO johnny_public;
GRANT DELETE ON rls_sub TO johnny_public;
GRANT SELECT ON rls_super TO alexandra_maine;
GRANT SELECT ON rls_main TO alexandra_maine;
GRANT SELECT ON rls_sub TO alexandra_maine;
GRANT DELETE ON rls_super TO alexandra_maine;
GRANT DELETE ON rls_main TO alexandra_maine;
GRANT DELETE ON rls_sub TO alexandra_maine;

SET ROLE johnny_public;
SELECT * FROM rls_main;                  -- This correctly only shows rows with access = 'access_public'
DELETE FROM rls_super WHERE id = 'set3'; -- This should not be allowed as it violates RLS policy
