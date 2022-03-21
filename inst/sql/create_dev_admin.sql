CREATE ROLE dev_admin WITH LOGIN PASSWORD 'dev_admin'; -- public/private distinction does not really make sense for admins
GRANT timeseries_admin TO dev_admin;
GRANT CREATE, USAGE ON SCHEMA timeseries TO dev_admin;
