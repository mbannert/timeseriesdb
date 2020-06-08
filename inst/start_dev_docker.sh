docker stop timeseriesdb_dev
docker run --rm -d -p 1111:5432 --name timeseriesdb_dev -e POSTGRES_PASSWORD=pgking  postgres:12.3
sleep 8
PGPASSWORD=pgking psql -p 1111 -h 'localhost' -d postgres -U postgres -f sql/create_roles.sql
PGPASSWORD=pgking psql -p 1111 -h 'localhost' -d postgres -U postgres -f sql/create_extensions.sql
PGPASSWORD=pgking psql -p 1111 -h 'localhost' -d postgres -U postgres -f sql/setup_test_env.sql
PGPASSWORD=dev_admin psql -p 1111 -h 'localhost' -d postgres -U dev_admin -f sql/create_tables.sql
PGPASSWORD=dev_admin psql -p 1111 -h 'localhost' -d postgres -U dev_admin -f sql/create_functions_collections.sql
PGPASSWORD=dev_admin psql -p 1111 -h 'localhost' -d postgres -U dev_admin -f sql/create_functions_datasets.sql
PGPASSWORD=dev_admin psql -p 1111 -h 'localhost' -d postgres -U dev_admin -f sql/create_functions_metadata.sql
PGPASSWORD=dev_admin psql -p 1111 -h 'localhost' -d postgres -U dev_admin -f sql/create_functions_ts.sql
PGPASSWORD=dev_admin psql -p 1111 -h 'localhost' -d postgres -U dev_admin -f sql/create_triggers.sql
PGPASSWORD=dev_admin psql -p 1111 -h 'localhost' -d postgres -U dev_admin -f sql/grant_rights.sql

