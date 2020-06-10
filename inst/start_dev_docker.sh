docker stop timeseriesdb_dev
docker run --rm -d -p 1111:5432 --name timeseriesdb_dev -e POSTGRES_PASSWORD=pgking  postgres:12.3
echo '8'
sleep 1
echo '7'
sleep 1
echo '6'
sleep 1
echo '5'
sleep 1
echo '4'
sleep 1
echo '3'
sleep 1
echo '2'
sleep 1
echo '1'
sleep 1
PGPASSWORD=pgking psql -p 1111 -h 'localhost' -d postgres -U postgres -f sql/create_roles.sql
PGPASSWORD=pgking psql -p 1111 -h 'localhost' -d postgres -U postgres -f sql/create_extensions.sql
PGPASSWORD=pgking psql -p 1111 -h 'localhost' -d postgres -U postgres -f sql/setup_test_env.sql
R -e "devtools::load_all('../'); install_timeseriesdb('dev_admin', 'dev_admin', 'postgres', 'localhost', 1111, 'timeseries')"
