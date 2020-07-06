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
# I leave it as an excercise for future generations to get this to work on git bash under windows
#  PGPASSWORD=pgking sed 's/timeseries/banaan/g' sql/create_roles.sql | psql -p 1111 -h 'localhost' -d postgres -U postgres

sed 's/timeseries/tsdb_test/g' sql/create_roles.sql > roles.sql
PGPASSWORD=pgking psql -p 1111 -h 'localhost' -d postgres -U postgres -f roles.sql
rm roles.sql
sed 's/timeseries/tsdb_test/g' sql/create_extensions.sql > extensions.sql
PGPASSWORD=pgking psql -p 1111 -h 'localhost' -d postgres -U postgres -f extensions.sql
rm extensions.sql
sed 's/timeseries/tsdb_test/g' sql/create_dev_admin.sql > admin.sql
PGPASSWORD=pgking psql -p 1111 -h 'localhost' -d postgres -U postgres -f admin.sql
rm admin.sql
R -e "devtools::load_all('../'); install_timeseriesdb('dev_admin', 'dev_admin', 'postgres', 'localhost', 1111, 'tsdb_test')"
sed 's/timeseries/tsdb_test/g' sql/finalize_dev_env.sql > fin.sql
PGPASSWORD=pgking psql -p 1111 -h 'localhost' -d postgres -U postgres -f fin.sql
rm fin.sql
