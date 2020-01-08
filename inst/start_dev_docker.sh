docker run --rm -d -p 1111:5432  postgres:11
sleep 2
psql -p 1111 -h 'localhost' -d postgres -U postgres -f sql/create_tables.sql