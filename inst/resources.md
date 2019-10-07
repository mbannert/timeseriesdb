http://jmoiron.net/blog/thoughts-on-timeseries-databases/


- hardware infos: http://highscalability.com/blog/2013/6/13/busting-4-modern-hardware-myths-are-memory-hdds-and-ssds-rea.html

## timescale

- timescale talk: https://www.youtube.com/watch?v=eQKbbCg0NqE
- drops chunks, avoids vacuums


## postgres 

POSTGRES AND JSON: https://www.postgresql.org/docs/11/functions-json.html
https://community.rstudio.com/t/inserting-json-objects-in-postgres-table/1705/2
- auto vacuum, check bulk deletes and updates, reclaim space...

```docker
docker run --rm --name pg -p 1111:5432 -e POSTGRES_PASSWORD=pgpass -d postgres

docker container ls

psql -p 1111 -h 'localhost' -d postgres -U postgres
```

## Some Partition notes


```sql
CREATE TABLE timeseries.timeseries_fvu (LIKE timeseries.timeseries);

ALTER TABLE timeseries.timeseries
ATTACH PARTITION timeseries.timeseries_fvu FOR VALUES IN ('fvu');
```


## Row Level Security

https://www.postgresql.org/docs/11/ddl-rowsecurity.html


