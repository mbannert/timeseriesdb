http://jmoiron.net/blog/thoughts-on-timeseries-databases/


- hardware infos: http://highscalability.com/blog/2013/6/13/busting-4-modern-hardware-myths-are-memory-hdds-and-ssds-rea.html

## timescale

- timescale talk: https://www.youtube.com/watch?v=eQKbbCg0NqE
- drops chunks, avoids vacuums


## postgres 

POSTGRES AND JSON: https://www.postgresql.org/docs/11/functions-json.html
https://community.rstudio.com/t/inserting-json-objects-in-postgres-table/1705/2
- auto vacuum, check bulk deletes and updates, reclaim space...

https://www.rdocumentation.org/packages/DBI/versions/0.5-1/topics/dbWithTransaction

creating a docker-managed volume (instead of a bind mount):
`docker volume create pg11`

```docker
docker run --rm --name pg -p 1111:5432 -e POSTGRES_PASSWORD=pgpass -d -v pg11:/var/lib/postgresql/data postgres:11


docker container ls

psql -p 1111 -h 'localhost' -d postgres -U postgres
```

run with logging enabled:
https://github.com/docker-library/docs/tree/master/postgres
https://stackoverflow.com/questions/722221/how-to-log-postgresql-queries

pay attenshun w/ disk space tho.

```
docker run --rm --name pg -p 1111:5432 -e POSTGRES_PASSWORD=pgpass -d -v pg11:/var/lib/postgresql/data postgres:11 \
  -c "log_directory=pg_log" -c "log_filename=postgresql-%Y-%m-%d_%H%M%S.log" -c "log_statement=all" \
  -c "logging_collector=on"
  
docker cp pg:/var/lib/postgresql/data/pg_log/ /c/sandbox/pg11
```

It should appear that RPostgres executes one insert per parameter tuple... Would be neat to talk to krlmlr about these things maybe.
Or what does this log output mean:
```
2019-10-09 08:52:39.987 UTC [41] LOG:  execute <unnamed>: 
	            INSERT INTO meta_locale_col VALUES ($1, $2, $3)
	            
2019-10-09 08:52:39.987 UTC [41] DETAIL:  parameters: $1 = '1', $2 = 'de', $3 = '{"field1": "some such", "field2": "Cthulhu f''tagn!"}'
2019-10-09 08:52:39.993 UTC [41] LOG:  execute <unnamed>: 
	            INSERT INTO meta_locale_col VALUES ($1, $2, $3)
	            
2019-10-09 08:52:39.993 UTC [41] DETAIL:  parameters: $1 = '1', $2 = 'en', $3 = '{"field1": "some such", "field2": "Cthulhu f''tagn!"}'
2019-10-09 08:52:39.995 UTC [41] LOG:  execute <unnamed>: 
	            INSERT INTO meta_locale_col VALUES ($1, $2, $3)
	            
2019-10-09 08:52:39.995 UTC [41] DETAIL:  parameters: $1 = '1', $2 = 'fr', $3 = '{"field1": "some such", "field2": "Cthulhu f''tagn!"}'

...
```

## Some Partition notes


```sql
CREATE TABLE timeseries.timeseries_fvu (LIKE timeseries.timeseries);

ALTER TABLE timeseries.timeseries
ATTACH PARTITION timeseries.timeseries_fvu FOR VALUES IN ('fvu');
```


## Row Level Security

https://www.postgresql.org/docs/11/ddl-rowsecurity.html


