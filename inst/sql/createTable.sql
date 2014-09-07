CREATE TABLE timeseries_main (ts_key varchar primary key, 
                              ts_data hstore, 
                              ts_frequency timestamptz,
                              md_generated_by varchar,
                              md_generated_on varchar);
