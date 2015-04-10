/* 
Create all tables and views necessary for timeseriesdb to work 
Remember to create this inside the schema timeseries for default usage.
*/

CREATE TABLE timeseries_main (ts_key varchar primary key, 
                              ts_data hstore, 
                              ts_frequency integer);

CREATE TABLE meta_data_unlocalized (ts_key varchar,
                                    md_generated_by varchar,
                                    md_resource_last_update timestamptz,
                                    md_coverage_temp varchar,
                                    meta_data hstore,
                                    primary key (ts_key),
                                    foreign key (ts_key) references timeseries_main (ts_key) on delete cascade
                                    );

CREATE TABLE meta_data_localized (ts_key varchar,
                                  locale_info varchar, 
                                  meta_data hstore,
                                  primary key (ts_key, locale_info),
                                  foreign key (ts_key) references timeseries_main (ts_key) on delete cascade
                                  );

CREATE TABLE timeseries_sets (setname varchar,
                              username varchar,
                              tstamp timestamptz,
                              key_set hstore,
                              set_description varchar,
                              active bool,
                              primary key(setname, username)              
                            );


CREATE VIEW v_timeseries_json AS SELECT timeseries_main.ts_key,
    row_to_json(timeseries_main.*) AS ts_json_records
   FROM timeseries_main;

