CREATE TABLE timeseries_main (ts_key varchar primary key, 
                              ts_data hstore, 
                              ts_frequency integer,
                              md_generated_by varchar,
                              md_generated_on timestamptz);

CREATE TABLE meta_data_unlocalized (ts_key varchar,
                                    meta_data hstore,
                                    primary key (ts_key),
                                    foreign key (ts_key) references timeseries_main (ts_key)
                                    );

CREATE TABLE meta_data_localized (ts_key varchar,
                                  locale_info varchar, 
                                  meta_data hstore,
                                  primary key (ts_key, locale_info),
                                  foreign key (ts_key) references timeseries_main (ts_key)
                                  );

