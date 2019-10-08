CREATE SCHEMA timeseries_1_0;

-- links public validity on dataset level to series
CREATE TABLE timeseries_1_0.releases(
    release text,
    ts_validity daterange,
    release_validity tstzrange,
    release_description text,
    primary key (release, ts_validity)
);

-- store different versions of time series
CREATE TABLE timeseries_1_0.timeseries_main (
    ts_key text,
    ts_validity daterange, 
    ts_data json,
    release text,
    access text, 
    primary key (ts_key, ts_validity),
    foreign key (release, ts_validity) references timeseries_1_0.releases
);




/* 
collections are user specific
favorite type of datasets.
collections are an approach to store
user selections within the timeseriesdb schema
*/
CREATE TABLE timeseries_1_0.collections(
    collection_name text,
    collection_owner text,
    collection_description text,
    primary key (collection_name, collection_owner)
)

-- collections to time series
CREATE TABLE timeseries_1_0.c_ts(
    collection_name text,
    ts_key text,
    primary key (collection_name, ts_key),
    foreign key (collection_name) references timeseries_1_0.collections,
    foreign key (ts_key) references timeseries_1_0.timeseries_main
)


/* 
established meta data approach 
extended /w validity
this costs disk space /w little in return
as is unlikely to change most of the time,
yet updating validity would screw FKs and
the opportunity to 
*/
CREATE TABLE timeseries_1_0.meta_data_unlocalized(
    ts_key text,
    ts_validity daterange,
    md_generated_by text,
    md_resource_last_update timestamptz,
    md_coverage_temp varchar,
    meta_data jsonb,
    primary key (ts_key, ts_validity),
    foreign key (ts_key) references timeseries_1_0.timeseries_main (ts_key) on delete cascade
)

CREATE TABLE timeseries_1_0.meta_data_localized(
    ts_key varchar,
    locale_info varchar, 
    md_validity daterange,
    meta_data jsonb,
    primary key (ts_key, locale_info, md_validity),
    foreign key (ts_key) references timeseries_1_0.timeseries_main (ts_key) 
    foreign key (md_validity) references timeseries_1_0.md_ts_validity (md_validity) 
)

/*
md: [1.1.,31.10), ts_validity [1.1.,2.1.)
md: [1.1.,31.10), ts_validity [1.1.,2.1.)
Maybe cleaning metadata function for when no vintages is available 
anymore is more practical.

*/
CREATE TABLE timeseries_1_0.md_ts_validity(
    ts_validity daterange,
    md_validity daterange
)







