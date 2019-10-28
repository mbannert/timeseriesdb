-- remove this line before going into production! ;)
DROP SCHEMA timeseries CASCADE;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE SCHEMA timeseries;

-- links public validity on dataset level to series
CREATE TABLE timeseries.releases(
    id UUID NOT NULL DEFAULT uuid_generate_v1() PRIMARY KEY, -- guess a sequence would be fine too
                                                             -- or even release as FK to allow sharing
    release text,
    release_description text
);

-- store different versions of time series
CREATE TABLE timeseries.timeseries_main (
    ts_key text,
    ts_data json,
    release_id UUID,
    ts_validity daterange,
    release_validity tstzrange,
    access text,
    usage_type integer, -- ???
    primary key (ts_key, ts_validity, release_validity),
    foreign key (release_id) references timeseries.releases(id),
    EXCLUDE USING GIST (ts_key WITH =, ts_validity WITH &&) WHERE (usage_type = 1 OR usage_type = 2), -- this only applies to 1 and 2
    EXCLUDE USING GIST (ts_key WITH =, release_validity WITH &&) WHERE (usage_type = 2 OR usage_type = 4) -- this only applies to 2 and 4
    -- 3 doesn't care, 1 will collide in second, 4 in first excludes
);

ALTER TABLE timeseries.timeseries_main ENABLE ROW LEVEL SECURITY;
CREATE POLICY timeseries_access ON timeseries.timeseries_main USING (pg_has_role(access, 'usage'));


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







