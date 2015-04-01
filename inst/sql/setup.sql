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


/*
Create a function to resolve time 
*/
CREATE FUNCTION ts_data(tkey varchar) RETURNS TABLE(date date, value varchar)
AS $$
  SELECT ((each(ts_data)).key)::date,
         ((each(ts_data)).value)::varchar
         FROM timeseries_main
         WHERE ts_key = tkey;
$$ LANGUAGE sql;


/* Hat tip to Charles Clavadetscher for helping out
moving these update / insert operations to the 
database level 

KNOWN ISSUE: EXACTLY CONCURRENT INSERT CAN CAUSE 
TROUBLE WITH THIS APPROACH. THIS CAN BE CURED BY
LOCKING TABLES AND WILL BE ADDRESSED IN THE NEXT RELEASE.
*/

create or replace function upsert_timeseries_tables()
returns trigger
as $$
begin
  case TG_TABLE_NAME
    when 'timeseries_main' then
      if TG_OP = 'INSERT' then
        if not exists (select 1 from timeseries_main
        where ts_key = new.ts_key) then
          return new;
        else
          update timeseries_main
          set ts_data = new.ts_data,
          ts_frequency = new.ts_frequency
          where ts_key = new.ts_key;
          return null;
        end if;
      end if;
    when 'meta_data_localized' then
      if TG_OP = 'INSERT' then
        if not exists (select 1 from meta_data_localized
        where ts_key = new.ts_key
        and locale_info = new.locale_info) then
          return new;
        else
          update meta_data_localized
          set meta_data = new.meta_data
          where ts_key = new.ts_key
          and locale_info = new.locale_info;
          return null;
        end if;
      end if;
    when 'meta_data_unlocalized' then
      if TG_OP = 'INSERT' then
        if not exists (select 1 from meta_data_unlocalized
        where ts_key = new.ts_key) then
          return new;
        else
          update meta_data_unlocalized
          set md_generated_by = new.md_generated_by,
          md_resource_last_update = new.md_resource_last_update,
          md_coverage_temp = new.md_coverage_temp
          where ts_key = new.ts_key;
          return null;
        end if;
      end if;
  end case;
end;
$$ language plpgsql;

create trigger upsert_timeseries_tables
before insert on timeseries_main
for each row execute procedure upsert_timeseries_tables();

create trigger upsert_timeseries_tables
before insert on meta_data_localized
for each row execute procedure upsert_timeseries_tables();

create trigger upsert_timeseries_tables
before insert on meta_data_unlocalized
for each row execute procedure upsert_timeseries_tables();













