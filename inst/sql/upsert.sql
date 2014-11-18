# Hat tip to Charles Clavadetscher for helping out
# moving this update / insert operations to the 
# database level

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
          ts_frequency = new.ts_frequency,
          md_generated_on = new.md_generated_on,
          md_generated_by = new.md_generated_by
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
          set meta_data = new.meta_data
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
