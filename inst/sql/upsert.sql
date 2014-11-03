# Hat tip to Charles Clavadetscher for helping out
# moving this update / insert operations to the 
# database level

create or replace function upsert_timeseries()
returns trigger
as $$
begin
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
end;
$$ language plpgsql;

create trigger upsert_timeseries
  before insert on timeseries_main
  for each row execute procedure upsert_timeseries();

