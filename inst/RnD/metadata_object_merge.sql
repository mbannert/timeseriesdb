drop table if exists a;
drop table if exists update;

create table a(
  id integer primary key,
  data jsonb not null default '{}'::jsonb
);

insert into a(id) values(1);

create table update(
  id integer,
  data jsonb
);

insert into update values(1, '{"a": 1}'::jsonb), (2, '{"mykey": "mybalue"}'::jsonb);

insert into a(id, data) as ahaha
select id, data
FROM update
on conflict(id) do update
set
  data = ahaha.data || EXCLUDED.data;

select * from a;
