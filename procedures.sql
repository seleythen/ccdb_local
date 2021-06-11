create table ccdb_paths_update (id bigint, last_update timestamp);
insert into ccdb_paths_update values (1, now());
create or replace function ccdb_paths_updated () returns void language plpgsql as $$ begin update ccdb_paths_update set last_update = now() where id = 1; end $$;
