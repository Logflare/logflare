ALTER SYSTEM SET wal_level = 'logical'; -- for wal
create schema if not exists analytics; -- for custom schema, cli integration