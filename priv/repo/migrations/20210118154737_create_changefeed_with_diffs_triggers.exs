defmodule Logflare.Repo.Migrations.CreateChangefeedWithDiffsTriggers do
  use Ecto.Migration

  def change do
    execute("""
      CREATE OR REPLACE FUNCTION jsonb_object_changes(old jsonb, new jsonb)
        RETURNS jsonb
        LANGUAGE sql
        IMMUTABLE
        STRICT
      AS
      $$
      SELECT json_object_agg(key, new -> key)
      FROM jsonb_object_keys(jsonb_concat(old, new)) AS key
      WHERE old -> key <> new -> key
        OR new -> key IS NULL
        OR old -> key IS NULL
      $$;
    """)

    execute("""
      CREATE OR REPLACE FUNCTION changefeed_notify()
        RETURNS trigger AS
      $$
      DECLARE
        current_row RECORD;
        changes     jsonb;
      BEGIN
        CASE
          WHEN tg_op = 'INSERT' THEN
            current_row := new;
            changes := to_jsonb(current_row);
          WHEN tg_op = 'UPDATE' THEN
            current_row := new;
            changes := jsonb_object_changes(to_jsonb(old), to_jsonb(new));
          WHEN tg_op = 'DELETE' THEN
            current_row := old;
            changes = 'null'::jsonb;
          ELSE
            RAISE NOTICE 'TG_OP should never be anything then INSERT, UPDATE or DELETE';
          END CASE;
        PERFORM pg_notify(
            tg_table_name || '_changefeed',
            json_build_object(
                'table', tg_table_name,
                'type', tg_op,
                'id', current_row.id,
                'changes', changes
              )::text
          );
        RETURN current_row;
      END;
      $$ LANGUAGE plpgsql;
    """)
  end
end
