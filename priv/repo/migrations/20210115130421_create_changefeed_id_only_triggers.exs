defmodule Logflare.Repo.Migrations.CreateChangefeedIdOnlyTriggers do
  use Ecto.Migration

  def change do
    execute("""
    CREATE OR REPLACE FUNCTION changefeed_id_only_notify()
    RETURNS trigger AS
    $$
    DECLARE
        current_row RECORD;
    BEGIN
        IF (TG_OP = 'INSERT' OR TG_OP = 'UPDATE') THEN
            current_row := NEW;
        ELSE
            current_row := OLD;
        END IF;
        PERFORM pg_notify(
                TG_TABLE_NAME || '_id_only_changefeed',
                json_build_object(
                        'table', TG_TABLE_NAME,
                        'type', TG_OP,
                        'id', current_row.id,
                    )::text
            );
        RETURN current_row;
    END;
    $$ LANGUAGE plpgsql;
    """)

  end
end
