defmodule Logflare.Repo.Migrations.CreateChangefeedTriggers do
  use Ecto.Migration

  def change do
    execute("""
    CREATE OR REPLACE FUNCTION changefeed_notify()
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
        IF (TG_OP = 'INSERT') THEN
            OLD := NEW;
        END IF;
        PERFORM pg_notify(
                TG_TABLE_NAME || '_changefeed',
                json_build_object(
                        'table', TG_TABLE_NAME,
                        'type', TG_OP,
                        'id', current_row.id,
                        'old', OLD,
                        'new', NEW
                    )::text
            );
        RETURN current_row;
    END;
    $$ LANGUAGE plpgsql;
    """)

    for table <- ~w(sources users teams team_users rules saved_searches) do
      execute("""
      CREATE TRIGGER #{table}_changefeed_trigger
          AFTER INSERT OR UPDATE OR DELETE
          ON #{table}
              FOR EACH ROW
      EXECUTE PROCEDURE changefeed_notify();
      """)
    end
  end
end
