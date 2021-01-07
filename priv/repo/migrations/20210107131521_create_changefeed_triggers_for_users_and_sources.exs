defmodule Logflare.Repo.Migrations.CreateChangefeedTriggersForUsersAndSources do
  use Ecto.Migration

  def change do
    execute("""
    begin;
    CREATE OR REPLACE FUNCTION users_changefeed()
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
                'users_changefeed',
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


    CREATE TRIGGER users_changefeed_trigger
        AFTER INSERT OR UPDATE OR DELETE
        ON users
        FOR EACH ROW
    EXECUTE PROCEDURE users_changefeed();

    commit;
    """)

    execute("""
    CREATE OR REPLACE FUNCTION sources_changefeed()
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
                'sources_changefeed',
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


    CREATE TRIGGER users_changefeed_trigger
        AFTER INSERT OR UPDATE OR DELETE
        ON users
        FOR EACH ROW
    EXECUTE PROCEDURE sources_changefeed();
    """)
  end
end
