defmodule Logflare.Repo.Migrations.CreateRejectedLogEvents do
  use Ecto.Migration

  @table :rejected_log_events
  def change do
    create table(@table) do
      add :source_id, references(:sources)
      add :ingested_at, :utc_datetime_usec
      add :params, :map
      add :validation_error, :text
    end

    execute("""
    CREATE TRIGGER #{@table}_changefeed_trigger
        AFTER INSERT OR UPDATE OR DELETE
        ON #{@table}
            FOR EACH ROW
    EXECUTE PROCEDURE changefeed_notify();
    """)
  end
end
