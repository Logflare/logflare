defmodule Logflare.Repo.Migrations.CreateSourceSchemasChangfeedIdOnlyTrigger do
  use Ecto.Migration

  def change do
      execute("""
      CREATE TRIGGER #{table}_changefeed_id_only_trigger
          AFTER INSERT OR UPDATE OR DELETE
          ON #{table}
              FOR EACH ROW
      EXECUTE PROCEDURE changefeed_id_only_notify();
      """)
  end
end
