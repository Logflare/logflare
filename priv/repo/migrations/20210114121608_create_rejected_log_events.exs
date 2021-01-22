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

  end
end
