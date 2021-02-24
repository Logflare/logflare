defmodule Logflare.Repo.Migrations.CreateLogEvents do
  use Ecto.Migration

  def change do
    create table(:log_events, primary_key: false) do
      add :id, :binary, primary_key: true
      add :body, :map
      add :valid, :boolean
      add :is_from_stale_query, :boolean
      add :ingested_at, :utc_datetime_usec
      add :sys_uint, :integer
      add :params, :map
      add :origin_source_id, :text
      add :via_rule, :map

      add :source_id, references(:sources, on_delete: :delete_all)
    end
  end
end
