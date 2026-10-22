defmodule Logflare.Repo.Migrations.AddEnforcedClickhouseSettingsToEndpointQueries do
  use Ecto.Migration

  def change do
    alter table(:endpoint_queries) do
      add(:enforced_clickhouse_settings, :map, default: %{}, null: false)
    end
  end
end
