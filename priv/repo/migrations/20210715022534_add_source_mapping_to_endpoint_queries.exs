defmodule Logflare.Repo.Migrations.AddSourceMappingToEndpointQueries do
  use Ecto.Migration

  def change do
    alter table(:endpoint_queries) do
      add :source_mapping, :map, null: false, default: %{}
    end
  end
end
