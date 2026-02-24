defmodule Logflare.Repo.Migrations.AddLabelsToEndpointQueriesTable do
  use Ecto.Migration

  def change do
    alter table(:endpoint_queries) do
      add :labels, :text
    end
  end
end
