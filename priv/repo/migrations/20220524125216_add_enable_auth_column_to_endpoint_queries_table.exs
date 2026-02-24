defmodule Logflare.Repo.Migrations.AddEnableAuthColumnToEndpointQueriesTable do
  use Ecto.Migration

  def change do
    alter table("endpoint_queries") do
      add :enable_auth, :boolean, default: false
    end

  end
end
