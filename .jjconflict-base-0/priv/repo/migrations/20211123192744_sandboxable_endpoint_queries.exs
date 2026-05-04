defmodule Logflare.Repo.Migrations.SandboxableEndpointQueries do
  use Ecto.Migration

  def change do
    alter table(:endpoint_queries) do
      add :sandboxable, :boolean, default: false
    end
  end
end
