defmodule Logflare.Repo.Migrations.RemoveSandboxQueryIdColumnFromEndpointQueriesTable do
  use Ecto.Migration

  def change do
    alter table(:endpoint_queries) do
      remove :sandbox_query_id, references(:endpoint_queries, on_delete: :nothing)
    end
  end
end
