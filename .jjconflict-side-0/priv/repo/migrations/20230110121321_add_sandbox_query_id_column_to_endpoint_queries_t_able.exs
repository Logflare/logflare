defmodule Logflare.Repo.Migrations.AddSandboxQueryIdColumnToEndpointQueriesTAble do
  use Ecto.Migration

  def change do

    alter table(:endpoint_queries) do
      add :sandbox_query_id, references(:endpoint_queries, on_delete: :nothing)
    end
  end
end
