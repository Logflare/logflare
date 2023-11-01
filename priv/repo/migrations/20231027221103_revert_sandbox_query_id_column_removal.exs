defmodule Logflare.Repo.Migrations.RevertSandboxQueryIdColumnRemoval do
  use Ecto.Migration

  def up do
    alter table(:endpoint_queries) do
      add_if_not_exists :sandbox_query_id, references(:endpoint_queries, on_delete: :nothing)
    end
  end
  def down do

  end
end
