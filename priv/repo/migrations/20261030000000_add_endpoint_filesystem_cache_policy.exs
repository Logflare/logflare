defmodule Logflare.Repo.Migrations.AddEndpointFilesystemCachePolicy do
  use Ecto.Migration

  def change do
    alter table(:endpoint_queries) do
      add :filesystem_cache_policy, :map
    end
  end
end
