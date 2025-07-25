defmodule Logflare.Repo.Migrations.AlterUsersRemoveBigqueryUdfsHash do
  use Ecto.Migration

  def change do
    alter table(:users) do
      remove :bigquery_udfs_hash, :string, default: "", null: false
    end
  end
end
