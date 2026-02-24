defmodule Logflare.Repo.Migrations.AlterUsersAddBigqueryUdfsHash do
  use Ecto.Migration

  def change do
    alter table(:users) do
      add :bigquery_udfs_hash, :string, default: "", null: false
    end
  end
end
