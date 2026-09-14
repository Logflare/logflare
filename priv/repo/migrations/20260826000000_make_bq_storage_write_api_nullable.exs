defmodule Logflare.Repo.Migrations.MakeBqStorageWriteApiNullable do
  use Ecto.Migration

  def up do
    alter table("sources") do
      modify :bq_storage_write_api, :boolean, default: nil
    end

    execute "UPDATE sources SET bq_storage_write_api = NULL WHERE bq_storage_write_api = false"
  end

  def down do
    execute "UPDATE sources SET bq_storage_write_api = false WHERE bq_storage_write_api IS NULL"

    alter table("sources") do
      modify :bq_storage_write_api, :boolean, default: false
    end
  end
end
