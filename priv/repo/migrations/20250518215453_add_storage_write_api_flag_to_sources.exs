defmodule Logflare.Repo.Migrations.AddStorageWriteApiFlagToSources do
  use Ecto.Migration

  def change do
    alter table "sources" do
      add :bq_storage_write_api, :boolean, default: false
    end
  end
end
