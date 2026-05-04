defmodule Logflare.Repo.Migrations.AddMetadataColumnToBackendsTable do
  use Ecto.Migration

  def change do

    alter table(:backends) do
      add :metadata, :map
    end
  end
end
