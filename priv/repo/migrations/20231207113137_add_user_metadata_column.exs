defmodule Logflare.Repo.Migrations.AddUserMetadataColumn do
  use Ecto.Migration

  def up do

    alter table(:users) do
      add :metadata, :map
      modify(:email, :string, null: true)
    end
  end

  def down do

    alter table(:users) do
      remove :metadata
      modify(:email, :string, null: false)
    end
  end
end
