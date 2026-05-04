defmodule Logflare.Repo.Migrations.FavoriteSource do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      add(:favorite, :boolean, default: false, null: false)
    end
  end
end
