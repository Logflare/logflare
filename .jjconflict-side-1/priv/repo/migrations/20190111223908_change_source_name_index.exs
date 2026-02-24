defmodule Logflare.Repo.Migrations.ChangeSourceNameIndex do
  use Ecto.Migration

  def change do
    drop index(:sources, [:name])
    create unique_index(:sources, [:name])
  end
end
