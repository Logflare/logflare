defmodule Logflare.Repo.Migrations.UniqueNameAndTokenSource do
  use Ecto.Migration

  def change do
    create unique_index(:sources, [:name])
  end
end
