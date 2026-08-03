defmodule Logflare.Repo.Migrations.AddUniqueIndexUserEmail do
  use Ecto.Migration

  def change do
    create unique_index(:users, ["lower(email)"])
  end
end
