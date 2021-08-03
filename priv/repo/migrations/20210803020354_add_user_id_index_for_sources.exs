defmodule Logflare.Repo.Migrations.AddUserIdIndexForSources do
  use Ecto.Migration

  def change do
    create index(:sources, [:user_id])
  end
end
