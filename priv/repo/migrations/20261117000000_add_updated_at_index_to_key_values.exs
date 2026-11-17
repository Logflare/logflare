defmodule Logflare.Repo.Migrations.AddUpdatedAtIndexToKeyValues do
  use Ecto.Migration

  def change do
    create index(:key_values, [:updated_at])
  end
end
