defmodule Logflare.Repo.Migrations.UniqueOnSourcePublicKey do
  use Ecto.Migration

  def change do
    create(unique_index(:sources, [:public_token]))
  end
end
