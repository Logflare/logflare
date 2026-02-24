defmodule Logflare.Repo.Migrations.IndexOnApiKey do
  use Ecto.Migration

  def change do
    create index(:users, [:api_key])
  end
end
