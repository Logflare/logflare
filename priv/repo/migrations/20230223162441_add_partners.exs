defmodule Logflare.Repo.Migrations.AddPartners do
  use Ecto.Migration

  def change do
    create table :partners do
      add :name, :binary
      add :token, :binary
      add :auth_token, :binary
    end

    unique_index(:partners, [:token])
  end
end
