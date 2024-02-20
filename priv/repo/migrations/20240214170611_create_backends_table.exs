defmodule Logflare.Repo.Migrations.CreateBackendsTable do
  use Ecto.Migration

  def change do

    create table("backends") do
      add :user_id, references("users")
      add :type, :string
      add :config, :map
      timestamps()
    end

    create table("sources_backends") do
      add :backend_id, references("backends")
      add :source_id, references("sources")
    end

  end
end
