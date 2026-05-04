defmodule Logflare.Repo.Migrations.CreateSourceBackendsTable do
  use Ecto.Migration

  def change do
    create table("source_backends") do
      add :source_id, references("sources")
      add :type, :string
      add :config, :map
      timestamps()
    end
  end
end
