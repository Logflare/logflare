defmodule Logtail.Repo.Migrations.CreateSources do
  use Ecto.Migration

  def change do
    create table(:sources) do
      add :name, :string
      add :token, :uuid

      timestamps()
    end

  end
end
