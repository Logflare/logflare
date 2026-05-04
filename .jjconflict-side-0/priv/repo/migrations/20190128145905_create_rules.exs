defmodule Logflare.Repo.Migrations.CreateRules do
  use Ecto.Migration

  def change do
    create table(:rules) do
      add :regex, :string
      add :sink, :uuid
      add :source_id, references(:sources, on_delete: :delete_all)

      timestamps()
    end

    create index(:rules, [:source_id])
  end
end
