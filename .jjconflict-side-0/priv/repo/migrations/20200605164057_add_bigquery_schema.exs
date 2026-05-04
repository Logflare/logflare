defmodule Logflare.Repo.Migrations.AddBigquerySchema do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      add :bigquery_schema, :binary
      remove_if_exists(:schema, :binary)
    end
  end
end
