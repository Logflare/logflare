defmodule Logflare.Repo.Migrations.AddBqTableSettings do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      add(:bigquery_table_ttl, :integer)
    end

    alter table(:users) do
      add(:bigquery_project_id, :string)
    end
  end
end
