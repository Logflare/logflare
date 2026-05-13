defmodule Logflare.Repo.Migrations.AddBigqueryAdditionalProjectsToUsers do
  use Ecto.Migration

  def change do
    alter table(:users) do
      add :bigquery_additional_projects, :string
    end
  end
end
