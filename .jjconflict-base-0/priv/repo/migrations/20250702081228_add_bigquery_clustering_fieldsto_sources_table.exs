defmodule Logflare.Repo.Migrations.AddBigqueryClusteringFieldstoSourcesTable do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      add :bigquery_clustering_fields, :string
    end
  end
end
