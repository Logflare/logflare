defmodule Logflare.Repo.Migrations.AddTransformCopyFieldsColumnToSourcesTable do
  use Ecto.Migration

  def change do

    alter table(:sources) do
      add :transform_copy_fields, :string
    end
  end
end
