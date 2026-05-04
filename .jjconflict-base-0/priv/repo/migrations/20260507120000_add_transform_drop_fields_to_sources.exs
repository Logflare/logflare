defmodule Logflare.Repo.Migrations.AddTransformDropFieldsToSources do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      add :transform_drop_fields, :text
    end
  end
end
