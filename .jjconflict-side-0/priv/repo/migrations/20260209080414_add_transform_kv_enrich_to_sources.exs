defmodule Logflare.Repo.Migrations.AddTransformKeyValuesToSources do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      add :transform_key_values, :string
    end
  end
end
