defmodule Logflare.Repo.Migrations.AlterSourcesTransformKeyValuesToText do
  use Ecto.Migration

  def up do
    alter table(:sources) do
      modify :transform_key_values, :text, from: :string
    end
  end

  def down do
    alter table(:sources) do
      modify :transform_key_values, :string, from: :text
    end
  end
end
