defmodule Logflare.Repo.Migrations.AddBigqueryFlatmap do
  use Ecto.Migration

  def change do
    alter table(:source_schemas) do
      add(:schema_flat_map, :binary)
    end
  end
end
