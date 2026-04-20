defmodule Repo.Migrations.AddVersions do
  use Ecto.Migration

  def change do
    create table(:versions) do
      add :event, :string, null: false, size: 10
      add :item_type, :string, null: false
      add :item_id, :integer
      add :item_changes, :map, null: false
      # you can change :users to your own foreign key constraint
      add :originator_id, references(:users)
      add :origin, :string, size: 255
      add :meta, :map

      # Configure timestamps type in config.ex :paper_trail :timestamps_type
      add :inserted_at, :utc_datetime, null: false
    end

    create index(:versions, [:originator_id])
    create index(:versions, [:item_id, :item_type])

    create unique_index(:versions, [:item_type, :item_id, "((meta->>'version_number')::integer)"],
             where: "item_type = 'Query' AND meta->>'version_number' IS NOT NULL",
             name: :versions_queries_item_id_version_number_index
           )

    # Uncomment if you want to add the following indexes to speed up special queries:
    # create index(:versions, [:event, :item_type])
    # create index(:versions, [:item_type, :inserted_at])
  end
end
