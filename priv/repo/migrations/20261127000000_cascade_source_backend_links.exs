defmodule Logflare.Repo.Migrations.CascadeSourceBackendLinks do
  use Ecto.Migration

  def up do
    alter table(:sources_backends) do
      modify :source_id, references(:sources, on_delete: :delete_all),
        from: references(:sources, on_delete: :nothing)
    end

    alter table(:source_backends) do
      modify :source_id, references(:sources, on_delete: :delete_all),
        from: references(:sources, on_delete: :nothing)
    end

    create_if_not_exists index(:backends, [:user_id])
    create_if_not_exists index(:sources_backends, [:backend_id])
    create_if_not_exists index(:sources_backends, [:source_id])
  end

  def down do
    drop_if_exists index(:sources_backends, [:source_id])
    drop_if_exists index(:sources_backends, [:backend_id])
    drop_if_exists index(:backends, [:user_id])

    alter table(:source_backends) do
      modify :source_id, references(:sources, on_delete: :nothing),
        from: references(:sources, on_delete: :delete_all)
    end

    alter table(:sources_backends) do
      modify :source_id, references(:sources, on_delete: :nothing),
        from: references(:sources, on_delete: :delete_all)
    end
  end
end
