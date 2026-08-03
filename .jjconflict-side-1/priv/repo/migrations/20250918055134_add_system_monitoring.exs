defmodule Logflare.Repo.Migrations.AddSystemMonitoring do
  use Ecto.Migration

  def change do
    alter table(:users) do
      add(:system_monitoring, :boolean, default: false, null: false)
    end

    alter table(:sources) do
      add(:system_source, :boolean, default: false)
      add(:system_source_type, :string)
    end

    create unique_index(:sources, [:user_id, :system_source_type])
  end
end
