defmodule Logflare.Repo.Migrations.AddSystemMonitoring do
  use Ecto.Migration

  def change do
    alter table(:users) do
      add(:system_monitoring, :boolean, default: false, null: false)
    end
  end
end
