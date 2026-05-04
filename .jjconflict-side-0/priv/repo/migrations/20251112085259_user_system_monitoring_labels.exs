defmodule Logflare.Repo.Migrations.UserSystemMonitoringLabels do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      add :labels, :text
    end
  end
end
