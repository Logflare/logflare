defmodule Logflare.Repo.Migrations.AddSystemMetricsTable do
  use Ecto.Migration

  def change do
    create table("system_metrics") do
      add :all_logs_logged, :integer
      add :node, :string

      timestamps()
    end
  end
end
