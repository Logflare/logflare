defmodule Logflare.Repo.Migrations.AddNodeNameIndexToSystemMetrics do
  use Ecto.Migration

  def change do
    create index(:system_metrics, [:node])
  end
end
