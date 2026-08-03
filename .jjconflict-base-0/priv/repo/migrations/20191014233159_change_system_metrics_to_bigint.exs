defmodule Logflare.Repo.Migrations.ChangeSystemMetricsToBigint do
  use Ecto.Migration

  def change do
    alter table(:system_metrics) do
      modify :all_logs_logged, :bigint
    end
  end
end
