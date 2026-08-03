defmodule Logflare.Repo.Migrations.AddLimitsToPlans do
  use Ecto.Migration

  def change do
    alter table(:plans) do
      add :limit_sources, :integer
      add :limit_rate_limit, :integer
      add :limit_alert_freq, :integer
    end
  end
end
