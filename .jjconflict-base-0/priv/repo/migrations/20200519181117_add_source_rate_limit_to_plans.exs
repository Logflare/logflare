defmodule Logflare.Repo.Migrations.AddSourceRateLimitToPlans do
  use Ecto.Migration

  def change do
    alter table(:plans) do
      add :limit_source_rate_limit, :integer
    end
  end
end
