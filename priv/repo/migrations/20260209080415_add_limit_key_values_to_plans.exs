defmodule Logflare.Repo.Migrations.AddLimitKeyValuesToPlans do
  use Ecto.Migration

  def change do
    alter table(:plans) do
      add :limit_key_values, :integer, default: 0
    end
  end
end
