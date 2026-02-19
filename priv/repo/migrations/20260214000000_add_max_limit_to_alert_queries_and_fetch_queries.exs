defmodule Logflare.Repo.Migrations.AddMaxLimitToAlertQueriesAndFetchQueries do
  use Ecto.Migration

  def change do
    alter table(:alert_queries) do
      add :max_limit, :integer, default: 1_000
    end
  end
end
