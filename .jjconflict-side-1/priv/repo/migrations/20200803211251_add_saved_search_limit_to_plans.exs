defmodule Logflare.Repo.Migrations.AddSavedSearchLimitToPlans do
  use Ecto.Migration

  def change do
    alter table(:plans) do
      add :limit_saved_search_limit, :integer
      add :limit_team_users_limit, :integer
      add :limit_source_fields_limit, :integer
    end
  end
end
