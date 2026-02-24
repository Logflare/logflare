defmodule Logflare.Repo.Migrations.AddApiQuotasToSourcesUsers do
  use Ecto.Migration

  def up do
    alter table(:sources) do
      add :api_quota, :integer, default: 5, null: false
    end

    alter table(:users) do
      add :api_quota, :integer, default: 125, null: false
    end

    execute "UPDATE sources SET api_quota = 100"
    execute "UPDATE users SET api_quota = 125"
  end

  def down do
    alter table(:sources) do
      remove :api_quota
    end

    alter table(:users) do
      remove :api_quota
    end
  end
end
