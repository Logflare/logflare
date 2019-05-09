defmodule Logflare.Repo.Migrations.AddApiQuotasToSourcesUsers do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      add :api_quota, :integer, default: 0, null: false
    end

    alter table(:users) do
      add :api_quota, :integer, default: 0, null: false
    end
  end
end
