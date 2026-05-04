defmodule Logflare.Repo.Migrations.AddUsersBigqueryEnableManagedServiceACcounts do
  use Ecto.Migration

  def change do
    alter table(:users) do
      add :bigquery_enable_managed_service_accounts, :boolean, default: false
    end
  end
end
