defmodule Logflare.Repo.Migrations.UserBillingEnabled do
  use Ecto.Migration

  def change do
    alter table(:users) do
      add :billing_enabled?, :boolean, null: false, default: false
    end
  end
end
