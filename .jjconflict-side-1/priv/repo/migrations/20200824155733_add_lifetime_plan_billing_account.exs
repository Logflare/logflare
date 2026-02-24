defmodule Logflare.Repo.Migrations.AddLifetimePlanBillingAccount do
  use Ecto.Migration

  def change do
    alter table(:billing_accounts) do
      add :lifetime_plan?, :boolean, default: false, nullable: false
      add :lifetime_plan_invoice, :string
    end
  end
end
