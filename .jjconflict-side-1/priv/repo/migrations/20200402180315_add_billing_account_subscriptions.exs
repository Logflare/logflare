defmodule Logflare.Repo.Migrations.AddBillingAccountSubscriptions do
  use Ecto.Migration

  def change do
    alter table(:billing_accounts) do
      add :stripe_subscriptions, :map
    end
  end
end
