defmodule Logflare.Repo.Migrations.MakeBillingAccountsUnique do
  use Ecto.Migration

  def change do
    create unique_index(:billing_accounts, [:user_id])
  end
end
