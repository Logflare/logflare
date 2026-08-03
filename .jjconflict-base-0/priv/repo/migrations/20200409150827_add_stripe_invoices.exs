defmodule Logflare.Repo.Migrations.AddStripeInvoices do
  use Ecto.Migration

  def change do
    alter table(:billing_accounts) do
      add :stripe_invoices, :map
    end
  end
end
