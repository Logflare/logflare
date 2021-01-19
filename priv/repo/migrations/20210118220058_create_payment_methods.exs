defmodule Logflare.Repo.Migrations.CreatePaymentMethods do
  use Ecto.Migration

  def change do
    create unique_index(:billing_accounts, :stripe_customer)

    create table(:payment_methods) do
      add :stripe_id, :string
      add :price_id, :string
      add :customer_id, references(:billing_accounts, type: :string, column: :stripe_customer, on_delete: :delete_all)

      timestamps()
    end

    create index(:payment_methods, [:customer_id])
  end
end
