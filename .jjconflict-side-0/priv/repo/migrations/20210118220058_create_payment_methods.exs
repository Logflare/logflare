defmodule Logflare.Repo.Migrations.CreatePaymentMethods do
  use Ecto.Migration

  def change do
    alter table(:billing_accounts) do
      add :default_payment_method, :string
    end

    create unique_index(:billing_accounts, :stripe_customer)

    create table(:payment_methods) do
      add :stripe_id, :string
      add :price_id, :string
      add :last_four, :string
      add :brand, :string
      add :exp_year, :integer
      add :exp_month, :integer
      add :customer_id, references(:billing_accounts, type: :string, column: :stripe_customer, on_delete: :delete_all)

      timestamps()
    end

    create index(:payment_methods, [:customer_id])

    create unique_index(:payment_methods, :stripe_id)
  end
end
