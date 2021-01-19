defmodule :"Elixir.Logflare.Repo.Migrations.Add-default-payment-method" do
  use Ecto.Migration

  def change do
    create unique_index(:payment_methods, :stripe_id)

    alter table(:billing_accounts) do
      add :default_payment_method, :string
    end
  end
end
