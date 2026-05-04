defmodule Logflare.Repo.Migrations.CreateBillingAccounts do
  use Ecto.Migration

  def change do
    create table(:billing_accounts) do
      add :latest_successful_stripe_session, :map
      add :stripe_customer, :string
      add :user_id, references(:users, on_delete: :delete_all)

      timestamps()
    end
  end
end
