defmodule Logflare.Billing.BillingAccount do
  use Ecto.Schema
  import Ecto.Changeset

  schema "billing_accounts" do
    field :latest_successful_stripe_session, :map
    field :stripe_customer, :string
    field :stripe_subscriptions, :map
    field :stripe_invoices, :map
    belongs_to :user, Logflare.User

    timestamps()
  end

  @doc false
  def changeset(billing_account, attrs) do
    billing_account
    |> cast(attrs, [
      :latest_successful_stripe_session,
      :stripe_customer,
      :stripe_subscriptions,
      :stripe_invoices
    ])
    |> validate_required([:user_id, :stripe_customer])
    |> unique_constraint(:user_id)
  end
end
