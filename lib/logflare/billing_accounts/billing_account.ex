defmodule Logflare.BillingAccounts.BillingAccount do
  use Ecto.Schema
  import Ecto.Changeset

  schema "billing_accounts" do
    field(:latest_successful_stripe_session, :map)
    field(:stripe_customer, :string)
    field(:stripe_subscriptions, :map)
    field(:stripe_invoices, :map)
    field(:custom_invoice_fields, {:array, :map}, default: [], nullable: false)
    field(:lifetime_plan?, :boolean, default: false, nullable: false)
    field(:lifetime_plan_invoice, :string)
    field(:default_payment_method, :string)
    belongs_to(:user, Logflare.User)

    timestamps()
  end

  @doc false
  def changeset(billing_account, attrs) do
    billing_account
    |> cast(attrs, [
      :latest_successful_stripe_session,
      :stripe_customer,
      :stripe_subscriptions,
      :stripe_invoices,
      :lifetime_plan?,
      :lifetime_plan_invoice,
      :default_payment_method,
      :custom_invoice_fields
    ])
    |> validate_required([:user_id, :stripe_customer])
    |> unique_constraint(:user_id)
  end
end
