defmodule Logflare.BillingAccounts.BillingAccount do
  use Ecto.Schema
  import Ecto.Changeset

  schema "billing_accounts" do
    field :latest_successful_stripe_session, :map
    field :stripe_customer, :string
    field :stripe_subscriptions, :map
    field :stripe_invoices, :map
    field :lifetime_plan, :boolean, default: false, nullable: false
    field :lifetime_plan_invoice, :string
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
      :stripe_invoices,
      :lifetime_plan,
      :lifetime_plan_invoice
    ])
    |> validate_required([:user_id, :stripe_customer])
    |> unique_constraint(:user_id)
  end

  def changefeed_changeset(attrs) do
    EctoChangesetExtras.cast_all_fields(struct(__MODULE__), attrs)
  end
end
