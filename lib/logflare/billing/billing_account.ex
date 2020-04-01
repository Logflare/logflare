defmodule Logflare.Billing.BillingAccount do
  use Ecto.Schema
  import Ecto.Changeset

  schema "billing_accounts" do
    field :latest_successful_stripe_session, :map
    field :stripe_customer, :string
    belongs_to :plan, Logflare.Plan
    belongs_to :user, Logflare.User

    timestamps()
  end

  @doc false
  def changeset(billing_account, attrs) do
    billing_account
    |> cast(attrs, [:latest_successful_stripe_session, :stripe_customer, :plan_id])
    |> validate_required([:user_id, :stripe_customer, :plan_id])
  end
end
