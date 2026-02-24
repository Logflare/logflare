defmodule Logflare.Billing.BillingCount do
  @moduledoc false
  use TypedEctoSchema

  import Ecto.Changeset

  typed_schema "billing_counts" do
    field :count, :integer
    field :node, :string

    belongs_to :user, Logflare.User
    belongs_to :source, Logflare.Sources.Source

    timestamps()
  end

  @doc false
  def changeset(billing_counts, attrs) do
    billing_counts
    |> cast(attrs, [:node, :count])
    |> validate_required([:node, :count])
  end
end
