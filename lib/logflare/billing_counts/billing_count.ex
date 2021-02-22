defmodule Logflare.BillingCounts.BillingCount do
  use TypedEctoSchema
  import Ecto.Changeset
  use Logflare.Changefeeds.ChangefeedSchema

  typed_schema "billing_counts" do
    field :count, :integer
    field :node, :string
    belongs_to :user, Logflare.User
    belongs_to :source, Logflare.Source

    # field :user_id, :id
    # field :source_id, :id

    timestamps()
  end

  @doc false
  def changeset(billing_counts, attrs) do
    billing_counts
    |> cast(attrs, [:node, :count])
    |> validate_required([:node, :count])
  end
end
