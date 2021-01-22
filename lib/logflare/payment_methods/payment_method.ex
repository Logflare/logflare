defmodule Logflare.PaymentMethods.PaymentMethod do
  use Ecto.Schema
  import Ecto.Changeset

  schema "payment_methods" do
    field :price_id, :string
    field :stripe_id, :string
    field :customer_id, :string
    field :last_four, :string
    field :brand, :string
    field :exp_month, :integer
    field :exp_year, :integer

    timestamps()
  end

  @doc false
  def changeset(payment_method, attrs) do
    payment_method
    |> cast(attrs, [
      :stripe_id,
      :price_id,
      :customer_id,
      :last_four,
      :brand,
      :exp_month,
      :exp_year
    ])
    |> validate_required([:stripe_id, :customer_id])
  end
end
