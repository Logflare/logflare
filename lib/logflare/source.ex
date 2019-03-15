defmodule Logflare.Source do
  use Ecto.Schema
  import Ecto.Changeset

  schema "sources" do
    field(:name, :string)
    field(:token, Ecto.UUID)
    field(:public_token, :string)
    belongs_to(:user, Logflare.User)
    has_many(:rules, Logflare.Rule)
    field(:overflow_source, Ecto.UUID)
    field(:avg_rate, :integer, virtual: true)

    timestamps()
  end

  @doc false
  def changeset(source, attrs) do
    source
    |> cast(attrs, [:name, :token, :public_token, :overflow_source, :avg_rate])
    |> validate_required([:name, :token])
    |> unique_constraint(:name)
    |> unique_constraint(:public_token)
    |> validate_min_avg_source_rate(:avg_rate)
  end

  def validate_min_avg_source_rate(changeset, field, options \\ []) do
    validate_change(changeset, field, fn _, avg_rate ->
      case avg_rate >= 1 do
        true ->
          []

        false ->
          [{field, options[:message] || "Average events per second must be at least 1"}]
      end
    end)
  end
end
