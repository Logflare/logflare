defmodule Logflare.KeyValues.KeyValue do
  @moduledoc false
  use TypedEctoSchema

  import Ecto.Changeset

  @derive {Jason.Encoder, only: [:id, :key, :value]}

  typed_schema "key_values" do
    field :key, :string
    field :value, :string

    belongs_to :user, Logflare.User
  end

  @spec changeset(%__MODULE__{}, map()) :: Ecto.Changeset.t()
  def changeset(key_value, attrs) do
    key_value
    |> cast(attrs, [:user_id, :key, :value])
    |> validate_required([:user_id, :key, :value])
    |> unique_constraint([:user_id, :key])
  end
end
