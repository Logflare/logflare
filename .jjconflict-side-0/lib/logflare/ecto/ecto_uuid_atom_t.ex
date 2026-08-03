defmodule Ecto.UUID.Atom do
  @moduledoc """
  UUID Atom type for Ecto
  """

  @behaviour Ecto.Type

  @type t :: atom()

  def type, do: :string

  def cast(value) when is_atom(value), do: {:ok, value}
  def cast(value) when is_binary(value), do: {:ok, String.to_atom(value)}
  def cast(_), do: :error

  def autogenerate, do: String.to_atom(Ecto.UUID.generate())

  def load(value) do
    with {:ok, value} <- Ecto.UUID.load(value) do
      {:ok, String.to_atom(value)}
    end
  end

  def dump(value) when is_atom(value) do
    value
    |> Atom.to_string()
    |> Ecto.UUID.dump()
  end

  def dump(_), do: :error

  def embed_as(_) do
    :self
  end

  def equal?(term1, term2) when is_atom(term1) and is_atom(term2) do
    term1 === term2
  end
end
