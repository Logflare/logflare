defmodule Ecto.Atom do
  @moduledoc """
  Ecto type for storing atoms as strings in the database.
  """

  @behaviour Ecto.Type

  @type t :: atom()

  def type, do: :string

  def cast(value) when is_atom(value), do: {:ok, value}
  def cast(_), do: :error

  def load(value) do
    {:ok, String.to_existing_atom(value)}
  rescue
    e in ArgumentError -> {:error, e}
  end

  def dump(value) when is_atom(value), do: {:ok, Atom.to_string(value)}
  def dump(_), do: :error

  def embed_as(_) do
    :self
  end

  def equal?(term1, term2) do
    term1 === term2
  end
end
