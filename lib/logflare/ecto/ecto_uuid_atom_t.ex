defmodule Ecto.UUID.Atom do
  @moduledoc """
  UUID Atom type for Ecto
  """

  @behaviour Ecto.Type

  def type, do: :string

  def cast(value) when is_atom(value), do: {:ok, value}
  def cast(value) when is_binary(value), do: {:ok, String.to_atom(value)}
  def cast(_), do: :error

  def load(value) do
    with {:ok, value} <- Ecto.UUID.load(value) do
      {:ok, String.to_atom(value)}
    else
      err -> err
    end
  end

  def dump(value) when is_atom(value) do
    value
    |> Atom.to_string()
    |> Ecto.UUID.dump()
  end

  def dump(_), do: :error
end
