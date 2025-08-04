defmodule Ecto.Term do
  @moduledoc """
  Generic Erlang term type for storing arbitrary Elixir values as binary in the database.
  """

  @behaviour Ecto.Type

  @spec type :: :binary
  def type, do: :binary

  def cast(value) do
    {:ok, value}
  end

  @spec load(binary() | nil) :: {:ok, any()} | {:error, ArgumentError.t()}
  def load(nil), do: {:ok, nil}
  def load(""), do: {:ok, ""}

  def load(value) do
    {:ok, :erlang.binary_to_term(value)}
  rescue
    e in ArgumentError -> {:error, e}
  end

  @spec dump(any()) :: {:ok, binary() | nil}
  def dump(nil), do: {:ok, nil}
  def dump(""), do: {:ok, ""}

  def dump(value) do
    {:ok, :erlang.term_to_binary(value)}
  end

  def embed_as(_), do: :self

  def equal?(term1, term2) do
    term1 === term2
  end
end
