defmodule Ecto.Term do
  @moduledoc """
  Generitc Erlang term type
  """

  @behaviour Ecto.Type

  @spec type :: :binary
  def type, do: :binary

  def cast(value) do
    {:ok, value}
  end

  @spec load(any) :: :error | {:ok, any}
  def load(value) do
    try do
      {:ok, :erlang.binary_to_term(value)}
    rescue
      e in ArgumentError -> {:error, e}
    end
  end

  def dump(value) do
    {:ok, :erlang.term_to_binary(value)}
  end

  def embed_as(_) do
    :self
  end

  def equal?(term1, term2) do
    term1 === term2
  end
end
