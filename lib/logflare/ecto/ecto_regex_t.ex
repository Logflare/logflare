defmodule Ecto.Regex do
  @moduledoc """
  Regex type for Ecto
  """

  @behaviour Ecto.Type

  @spec type :: :binary
  def type, do: :binary

  def cast(value) when is_binary(value) do
    case Regex.compile(value) do
      {:ok, r} -> {:ok, r}
      {:error, _} -> :error
    end
  end

  def cast(%Regex{} = value), do: value
  def cast(_), do: :error

  @spec load(any) :: :error | {:ok, any}
  def load(value) do
    try do
      {:ok, :erlang.binary_to_term(value)}
    rescue
      e in ArgumentError -> {:error, e}
    end
  end

  def dump(%Regex{} = value) do
    {:ok, :erlang.term_to_binary(value)}
  end

  def dump(_), do: :error

  def embed_as(_) do
    :self
  end

  def equal?(term1, term2) do
    term1 === term2
  end
end
