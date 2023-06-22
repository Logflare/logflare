defmodule Logflare.Utils do
  @doc """
  checks if a string is a uuid

    iex> is_uuid?("some")
    false
    iex> is_uuid?("2f0a0f10-d560-4cdd-8b78-b5aa74aab133")
    true
  """
  def is_uuid?(value) when is_binary(value) do
    case Ecto.UUID.cast(value) do
      {:ok, _} -> true
      _ -> false
    end
  end
end
