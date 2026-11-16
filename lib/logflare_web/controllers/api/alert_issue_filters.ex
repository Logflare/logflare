defmodule LogflareWeb.Api.AlertIssueFilters do
  @moduledoc """
  Parses `filters[identifiers.<key>]=<value>` request params into an identifier map.
  """

  @identifier_prefix "identifiers."

  @spec parse(term()) :: {:ok, %{String.t() => String.t()}} | {:error, String.t()}
  def parse(filters) when is_map(filters) and map_size(filters) > 0 do
    Enum.reduce_while(filters, {:ok, %{}}, fn
      {@identifier_prefix <> key, value}, {:ok, acc} when is_binary(value) ->
        {:cont, {:ok, Map.put(acc, key, value)}}

      {key, _value}, _acc ->
        {:halt, {:error, "unsupported filter: #{key}"}}
    end)
  end

  def parse(_filters), do: {:error, "filters[identifiers.<key>] is required"}
end
