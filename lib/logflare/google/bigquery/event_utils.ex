defmodule Logflare.Google.BigQuery.EventUtils do
  @moduledoc """
  Event utils for BigQuery.
  """

  @doc """
  Prepares an event for ingest into BigQuery
  """
  @spec prepare_for_ingest(event :: map()) :: [map()]
  def prepare_for_ingest(event) do
    [wrap_fields(event)]
  end

  defp wrap_fields(value) when is_map(value) do
    value
    |> Enum.map(&wrap_fields/1)
    |> Enum.into(%{})
  end

  defp wrap_fields({k, v}) when is_map(v) do
    wrapped =
      v
      |> Enum.map(&wrap_fields/1)
      |> Enum.into(%{})
      |> List.wrap()

    {k, wrapped}
  end

  defp wrap_fields({k, v}) when is_list(v) and is_map(hd(v)) do
    {k, Enum.map(v, &wrap_fields/1)}
  end

  defp wrap_fields({k, v}), do: {k, v}
end
