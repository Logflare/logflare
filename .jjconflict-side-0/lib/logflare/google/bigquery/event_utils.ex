defmodule Logflare.Google.BigQuery.EventUtils do
  @moduledoc """
  Event utils for BigQuery.
  """

  @doc """
  Converts LogEvent's body into a valid dataframe struct for Explorer
  """
  def log_event_to_df_struct(%Logflare.LogEvent{body: body}) do
    {:ok, bq_timestamp} = DateTime.from_unix(body["timestamp"], :microsecond)

    for {k, v} <- body, into: %{} do
      if is_map(v) do
        {k, prepare_for_ingest(v)}
      else
        {k, v}
      end
    end
    |> Map.put("timestamp", bq_timestamp)
    |> Map.put("event_message", body["event_message"])
  end

  @doc """
  Checks for all maps fields from the dataframe list, then adds the missing fields to the
  ones that don't have a field set with the default value `nil`
  """
  def normalize_df_struct_fields(dataframes) do
    keys =
      dataframes
      |> Enum.reduce(MapSet.new(), fn x, acc ->
        keys = Map.keys(x) |> MapSet.new()
        MapSet.union(acc, keys)
      end)
      |> MapSet.to_list()

    normalized_struct = Map.from_keys(keys, nil)

    Enum.map(dataframes, fn x ->
      Map.merge(normalized_struct, x)
    end)
  end

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
