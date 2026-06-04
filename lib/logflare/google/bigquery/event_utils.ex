defmodule Logflare.Google.BigQuery.EventUtils do
  @moduledoc """
  Event utils for BigQuery.
  """

  @doc """
  Converts LogEvent's body into a valid dataframe struct for Explorer
  """
  def log_event_to_df_struct(%Logflare.LogEvent{body: body}) do
    for {k, v} <- body, into: %{} do
      if is_map(v) do
        {k, prepare_for_ingest(v)}
      else
        {k, v}
      end
    end
    |> Map.put("event_message", body["event_message"])
    |> convert_to_seconds()
  end

  @doc """
  Recursively converts nanosecond/microsecond timestamps to seconds
  https://docs.cloud.google.com/bigquery/docs/streaming-data-into-bigquery#send_date_and_time_data
  """
  @spec convert_to_seconds(map()) :: map()
  def convert_to_seconds(%{"start_time" => ts} = data) when ts > 1_000_000_000_000_000_000 do
    %{data | "start_time" => ts / :math.pow(1000, 3)}
    |> convert_to_seconds()
  end

  def convert_to_seconds(%{"end_time" => ts} = data) when ts > 1_000_000_000_000_000_000 do
    %{data | "end_time" => ts / :math.pow(1000, 3)}
    |> convert_to_seconds()
  end

  def convert_to_seconds(%{"timestamp" => ts} = data) when ts > 1_000_000_000_000_000_000 do
    %{data | "timestamp" => ts / :math.pow(1000, 3)}
    |> convert_to_seconds()
  end

  def convert_to_seconds(%{"timestamp" => ts} = data) when ts > 1_000_000_000_000 do
    %{data | "timestamp" => ts / :math.pow(1000, 2)}
    |> convert_to_seconds()
  end

  def convert_to_seconds(data), do: data

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

  defp wrap_fields(value) when is_list(value) do
    Enum.map(value, &wrap_fields/1)
  end

  defp wrap_fields(value), do: value
end
