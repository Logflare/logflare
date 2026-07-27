defmodule Logflare.Google.BigQuery.EventUtils do
  @moduledoc """
  Event utils for BigQuery.
  """

  alias Logflare.LogEvent.TypeDetection

  @doc """
  Converts LogEvent's body into a valid dataframe struct for Explorer
  """
  def log_event_to_df_struct(%Logflare.LogEvent{body: body, otel_timestamps: otel_timestamps?}) do
    for {k, v} <- body, into: %{} do
      if is_map(v) do
        {k, prepare_for_ingest(v)}
      else
        {k, v}
      end
    end
    |> Map.put("event_message", body["event_message"])
    |> convert_to_seconds(otel_timestamps?)
  end

  @doc """
  Converts nanosecond/microsecond timestamps to seconds.
  https://docs.cloud.google.com/bigquery/docs/streaming-data-into-bigquery#send_date_and_time_data

  `timestamp` is always converted. `start_time`/`end_time` are only converted
  when the event carries OTel timestamps, mirroring the `TIMESTAMP` column
  typing in `SchemaBuilder`. The flag is detected at ingest and stored on
  `Logflare.LogEvent` as `otel_timestamps` — pass it via `convert_to_seconds/2`;
  `convert_to_seconds/1` falls back to detecting it from `data`.
  """
  @spec convert_to_seconds(map()) :: map()
  def convert_to_seconds(data) do
    convert_to_seconds(data, TypeDetection.otel_timestamps?(data))
  end

  @spec convert_to_seconds(map(), boolean()) :: map()
  def convert_to_seconds(data, otel_timestamps?) do
    data = timestamp_to_seconds(data)

    if otel_timestamps? do
      data
      |> ns_to_seconds("start_time")
      |> ns_to_seconds("end_time")
    else
      data
    end
  end

  defp ns_to_seconds(data, field) do
    case data do
      %{^field => ts} when is_integer(ts) and ts > 1_000_000_000_000_000_000 ->
        %{data | field => ts / :math.pow(1000, 3)}

      _ ->
        data
    end
  end

  defp timestamp_to_seconds(%{"timestamp" => ts} = data)
       when is_integer(ts) and ts > 1_000_000_000_000_000_000,
       do: %{data | "timestamp" => ts / :math.pow(1000, 3)}

  defp timestamp_to_seconds(%{"timestamp" => ts} = data)
       when is_integer(ts) and ts > 1_000_000_000_000,
       do: %{data | "timestamp" => ts / :math.pow(1000, 2)}

  defp timestamp_to_seconds(data), do: data

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
