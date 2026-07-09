defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.Ingester do
  @moduledoc """
  Simplified ingestion-only functionality for ClickHouse.
  """

  import Logflare.Utils.Guards

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.QueryTemplates
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.RowBinaryEncoder
  alias Logflare.Backends.Backend
  alias Logflare.LogEvent
  alias Logflare.LogEvent.TypeDetection

  @finch_pool Logflare.FinchClickHouseIngest
  @max_retries 1
  @initial_delay 2_500
  @max_delay 5_000
  @pool_timeout 8_000
  @receive_timeout 20_000

  @doc """
  Inserts a list of `LogEvent` structs into ClickHouse.

  This function expects that all LogEvents share the same `event_type`.

  Not intended for direct use. Use `Logflare.Backends.Adaptor.ClickHouseAdaptor.insert_log_events/4` instead.
  """
  @spec insert(
          Backend.t() | Keyword.t(),
          table :: String.t(),
          log_events :: [LogEvent.t()],
          TypeDetection.event_type(),
          opts :: keyword()
        ) ::
          :ok | {:error, String.t()}
  def insert(backend_or_conn_opts, table, log_events, event_type, opts \\ [])

  def insert(_backend_or_conn_opts, _table, [], _event_type, _opts), do: :ok

  def insert(%Backend{} = backend, table, log_events, event_type, opts)
      when is_list(log_events) and is_event_type(event_type) do
    with {:ok, connection_opts} <- build_connection_opts(backend) do
      insert(connection_opts, table, log_events, event_type, opts)
    end
  end

  def insert(
        connection_opts,
        table,
        [%LogEvent{event_type: event_type} | _] = log_events,
        event_type,
        opts
      )
      when is_list(connection_opts) and is_non_empty_binary(table) and is_event_type(event_type) and
             is_list(opts) do
    request_body = log_events |> encode_batch(event_type) |> :zlib.gzip()
    do_insert(connection_opts, table, event_type, request_body, opts)
  end

  @spec do_insert(Keyword.t(), String.t(), TypeDetection.event_type(), iodata(), keyword()) ::
          :ok | {:error, String.t()}
  defp do_insert(connection_opts, table, event_type, request_body, opts) do
    client = build_client(connection_opts)
    url = build_request_url(connection_opts, table, event_type, opts)

    case Tesla.post(client, url, request_body) do
      {:ok, %Tesla.Env{status: 200}} ->
        :ok

      {:ok, %Tesla.Env{status: status, body: response_body}} ->
        {:error, "HTTP #{status}: #{response_body}"}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec build_client(Keyword.t()) :: Tesla.Client.t()
  defp build_client(connection_opts) do
    middleware = [
      {Tesla.Middleware.Headers,
       [{"content-type", "application/octet-stream"}, {"content-encoding", "gzip"}]},
      {Tesla.Middleware.BasicAuth,
       %{
         username: Keyword.get(connection_opts, :username),
         password: Keyword.get(connection_opts, :password)
       }},
      {Tesla.Middleware.Retry,
       delay: @initial_delay,
       max_retries: @max_retries,
       max_delay: @max_delay,
       should_retry: &retriable?/1}
    ]

    adapter =
      {Tesla.Adapter.Finch,
       name: @finch_pool, pool_timeout: @pool_timeout, receive_timeout: @receive_timeout}

    Tesla.client(middleware, adapter)
  end

  @spec retriable?({:ok, Tesla.Env.t()} | {:error, term()}) :: boolean()
  defp retriable?({:ok, %Tesla.Env{status: status}}) when status >= 500, do: true
  defp retriable?({:ok, %Tesla.Env{status: 429}}), do: true
  defp retriable?({:ok, _env}), do: false

  defp retriable?({:error, reason})
       when reason in [:timeout, :econnrefused, :econnreset, :closed],
       do: true

  defp retriable?({:error, _reason}), do: false

  @doc """
  Inserts a pre-gzipped RowBinary payload directly into ClickHouse.

  Skips encoding and compression — callers must supply a valid gzip-compressed RowBinary binary.
  Intended for use with streaming-zlib pipelines that build the compressed body incrementally.
  """
  @spec insert_compressed(
          Backend.t() | Keyword.t(),
          table :: String.t(),
          TypeDetection.event_type(),
          compressed :: binary(),
          opts :: keyword()
        ) :: :ok | {:error, String.t()}
  def insert_compressed(backend_or_conn_opts, table, event_type, compressed, opts \\ [])

  def insert_compressed(%Backend{} = backend, table, event_type, compressed, opts)
      when is_event_type(event_type) and is_binary(compressed) do
    with {:ok, connection_opts} <- build_connection_opts(backend) do
      insert_compressed(connection_opts, table, event_type, compressed, opts)
    end
  end

  def insert_compressed(connection_opts, table, event_type, compressed, opts)
      when is_list(connection_opts) and is_non_empty_binary(table) and is_event_type(event_type) and
             is_binary(compressed) do
    do_insert(connection_opts, table, event_type, compressed, opts)
  end

  @doc false
  @spec encode_mapping_config_id(String.t()) :: iodata()
  def encode_mapping_config_id(config_id), do: RowBinaryEncoder.uuid(config_id)

  @doc false
  @spec encode_row(LogEvent.t(), TypeDetection.event_type()) :: iodata()
  def encode_row(%LogEvent{body: body} = event, event_type) when is_event_type(event_type) do
    encode_row(event, event_type, encode_mapping_config_id(body["mapping_config_id"]))
  end

  @doc false
  @spec encode_row(LogEvent.t(), TypeDetection.event_type(), iodata()) :: iodata()
  def encode_row(%LogEvent{} = event, :log, mapping_config_id),
    do: encode_log_row(event, mapping_config_id)

  def encode_row(%LogEvent{} = event, :metric, mapping_config_id),
    do: encode_metric_row(event, mapping_config_id)

  def encode_row(%LogEvent{} = event, :trace, mapping_config_id),
    do: encode_trace_row(event, mapping_config_id)

  @doc false
  @spec encode_batch([LogEvent.t()], TypeDetection.event_type()) :: iodata()
  def encode_batch([%LogEvent{body: body} | _] = rows, event_type)
      when is_event_type(event_type) do
    # mapping_config_id is uniform across the batch (set from the event-type's
    # config_id), so encode it once. Do NOT hoist source_uuid/source_name the
    # same way -- a batch can mix sources (see pipeline.ex), so those vary per row.
    mapping_config_id = RowBinaryEncoder.uuid(body["mapping_config_id"])
    Enum.map(rows, &encode_row(&1, event_type, mapping_config_id))
  end

  @doc false
  defdelegate columns_for_type(event_type), to: QueryTemplates

  @spec encode_log_row(LogEvent.t(), iodata()) :: iodata()
  defp encode_log_row(
         %LogEvent{
           id: id,
           body: body,
           source_uuid: source_uuid,
           source_name: source_name,
           ingested_at: ingested_at
         },
         mapping_config_id
       ) do
    source_uuid_str = Atom.to_string(source_uuid)

    [
      RowBinaryEncoder.uuid(id),
      RowBinaryEncoder.string(source_uuid_str),
      RowBinaryEncoder.string(source_name || ""),
      RowBinaryEncoder.string(body["project"] || ""),
      RowBinaryEncoder.string(body["trace_id"] || ""),
      RowBinaryEncoder.string(body["span_id"] || ""),
      RowBinaryEncoder.uint8(body["trace_flags"] || 0),
      RowBinaryEncoder.string(body["severity_text"] || ""),
      RowBinaryEncoder.uint8(body["severity_number"] || 0),
      RowBinaryEncoder.string(body["service_name"] || ""),
      RowBinaryEncoder.string(body["event_message"] || ""),
      RowBinaryEncoder.string(body["scope_name"] || ""),
      RowBinaryEncoder.string(body["scope_version"] || ""),
      RowBinaryEncoder.string(body["scope_schema_url"] || ""),
      RowBinaryEncoder.string(body["resource_schema_url"] || ""),
      RowBinaryEncoder.map_string_string(body["resource_attributes"] || %{}),
      RowBinaryEncoder.map_string_string(body["scope_attributes"] || %{}),
      RowBinaryEncoder.map_string_string(body["log_attributes"] || %{}),
      mapping_config_id,
      RowBinaryEncoder.nullable(ingested_at, &RowBinaryEncoder.datetime64(&1, 6)),
      RowBinaryEncoder.int64(body["timestamp"])
    ]
  end

  @spec encode_metric_row(LogEvent.t(), iodata()) :: iodata()
  defp encode_metric_row(
         %LogEvent{
           id: id,
           body: body,
           source_uuid: source_uuid,
           source_name: source_name,
           ingested_at: ingested_at
         },
         mapping_config_id
       ) do
    source_uuid_str = Atom.to_string(source_uuid)

    [
      RowBinaryEncoder.uuid(id),
      RowBinaryEncoder.string(source_uuid_str),
      RowBinaryEncoder.string(source_name || ""),
      RowBinaryEncoder.string(body["project"] || ""),
      RowBinaryEncoder.nullable(body["time_unix"], &RowBinaryEncoder.int64/1),
      RowBinaryEncoder.nullable(body["start_time_unix"], &RowBinaryEncoder.int64/1),
      RowBinaryEncoder.string(body["metric_name"] || ""),
      RowBinaryEncoder.string(body["metric_description"] || ""),
      RowBinaryEncoder.string(body["metric_unit"] || ""),
      RowBinaryEncoder.enum8(body["metric_type"] || 1),
      RowBinaryEncoder.string(body["service_name"] || ""),
      RowBinaryEncoder.string(body["event_message"] || ""),
      RowBinaryEncoder.string(body["scope_name"] || ""),
      RowBinaryEncoder.string(body["scope_version"] || ""),
      RowBinaryEncoder.string(body["scope_schema_url"] || ""),
      RowBinaryEncoder.string(body["resource_schema_url"] || ""),
      RowBinaryEncoder.map_string_string(body["resource_attributes"] || %{}),
      RowBinaryEncoder.map_string_string(body["scope_attributes"] || %{}),
      RowBinaryEncoder.map_string_string(body["attributes"] || %{}),
      RowBinaryEncoder.string(body["aggregation_temporality"] || ""),
      RowBinaryEncoder.bool(body["is_monotonic"] || false),
      RowBinaryEncoder.uint32(body["flags"] || 0),
      RowBinaryEncoder.float64(body["value"] || 0),
      RowBinaryEncoder.uint64(body["count"] || 0),
      RowBinaryEncoder.float64(body["sum"] || 0),
      RowBinaryEncoder.float64(body["min"] || 0),
      RowBinaryEncoder.float64(body["max"] || 0),
      RowBinaryEncoder.int32(body["scale"] || 0),
      RowBinaryEncoder.uint64(body["zero_count"] || 0),
      RowBinaryEncoder.int32(body["positive_offset"] || 0),
      RowBinaryEncoder.int32(body["negative_offset"] || 0),
      RowBinaryEncoder.array_uint64(body["bucket_counts"] || []),
      RowBinaryEncoder.array_float64(body["explicit_bounds"] || []),
      RowBinaryEncoder.array_uint64(body["positive_bucket_counts"] || []),
      RowBinaryEncoder.array_uint64(body["negative_bucket_counts"] || []),
      RowBinaryEncoder.array_float64(body["quantile_values"] || []),
      RowBinaryEncoder.array_float64(body["quantiles"] || []),
      RowBinaryEncoder.array(
        body["exemplars.filtered_attributes"] || [],
        &RowBinaryEncoder.map_string_string/1
      ),
      RowBinaryEncoder.array(body["exemplars.time_unix"] || [], &RowBinaryEncoder.int64/1),
      RowBinaryEncoder.array_float64(body["exemplars.value"] || []),
      RowBinaryEncoder.array_string(body["exemplars.span_id"] || []),
      RowBinaryEncoder.array_string(body["exemplars.trace_id"] || []),
      mapping_config_id,
      RowBinaryEncoder.nullable(ingested_at, &RowBinaryEncoder.datetime64(&1, 6)),
      RowBinaryEncoder.int64(body["timestamp"])
    ]
  end

  @spec encode_trace_row(LogEvent.t(), iodata()) :: iodata()
  defp encode_trace_row(
         %LogEvent{
           id: id,
           body: body,
           source_uuid: source_uuid,
           source_name: source_name,
           ingested_at: ingested_at
         },
         mapping_config_id
       ) do
    source_uuid_str = Atom.to_string(source_uuid)

    [
      RowBinaryEncoder.uuid(id),
      RowBinaryEncoder.string(source_uuid_str),
      RowBinaryEncoder.string(source_name || ""),
      RowBinaryEncoder.string(body["project"] || ""),
      RowBinaryEncoder.string(body["trace_id"] || ""),
      RowBinaryEncoder.string(body["span_id"] || ""),
      RowBinaryEncoder.string(body["parent_span_id"] || ""),
      RowBinaryEncoder.string(body["trace_state"] || ""),
      RowBinaryEncoder.string(body["span_name"] || ""),
      RowBinaryEncoder.string(body["span_kind"] || ""),
      RowBinaryEncoder.string(body["service_name"] || ""),
      RowBinaryEncoder.string(body["event_message"] || ""),
      RowBinaryEncoder.uint64(body["duration"] || 0),
      RowBinaryEncoder.string(body["status_code"] || ""),
      RowBinaryEncoder.string(body["status_message"] || ""),
      RowBinaryEncoder.string(body["scope_name"] || ""),
      RowBinaryEncoder.string(body["scope_version"] || ""),
      RowBinaryEncoder.map_string_string(body["resource_attributes"] || %{}),
      RowBinaryEncoder.map_string_string(body["span_attributes"] || %{}),
      RowBinaryEncoder.array(body["events.timestamp"] || [], &RowBinaryEncoder.int64/1),
      RowBinaryEncoder.array_string(body["events.name"] || []),
      RowBinaryEncoder.array(
        body["events.attributes"] || [],
        &RowBinaryEncoder.map_string_string/1
      ),
      RowBinaryEncoder.array_string(body["links.trace_id"] || []),
      RowBinaryEncoder.array_string(body["links.span_id"] || []),
      RowBinaryEncoder.array_string(body["links.trace_state"] || []),
      RowBinaryEncoder.array(
        body["links.attributes"] || [],
        &RowBinaryEncoder.map_string_string/1
      ),
      mapping_config_id,
      RowBinaryEncoder.nullable(ingested_at, &RowBinaryEncoder.datetime64(&1, 6)),
      RowBinaryEncoder.int64(body["timestamp"])
    ]
  end

  @spec build_connection_opts(Backend.t()) :: {:ok, Keyword.t()} | {:error, String.t()}
  defp build_connection_opts(%Backend{config: config}) do
    %{
      database: database,
      username: username,
      password: password
    } = config

    {url, port} = resolve_ingest_url(config)

    {:ok,
     [
       url: url,
       port: port,
       database: database,
       username: username,
       password: password
     ]}
  end

  defp build_connection_opts(_backend) do
    {:error, "Unable to build connection options"}
  end

  @spec resolve_ingest_url(map()) :: {String.t(), pos_integer()}
  defp resolve_ingest_url(%{facade_url: facade_url}) when is_non_empty_binary(facade_url) do
    {facade_url, facade_port(facade_url)}
  end

  defp resolve_ingest_url(%{url: url, port: port}), do: {url, port}

  @spec facade_port(String.t()) :: pos_integer()
  defp facade_port(facade_url) do
    %URI{scheme: scheme} = URI.parse(facade_url)

    case Regex.run(~r{://(?:[^/?#@]*@)?[^/?#:]+:(\d+)}, facade_url) do
      [_, port] -> String.to_integer(port)
      _ -> default_port(scheme || "http")
    end
  end

  @spec build_request_url(
          connection_opts :: Keyword.t(),
          table :: String.t(),
          TypeDetection.event_type(),
          opts :: keyword()
        ) :: String.t()
  defp build_request_url(connection_opts, table, event_type, opts) do
    base_url = Keyword.get(connection_opts, :url)
    database = Keyword.get(connection_opts, :database)

    uri = URI.parse(base_url)
    scheme = uri.scheme || "http"
    host = uri.host
    port = Keyword.get(connection_opts, :port, default_port(scheme))

    columns = columns_for_type(event_type) |> Enum.join(", ")
    query = "INSERT INTO #{database}.#{table} (#{columns}) FORMAT RowBinary"

    # If JSON columns are ever added to the DDL, include
    # "input_format_binary_read_json_as_string" => "1" so ClickHouse reads
    # RowBinary JSON values as plain strings and parses them server-side.
    params =
      %{
        "query" => query,
        "low_cardinality_allow_in_native_format" => "0"
      }
      |> merge_settings(opts)
      |> URI.encode_query()

    "#{scheme}://#{host}:#{port}/?#{params}"
  end

  @spec merge_settings(map(), keyword()) :: map()
  defp merge_settings(params, opts) do
    Enum.reduce(opts, params, fn {key, value}, acc ->
      Map.put(acc, to_string(key), to_string(value))
    end)
  end

  defp default_port("https"), do: 8443
  defp default_port(_), do: 8123
end
