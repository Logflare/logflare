defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.Ingester do
  @moduledoc """
  Simplified ingestion-only functionality for ClickHouse.
  """

  import Logflare.Utils.Guards

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.RowBinaryEncoder
  alias Logflare.Backends.Backend
  alias Logflare.LogEvent
  alias Logflare.LogEvent.TypeDetection

  @finch_pool Logflare.FinchClickHouseIngest
  @max_retries 1
  @initial_delay 500
  @max_delay 4_000
  @pool_timeout 8_000
  @receive_timeout 30_000

  @log_columns ~w(id source_uuid source_name project event_message log_attributes timestamp)
  @metric_columns ~w(id source_uuid source_name project event_message time_unix start_time_unix metric_type attributes timestamp)
  @trace_columns ~w(id source_uuid source_name project event_message span_attributes timestamp)

  @doc """
  Inserts a list of `LogEvent` structs into ClickHouse.

  This function expects that all LogEvents share the same `log_type`.

  Not intended for direct use. Use `Logflare.Backends.Adaptor.ClickHouseAdaptor.insert_log_events/3` instead.
  """
  @spec insert(
          Backend.t() | Keyword.t(),
          table :: String.t(),
          log_events :: [LogEvent.t()],
          TypeDetection.log_type()
        ) ::
          :ok | {:error, String.t()}
  def insert(_backend_or_conn_opts, _table, [], _log_type), do: :ok

  def insert(%Backend{} = backend, table, log_events, log_type)
      when is_list(log_events) and is_log_type(log_type) do
    with {:ok, connection_opts} <- build_connection_opts(backend) do
      insert(connection_opts, table, log_events, log_type)
    end
  end

  def insert(connection_opts, table, [%LogEvent{log_type: log_type} | _] = log_events, log_type)
      when is_list(connection_opts) and is_non_empty_binary(table) and is_log_type(log_type) do
    client = build_client(connection_opts)
    url = build_request_url(connection_opts, table, log_type)
    request_body = log_events |> encode_batch(log_type) |> :zlib.gzip()

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
  defp retriable?({:error, _reason}), do: true

  @doc false
  @spec encode_row(LogEvent.t(), TypeDetection.log_type()) :: iodata()
  def encode_row(%LogEvent{} = event, :log), do: encode_log_row(event)
  def encode_row(%LogEvent{} = event, :metric), do: encode_metric_row(event)
  def encode_row(%LogEvent{} = event, :trace), do: encode_trace_row(event)

  @doc false
  @spec encode_batch([LogEvent.t()], TypeDetection.log_type()) :: iodata()
  def encode_batch([%LogEvent{} | _] = rows, log_type) when is_log_type(log_type) do
    Enum.map(rows, &encode_row(&1, log_type))
  end

  @doc false
  @spec columns_for_type(TypeDetection.log_type()) :: [String.t()]
  def columns_for_type(:log), do: @log_columns
  def columns_for_type(:metric), do: @metric_columns
  def columns_for_type(:trace), do: @trace_columns

  @spec encode_log_row(LogEvent.t()) :: iodata()
  defp encode_log_row(%LogEvent{
         id: id,
         body: body,
         origin_source_uuid: origin_source_uuid,
         origin_source_name: origin_source_name
       }) do
    source_uuid_str = Atom.to_string(origin_source_uuid)
    timestamp_us = body_timestamp_us(body["timestamp"])

    [
      # id
      RowBinaryEncoder.uuid(id),
      # source_uuid
      RowBinaryEncoder.string(source_uuid_str),
      # source_name
      RowBinaryEncoder.string(origin_source_name || ""),
      # project
      RowBinaryEncoder.string(""),
      # event_message
      RowBinaryEncoder.string(body["event_message"] || ""),
      # log_attributes
      RowBinaryEncoder.json(body),
      # timestamp
      RowBinaryEncoder.datetime64_from_unix(timestamp_us, :microsecond, 9)
    ]
  end

  @spec encode_metric_row(LogEvent.t()) :: iodata()
  defp encode_metric_row(%LogEvent{
         id: id,
         body: body,
         origin_source_uuid: origin_source_uuid,
         origin_source_name: origin_source_name
       }) do
    source_uuid_str = Atom.to_string(origin_source_uuid)
    timestamp_us = body_timestamp_us(body["timestamp"])

    [
      # id
      RowBinaryEncoder.uuid(id),
      # source_uuid
      RowBinaryEncoder.string(source_uuid_str),
      # source_name
      RowBinaryEncoder.string(origin_source_name || ""),
      # project
      RowBinaryEncoder.string(""),
      # event_message
      RowBinaryEncoder.string(body["event_message"] || ""),
      # time_unix
      RowBinaryEncoder.datetime64_from_unix(timestamp_us, :microsecond, 9),
      # start_time_unix
      RowBinaryEncoder.datetime64_from_unix(timestamp_us, :microsecond, 9),
      # metric_type
      RowBinaryEncoder.enum8(1),
      # attributes
      RowBinaryEncoder.json(body),
      # timestamp
      RowBinaryEncoder.datetime64_from_unix(timestamp_us, :microsecond, 9)
    ]
  end

  @spec encode_trace_row(LogEvent.t()) :: iodata()
  defp encode_trace_row(%LogEvent{
         id: id,
         body: body,
         origin_source_uuid: origin_source_uuid,
         origin_source_name: origin_source_name
       }) do
    source_uuid_str = Atom.to_string(origin_source_uuid)
    timestamp_us = body_timestamp_us(body["timestamp"])

    [
      # id
      RowBinaryEncoder.uuid(id),
      # source_uuid
      RowBinaryEncoder.string(source_uuid_str),
      # source_name
      RowBinaryEncoder.string(origin_source_name || ""),
      # project
      RowBinaryEncoder.string(""),
      # event_message
      RowBinaryEncoder.string(body["event_message"] || ""),
      # span_attributes
      RowBinaryEncoder.json(body),
      # timestamp
      RowBinaryEncoder.datetime64_from_unix(timestamp_us, :microsecond, 9)
    ]
  end

  @spec body_timestamp_us(integer() | nil) :: integer()
  defp body_timestamp_us(nil) do
    DateTime.to_unix(DateTime.utc_now(), :microsecond)
  end

  defp body_timestamp_us(timestamp_us) when is_pos_integer(timestamp_us), do: timestamp_us

  @spec build_connection_opts(Backend.t()) :: {:ok, Keyword.t()} | {:error, String.t()}
  defp build_connection_opts(%Backend{config: config}) do
    %{
      url: url,
      port: port,
      database: database,
      username: username,
      password: password
    } = config

    {:ok,
     [
       url: url,
       port: port,
       database: database,
       username: username,
       password: password,
       async_insert: Map.get(config, :async_insert, false)
     ]}
  end

  defp build_connection_opts(_backend) do
    {:error, "Unable to build connection options"}
  end

  @spec build_request_url(
          connection_opts :: Keyword.t(),
          table :: String.t(),
          TypeDetection.log_type()
        ) :: String.t()
  defp build_request_url(connection_opts, table, log_type) do
    base_url = Keyword.get(connection_opts, :url)
    database = Keyword.get(connection_opts, :database)
    async_insert = Keyword.get(connection_opts, :async_insert, false)

    uri = URI.parse(base_url)
    scheme = uri.scheme || "http"
    host = uri.host
    port = Keyword.get(connection_opts, :port, default_port(scheme))

    columns = columns_for_type(log_type) |> Enum.join(", ")
    query = "INSERT INTO #{database}.#{table} (#{columns}) FORMAT RowBinary"

    params =
      %{
        "query" => query,
        "low_cardinality_allow_in_native_format" => "0",
        "input_format_binary_read_json_as_string" => "1"
      }
      |> maybe_add_async_insert(async_insert)
      |> URI.encode_query()

    "#{scheme}://#{host}:#{port}/?#{params}"
  end

  @spec maybe_add_async_insert(map(), boolean()) :: map()
  defp maybe_add_async_insert(params, true) do
    params
    |> Map.put("async_insert", "1")
    |> Map.put("wait_for_async_insert", "1")
  end

  defp maybe_add_async_insert(params, _), do: params

  defp default_port("https"), do: 8443
  defp default_port(_), do: 8123
end
