defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.Ingester do
  @moduledoc """
  Simplified ingestion-only functionality for ClickHouse.
  """

  import Logflare.Utils.Guards

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.RowBinaryEncoder
  alias Logflare.Backends.Backend
  alias Logflare.LogEvent

  @finch_pool Logflare.FinchClickHouseIngest
  @max_retries 1
  @initial_delay 500
  @max_delay 4_000
  @pool_timeout 8_000
  @receive_timeout 30_000

  @doc """
  Inserts a list of `LogEvent` structs into ClickHouse.

  Not intended for direct use. Use `Logflare.Backends.Adaptor.ClickHouseAdaptor.insert_log_events/2` instead.
  """
  @spec insert(Backend.t() | Keyword.t(), table :: String.t(), log_events :: [LogEvent.t()]) ::
          :ok | {:error, String.t()}
  def insert(_backend_or_conn_opts, _table, []), do: :ok

  def insert(%Backend{} = backend, table, log_events) when is_list(log_events) do
    with {:ok, connection_opts} <- build_connection_opts(backend) do
      insert(connection_opts, table, log_events)
    end
  end

  def insert(connection_opts, table, [%LogEvent{} | _] = log_events)
      when is_list(connection_opts) and is_non_empty_binary(table) do
    client = build_client(connection_opts)
    url = build_request_url(connection_opts, table)
    request_body = log_events |> encode_batch() |> :zlib.gzip()

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
  @spec encode_row(LogEvent.t()) :: iodata()
  def encode_row(%LogEvent{
        body: body,
        origin_source_uuid: origin_source_uuid,
        origin_source_name: origin_source_name,
        ingested_at: ingested_at
      }) do
    source_uuid_str = Atom.to_string(origin_source_uuid)
    ingested_at = ingested_at || NaiveDateTime.utc_now()

    [
      RowBinaryEncoder.uuid(body["id"]),
      RowBinaryEncoder.uuid(source_uuid_str),
      RowBinaryEncoder.string(origin_source_name || ""),
      RowBinaryEncoder.string(Jason.encode_to_iodata!(body)),
      RowBinaryEncoder.datetime64(DateTime.from_naive!(ingested_at, "Etc/UTC"), 6),
      RowBinaryEncoder.datetime64(DateTime.from_unix!(body["timestamp"], :microsecond), 6)
    ]
  end

  @doc false
  @spec encode_batch([LogEvent.t()]) :: iodata()
  def encode_batch([%LogEvent{} | _] = rows) do
    Enum.map(rows, &encode_row/1)
  end

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

  @spec build_request_url(connection_opts :: Keyword.t(), table :: String.t()) :: String.t()
  defp build_request_url(connection_opts, table) do
    base_url = Keyword.get(connection_opts, :url)
    database = Keyword.get(connection_opts, :database)
    async_insert = Keyword.get(connection_opts, :async_insert, false)

    uri = URI.parse(base_url)
    scheme = uri.scheme || "http"
    host = uri.host
    port = Keyword.get(connection_opts, :port, default_port(scheme))

    query = "INSERT INTO #{database}.#{table} FORMAT RowBinary"

    params =
      %{"query" => query}
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
