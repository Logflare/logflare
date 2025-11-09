defmodule Logflare.Backends.Adaptor.ClickhouseAdaptor.Ingester do
  @moduledoc """
  Simplified ingestion-only functionality for ClickHouse.
  """

  import Bitwise
  import Logflare.Utils.Guards

  alias Logflare.Backends.Backend
  alias Logflare.LogEvent

  @finch_pool Logflare.FinchClickhouseIngest

  @doc """
  Handles inserting of a list of `LogEvent` structs into ClickHouse.

  Not intended for direct use. Use `Logflare.Backends.Adaptor.ClickhouseAdaptor.insert_log_events/2` instead.

  ## Options

  - `compress`: Boolean indicating whether to gzip compress the body before sending it to ClickHouse. Defaults to `true`.
  - `async`: Boolean indicating whether to use ClickHouse's async insert mechanism. Defaults to `true`.
  """
  @spec insert(
          Backend.t() | Keyword.t(),
          table :: String.t(),
          log_events :: [LogEvent.t()],
          opts :: Keyword.t()
        ) ::
          :ok | {:error, String.t()}
  def insert(backend_or_conn_opts, table, log_events, opts \\ [])

  def insert(_backend_or_conn_opts, _table, [], _opts), do: :ok

  def insert(%Backend{} = backend, table, log_events, opts)
      when is_list(log_events) and is_list(opts) do
    with {:ok, connection_opts} <- build_connection_opts(backend) do
      insert(connection_opts, table, log_events, opts)
    end
  end

  def insert(connection_opts, table, [%LogEvent{} | _] = log_events, opts)
      when is_list(connection_opts) and is_non_empty_binary(table) and is_list(opts) do
    compress = Keyword.get(opts, :compress, true)
    async = Keyword.get(opts, :async, true)
    url = build_request_url(connection_opts, table, async)
    request_body = encode_batch(log_events)

    {request_body, headers} =
      if compress do
        compressed = :zlib.gzip(request_body)
        {compressed, [{"content-encoding", "gzip"}]}
      else
        {request_body, []}
      end

    headers = [
      {"content-type", "application/octet-stream"},
      build_auth_header(connection_opts)
      | headers
    ]

    request = Finch.build(:post, url, headers, request_body)

    case Finch.request(request, @finch_pool) do
      {:ok, %{status: 200}} ->
        :ok

      {:ok, %{status: status, body: response_body}} ->
        {:error, "HTTP #{status}: #{response_body}"}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc false
  @spec encode_row(LogEvent.t()) :: iodata()
  def encode_row(%LogEvent{body: body}) do
    [
      encode_as_uuid(body["id"]),
      encode_as_string(Jason.encode_to_iodata!(body)),
      encode_as_datetime64(DateTime.from_unix!(body["timestamp"], :microsecond))
    ]
  end

  @doc false
  @spec encode_batch([LogEvent.t()]) :: iodata()
  def encode_batch([%LogEvent{} | _] = rows) do
    Enum.map(rows, &encode_row/1)
  end

  @doc false
  @spec encode_as_uuid(Ecto.UUID.t() | String.t()) :: binary()
  def encode_as_uuid(uuid_string) when is_non_empty_binary(uuid_string) do
    uuid_string
    |> String.replace("-", "")
    |> Base.decode16!(case: :mixed)
  end

  @doc false
  @spec encode_as_string(iodata()) :: iodata()
  def encode_as_string(value) when is_list(value) do
    length = IO.iodata_length(value)
    [encode_as_varint(length), value]
  end

  @doc false
  @spec encode_as_datetime64(DateTime.t()) :: binary()
  def encode_as_datetime64(%DateTime{microsecond: {microsecond, _precision}} = value) do
    timestamp_seconds = DateTime.to_unix(value, :second)
    timestamp_scaled = timestamp_seconds * 1_000_000 + microsecond
    <<timestamp_scaled::little-signed-64>>
  end

  @doc false
  @spec encode_as_varint(non_neg_integer()) :: binary()
  def encode_as_varint(n) when is_non_negative_integer(n) and n < 128, do: <<n>>

  def encode_as_varint(n) when is_non_negative_integer(n),
    do: <<1::1, n::7, encode_as_varint(n >>> 7)::binary>>

  @spec build_connection_opts(Backend.t()) :: {:ok, Keyword.t()} | {:error, String.t()}
  defp build_connection_opts(%Backend{
         config: %{
           url: url,
           port: port,
           database: database,
           username: username,
           password: password
         }
       }) do
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

  @spec build_request_url(connection_opts :: Keyword.t(), table :: String.t(), async :: boolean()) ::
          String.t()
  defp build_request_url(connection_opts, table, async) do
    base_url = Keyword.get(connection_opts, :url)
    database = Keyword.get(connection_opts, :database)

    uri = URI.parse(base_url)
    scheme = uri.scheme || "http"
    host = uri.host
    port = Keyword.get(connection_opts, :port, default_port(scheme))

    query = "INSERT INTO #{database}.#{table} FORMAT RowBinary"

    params =
      URI.encode_query(%{
        "query" => query,
        "async_insert" => if(async, do: "1", else: "0"),
        "wait_for_async_insert" => "0"
      })

    "#{scheme}://#{host}:#{port}/?#{params}"
  end

  @spec build_auth_header(Keyword.t()) :: {String.t(), String.t()}
  defp build_auth_header(connection_opts) when is_list(connection_opts) do
    username = Keyword.get(connection_opts, :username)
    password = Keyword.get(connection_opts, :password)
    credentials = Base.encode64("#{username}:#{password}")
    {"authorization", "Basic #{credentials}"}
  end

  defp default_port("https"), do: 8443
  defp default_port(_), do: 8123
end
