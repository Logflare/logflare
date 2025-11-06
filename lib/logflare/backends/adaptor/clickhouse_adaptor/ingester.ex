defmodule Logflare.Backends.Adaptor.ClickhouseAdaptor.Ingester do
  @moduledoc """
  Simplified ingestion-only functionality for ClickHouse.
  """

  use TypedStruct

  import Bitwise
  import Logflare.Utils.Guards

  alias Logflare.Backends.Backend
  alias Logflare.LogEvent

  typedstruct module: Row do
    field :id, Ecto.UUID.t()
    field :body, String.t()
    field :timestamp, DateTime.t()
  end

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
          rows :: [LogEvent.t()] | [__MODULE__.Row.t()],
          opts :: Keyword.t()
        ) ::
          :ok | {:error, String.t()}
  def insert(backend_or_conn_opts, table, rows, opts \\ [])

  def insert(_backend_or_conn_opts, _table, [], _opts), do: :ok

  def insert(%Backend{} = backend, table, rows, opts) when is_list(rows) and is_list(opts) do
    with {:ok, connection_opts} <- build_connection_opts(backend) do
      insert(connection_opts, table, rows, opts)
    end
  end

  def insert(connection_opts, table, [%LogEvent{} | _] = rows, opts)
      when is_list(connection_opts) and is_non_empty_binary(table) and is_list(opts) do
    converted_rows =
      rows
      |> Enum.map(fn %LogEvent{} = log_event ->
        %__MODULE__.Row{
          id: log_event.body["id"],
          body: Jason.encode!(log_event.body),
          timestamp: DateTime.from_unix!(log_event.body["timestamp"], :microsecond)
        }
      end)

    insert(connection_opts, table, converted_rows, opts)
  end

  def insert(connection_opts, table, [%__MODULE__.Row{} | _] = rows, opts)
      when is_list(connection_opts) and is_non_empty_binary(table) and is_list(rows) and
             is_list(opts) do
    compress = Keyword.get(opts, :compress, true)
    async = Keyword.get(opts, :async, true)
    url = build_url(connection_opts, table, async)
    body = encode_batch(rows)

    {body, headers} =
      if compress do
        compressed = :zlib.gzip(body)
        {compressed, [{"content-encoding", "gzip"}]}
      else
        {body, []}
      end

    headers = [
      {"content-type", "application/octet-stream"},
      build_auth_header(connection_opts)
      | headers
    ]

    request = Finch.build(:post, url, headers, body)

    case Finch.request(request, @finch_pool) do
      {:ok, %{status: 200}} -> :ok
      {:ok, %{status: status, body: body}} -> {:error, "HTTP #{status}: #{body}"}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc false
  @spec encode_row(__MODULE__.Row.t()) :: binary()
  def encode_row(%__MODULE__.Row{id: id, body: body, timestamp: timestamp}) do
    <<
      encode_uuid(id)::binary,
      encode_string(body)::binary,
      encode_datetime64(timestamp)::binary
    >>
  end

  @doc false
  @spec encode_batch([__MODULE__.Row.t()]) :: binary()
  def encode_batch([%__MODULE__.Row{} | _] = rows) do
    rows
    |> Enum.map(&encode_row/1)
    |> :erlang.iolist_to_binary()
  end

  @doc false
  @spec encode_uuid(Ecto.UUID.t() | String.t()) :: binary()
  def encode_uuid(uuid_string) when is_non_empty_binary(uuid_string) do
    uuid_string
    |> String.replace("-", "")
    |> Base.decode16!(case: :mixed)
  end

  @doc false
  @spec encode_string(String.t()) :: binary()
  def encode_string(value) when is_non_empty_binary(value) do
    bytes = :unicode.characters_to_binary(value, :utf8)
    length = byte_size(bytes)
    <<encode_varint(length)::binary, bytes::binary>>
  end

  @doc false
  @spec encode_datetime64(DateTime.t()) :: binary()
  def encode_datetime64(%DateTime{microsecond: {microsecond, _precision}} = value) do
    timestamp_seconds = DateTime.to_unix(value, :second)
    timestamp_scaled = timestamp_seconds * 1_000_000 + microsecond
    <<timestamp_scaled::little-signed-64>>
  end

  @doc false
  @spec encode_varint(non_neg_integer()) :: binary()
  def encode_varint(n) when is_non_negative_integer(n) and n < 128, do: <<n>>

  def encode_varint(n) when is_non_negative_integer(n),
    do: <<1::1, n::7, encode_varint(n >>> 7)::binary>>

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

  @spec build_url(connection_opts :: Keyword.t(), table :: String.t(), async :: boolean()) ::
          String.t()
  defp build_url(connection_opts, table, async) do
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
