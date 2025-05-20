defmodule Logflare.Backends.Adaptor.ClickhouseAdaptor.Client do
  @moduledoc false

  alias Logflare.Backends.Adaptor.ClickhouseAdaptor.Supervisor
  alias Logflare.Backends.Backend
  alias Logflare.LogEvent
  alias Logflare.Source
  require Logger

  @doc """
  Returns a connection PID for a given `Backend`.
  """
  @spec get_connection(Backend.t()) :: pid()
  def get_connection(%Backend{} = backend) do
    {:ok, connection_pid} = Supervisor.find_or_create_ch_connection(backend)

    connection_pid
  end

  @doc """
  Returns the table name for a given Source.
  """
  @spec table_name(Source.t()) :: binary()
  def table_name(%Source{token: token}) do
    token
    |> Atom.to_string()
    |> String.replace("-", "_")
    |> then(&"log_events_#{&1}")
  end

  @doc """
  Wrapper function around `Ch.query/4`.

  If provided with a `Backend`, it will first retrieve the `DbConnection` PID using `get_connection/1`.
  """
  @spec execute_ch_query(
          Backend.t() | DBConnection.conn(),
          statement :: iodata(),
          params :: map | [term] | [row :: [term]] | iodata | Enumerable.t(),
          [Ch.query_option()]
        ) :: {:ok, Ch.Result.t()} | {:error, Exception.t()}
  def execute_ch_query(backend_or_conn, statement, params \\ [], opts \\ [])

  def execute_ch_query(%Backend{} = backend, statement, params, opts)
      when is_list(params) and is_list(opts) do
    backend
    |> get_connection()
    |> execute_ch_query(statement, params, opts)
  end

  def execute_ch_query(conn, statement, params, opts)
      when is_pid(conn) and is_list(params) and is_list(opts) do
    Ch.query(conn, statement, params, opts)
  end

  @doc """
  Inserts a `LogEvent` struct into the given source backend table
  """
  @spec insert_log_event(Source.t(), Backend.t(), LogEvent.t()) :: {:ok, PgLogEvent.t()}
  def insert_log_event(source, backend, %LogEvent{} = le),
    do: insert_log_events(source, backend, [le])

  @doc """
  Inserts a list of `LogEvent` structs into the given source backend table
  """
  @spec insert_log_events(Source.t(), Backend.t(), [LogEvent.t()]) :: {:ok, [PgLogEvent.t()]}
  def insert_log_events(_source, backend, events) when is_list(events) do
    config = Map.get(backend, :config)
    table = Map.get(config, :table)

    # table = table_name(source)

    conn_pid = get_connection(backend)

    event_params =
      Enum.map(events, fn log_event ->
        body = Map.drop(log_event.body, ["id", "event_message", "timestamp"])

        [
          log_event.body["id"],
          log_event.body["event_message"],
          Jason.encode!(body),
          DateTime.from_unix!(log_event.body["timestamp"], :microsecond)
        ]
      end)

    IO.inspect(event_params, label: "event_params", limit: :infinity, pretty: true)

    opts = [
      names: ["id", "event_message", "body", "timestamp"],
      types: ["UUID", "String", "String", "DateTime64(6)"]
    ]

    query_res =
      execute_ch_query(
        conn_pid,
        "INSERT INTO #{table} FORMAT RowBinaryWithNamesAndTypes",
        event_params,
        opts
      )

    IO.inspect(query_res, label: "query result", limit: :infinity, pretty: true)

    query_res
  end
end
