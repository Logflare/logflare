defmodule Logflare.Backends.UserMonitoring.QueryTraces do
  @moduledoc """
  Records queries that Logflare executes for a user in the user's `system.traces` source.

  Wrap a backend query with `measure/3`. The query runs in the calling process and its
  result is returned unchanged:

      QueryTraces.measure(user_id, %{query_type: :endpoint, query: sql}, fn ->
        adaptor.execute_query(backend, query_args, opts)
      end)

  The caller only sends the trace to this process and never waits for it. This process ingests
  traces one at a time, each in a supervised task, so a failing lookup or ingest can not crash
  it. The task checks whether the user has system monitoring on before ingesting.

  A trace is dropped and `[:logflare, :user_monitoring, :query_traces, :dropped]` is emitted when
  this process is down, when its mailbox holds `max_queue_len` traces (10 000 by default, set with
  `config :logflare, Logflare.Backends.UserMonitoring.QueryTraces, max_queue_len: n`), or when
  its ingest task fails or takes longer than 15 seconds.
  """

  use GenServer

  import Logflare.Utils.Guards

  alias Logflare.Backends.QueryError
  alias Logflare.Logs.Processor
  alias Logflare.Logs.Raw
  alias Logflare.Sources
  alias Logflare.Sources.Source
  alias Logflare.User
  alias Logflare.Users

  @default_max_queue_len 10_000
  @ingest_timeout :timer.seconds(15)

  @type attrs :: %{
          required(:query_type) => atom(),
          required(:query) => String.t() | (-> String.t() | nil),
          optional(:endpoint_language) => atom(),
          optional(:search_kind) => atom(),
          optional(:lql) => String.t() | nil,
          optional(:source_id) => pos_integer(),
          optional(:source_token) => atom(),
          optional(:source_name) => String.t(),
          optional(:backend_type) => atom(),
          optional(:backend_id) => pos_integer() | nil,
          optional(:endpoint_id) => pos_integer() | nil,
          optional(:endpoint_uuid) => String.t() | nil,
          optional(:params) => map(),
          optional(:labels) => map()
        }

  @type query_result :: {:ok, map()} | {:error, term()}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec measure(pos_integer(), attrs(), (-> result)) :: result when result: query_result()
  def measure(user_id, attrs, fun) when is_pos_integer(user_id) and is_function(fun, 0) do
    timestamp = System.system_time(:microsecond)
    started_at = System.monotonic_time()
    result = fun.()
    duration = System.monotonic_time() - started_at

    attrs
    |> Map.merge(result_attrs(result))
    |> Map.merge(%{
      timestamp: timestamp,
      duration_ms: System.convert_time_unit(duration, :native, :millisecond)
    })
    |> then(&push(user_id, &1))

    result
  end

  @doc """
  Builds the `system.traces` event body.

  `params` and `labels` become lists of `%{"name" => name, "value" => value}` records, so the
  source schema does not grow with each new parameter or label name. A `query` function is
  called here, so the caller does not pay for building the query text.
  """
  @spec build_event(map()) :: map()
  def build_event(attrs) do
    event =
      for {key, value} <- attrs,
          value = event_value(key, value),
          not is_nil(value),
          into: %{} do
        {Atom.to_string(key), value}
      end

    Map.put(event, "event_message", "query.#{attrs.query_type}")
  end

  @impl GenServer
  def init(_opts), do: {:ok, %{}}

  @impl GenServer
  def handle_info({:trace, user_id, attrs}, state) do
    task =
      Task.Supervisor.async_nolink(
        {:via, PartitionSupervisor, {Logflare.TaskSupervisors, user_id}},
        fn -> ingest(user_id, attrs) end
      )

    case Task.yield(task, @ingest_timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, _result} -> :ok
      _failed -> drop(:failed)
    end

    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @spec ingest(pos_integer(), map()) :: term()
  defp ingest(user_id, attrs) do
    with %User{system_monitoring: true} <- Users.Cache.get(user_id),
         %Source{} = source <- Sources.Cache.get_by(user_id: user_id, system_source_type: :traces) do
      Processor.ingest([build_event(attrs)], Raw, source)
    end
  end

  @spec push(pos_integer(), map()) :: :ok
  defp push(user_id, attrs) do
    max_queue_len = max_queue_len()

    with pid when is_pid(pid) <- Process.whereis(__MODULE__),
         {:message_queue_len, len} when len < max_queue_len <-
           Process.info(pid, :message_queue_len) do
      send(pid, {:trace, user_id, attrs})
      :ok
    else
      {:message_queue_len, _len} -> drop(:saturated)
      nil -> drop(:unavailable)
    end
  end

  @spec drop(:saturated | :unavailable | :failed) :: :ok
  defp drop(reason) do
    :telemetry.execute(
      [:logflare, :user_monitoring, :query_traces, :dropped],
      %{count: 1},
      %{reason: reason}
    )
  end

  @spec max_queue_len() :: non_neg_integer()
  defp max_queue_len do
    Application.get_env(:logflare, __MODULE__, [])[:max_queue_len] || @default_max_queue_len
  end

  @spec result_attrs(query_result()) :: map()
  defp result_attrs({:ok, %{total_bytes_processed: bytes}}) when is_integer(bytes),
    do: %{status: :ok, total_bytes_processed: bytes}

  defp result_attrs({:ok, _result}), do: %{status: :ok}
  defp result_attrs({:error, error}), do: %{status: :error, error: error_message(error)}

  @spec error_message(term()) :: String.t()
  defp error_message(%QueryError{description: description}) when is_non_empty_binary(description),
    do: description

  defp error_message(%QueryError{kind: kind}), do: Atom.to_string(kind)
  defp error_message(error) when is_binary(error), do: error
  defp error_message(error), do: inspect(error)

  @spec event_value(atom(), term()) :: term()
  defp event_value(_key, nil), do: nil
  defp event_value(:query, fun) when is_function(fun, 0), do: fun.()

  defp event_value(key, map) when key in [:params, :labels] and is_map(map) do
    for {name, value} <- Enum.sort(map), not is_nil(value) do
      %{"name" => to_string(name), "value" => stringify(value)}
    end
  end

  defp event_value(_key, value) when is_atom(value), do: Atom.to_string(value)
  defp event_value(_key, value), do: value

  @spec stringify(term()) :: String.t()
  defp stringify(value) when is_binary(value), do: value
  defp stringify(value) when is_number(value) or is_atom(value), do: to_string(value)
  defp stringify(value), do: Jason.encode!(value)
end
