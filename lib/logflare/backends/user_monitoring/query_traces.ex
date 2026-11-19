defmodule Logflare.Backends.UserMonitoring.QueryTraces do
  @moduledoc """
  Records queries that Logflare executes for a user in the user's `system.traces` source.

  Wrap a backend query with `measure/3`. The query runs in the calling process and its
  result is returned unchanged:

      QueryTraces.measure(user_id, %{query_type: :endpoint, query: sql}, fn ->
        adaptor.execute_query(backend, query_args, opts)
      end)

  Traces are recorded only for users with system monitoring on. Each trace is ingested in a
  separate task, so tracing never adds latency to the query or fails it.
  """

  import Logflare.Utils.Guards

  alias Logflare.Backends.Adaptor.QueryResult
  alias Logflare.Backends.QueryError
  alias Logflare.Logs.Processor
  alias Logflare.Logs.Raw
  alias Logflare.Sources
  alias Logflare.Sources.Source
  alias Logflare.User
  alias Logflare.Users

  @type attrs :: %{
          required(:query_type) => atom(),
          required(:query) => String.t(),
          optional(:query_language) => atom(),
          optional(:backend_type) => atom(),
          optional(:backend_id) => pos_integer() | nil,
          optional(:endpoint_id) => pos_integer() | nil,
          optional(:endpoint_uuid) => String.t() | nil,
          optional(:params) => map(),
          optional(:labels) => map()
        }

  @type query_result :: {:ok, QueryResult.t() | map()} | {:error, term()}

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
    |> then(&record(user_id, &1))

    result
  end

  @spec record(pos_integer(), map()) :: :ok
  defp record(user_id, attrs) do
    with %User{system_monitoring: true} <- Users.Cache.get(user_id) do
      Task.Supervisor.start_child(
        {:via, PartitionSupervisor, {Logflare.TaskSupervisors, user_id}},
        fn -> ingest(user_id, to_event(attrs)) end
      )
    end

    :ok
  end

  @spec ingest(pos_integer(), map()) :: term()
  defp ingest(user_id, event) do
    with %Source{} = source <- Sources.Cache.get_by(user_id: user_id, system_source_type: :traces) do
      Processor.ingest([event], Raw, source)
    end
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

  @spec to_event(map()) :: map()
  defp to_event(attrs) do
    event =
      for {key, value} <- attrs, not is_nil(value), into: %{} do
        {Atom.to_string(key), event_value(key, value)}
      end

    Map.put(event, "event_message", "query.#{attrs.query_type}")
  end

  @spec event_value(atom(), term()) :: term()
  defp event_value(key, map) when key in [:params, :labels] and is_map(map) do
    for {k, v} <- map, not is_nil(v), into: %{}, do: {to_string(k), stringify(v)}
  end

  defp event_value(_key, value) when is_atom(value), do: Atom.to_string(value)
  defp event_value(_key, value), do: value

  @spec stringify(term()) :: String.t()
  defp stringify(value) when is_binary(value), do: value
  defp stringify(value) when is_number(value) or is_atom(value), do: to_string(value)
  defp stringify(value), do: Jason.encode!(value)
end
