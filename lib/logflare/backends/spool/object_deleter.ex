defmodule Logflare.Backends.Spool.ObjectDeleter do
  @moduledoc """
  Deletes spool objects from storage once their queue message has been acked,
  without ever blocking the caller.

  `enqueue/1` is an ETS insert into a bounded FIFO buffer (plus a wake-up message
  when the insert made it non-empty), so it is safe to call from latency-sensitive
  processes. A single worker drains the buffer
  with at most `:delete_concurrency` storage requests in flight, each killed
  after `:delete_timeout_ms`.

  When the buffer already holds `:delete_buffer_max` entries, new objects are
  dropped rather than queued, which bounds memory. A dropped, timed-out or
  failed delete is not retried: the object is left for the bucket's lifecycle
  rule. Entries still buffered when the node stops are lost the same way,
  unless the shutdown drain waits for them with `flush/1`.

  Failures are reported through telemetry for every delete, and logged at most
  once per `@log_interval_ms` with the counts accumulated since the last log, so
  an unreachable store cannot flood the logs.

  Tuned through `config :logflare, :spool`, or the `SPOOL_DELETE_BUFFER_MAX`,
  `SPOOL_DELETE_CONCURRENCY` and `SPOOL_DELETE_TIMEOUT_MS` env vars.
  """

  use GenServer

  require Logger

  alias Logflare.Backends.Spool.Storage

  @table :spool_object_deleter
  @default_buffer_max 50_000
  @default_concurrency 32
  @default_timeout_ms 2_000
  @idle_interval_ms 1_000
  @log_interval_ms 10_000

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl GenServer
  def init(_opts) do
    :ets.new(@table, [:public, :named_table, :ordered_set, write_concurrency: true])
    send(self(), :drain)
    {:ok, %{timer: nil, last_log_at: nil, failed: %{}}}
  end

  @spec enqueue(Storage.object()) :: :ok | :dropped
  def enqueue({_storage_mod, _bucket, _key} = object) do
    max_pending = config(:delete_buffer_max, @default_buffer_max)

    case pending_count() do
      :undefined -> drop(:not_running)
      pending when pending >= max_pending -> drop(:buffer_full)
      _pending -> insert(object)
    end
  end

  defp insert(object) do
    :ets.insert(@table, {System.unique_integer([:monotonic]), object})
    if pending_count() == 1, do: send(__MODULE__, :drain)
    :ok
  rescue
    ArgumentError -> drop(:not_running)
  end

  defp drop(reason) do
    :telemetry.execute(
      [:logflare, :backends, :spool, :delete_buffer, :dropped],
      %{count: 1},
      %{reason: reason}
    )

    :dropped
  end

  @spec pending_count() :: non_neg_integer() | :undefined
  def pending_count, do: :ets.info(@table, :size)

  @doc """
  Returns once everything buffered when called, and anything added while it
  waits, has been attempted, or `:timeout` if that takes longer than `timeout_ms`.
  """
  @spec flush(timeout()) :: :ok | :timeout
  def flush(timeout_ms) do
    GenServer.call(__MODULE__, :flush, timeout_ms)
  catch
    :exit, {:timeout, _call} -> :timeout
  end

  @impl GenServer
  def handle_call(:flush, _from, state), do: {:reply, :ok, drain_pass(state)}

  @impl GenServer
  def handle_info(:drain, %{timer: timer} = state) do
    if timer, do: Process.cancel_timer(timer)
    state = drain_pass(state)
    {:noreply, %{state | timer: Process.send_after(self(), :drain, @idle_interval_ms)}}
  end

  defp drain_pass(state) do
    failed = if pending_count() > 0, do: drain(), else: %{}

    state
    |> Map.update!(:failed, &Map.merge(&1, failed, fn _tag, a, b -> a + b end))
    |> maybe_log_failures()
  end

  defp drain do
    results =
      {:via, PartitionSupervisor, {Logflare.TaskSupervisors, self()}}
      |> Task.Supervisor.async_stream_nolink(
        Stream.repeatedly(&pop/0) |> Stream.take_while(& &1),
        fn {storage_mod, bucket, key} -> storage_mod.delete(bucket, key) end,
        max_concurrency: config(:delete_concurrency, @default_concurrency),
        timeout: config(:delete_timeout_ms, @default_timeout_ms),
        on_timeout: :kill_task,
        ordered: false
      )
      |> Enum.frequencies_by(&result_tag/1)

    Map.delete(results, :ok)
  end

  defp pop do
    case :ets.first(@table) do
      :"$end_of_table" ->
        nil

      key ->
        [{^key, object}] = :ets.take(@table, key)
        object
    end
  end

  defp result_tag(result) do
    tag =
      case result do
        {:ok, :ok} -> :ok
        {:exit, :timeout} -> :timeout
        _ -> :error
      end

    :telemetry.execute([:logflare, :backends, :spool, :storage, :delete], %{}, %{result: tag})
    tag
  end

  defp maybe_log_failures(%{failed: failed} = state) when failed == %{}, do: state

  defp maybe_log_failures(%{failed: failed, last_log_at: last_log_at} = state) do
    now = System.monotonic_time(:millisecond)

    if is_nil(last_log_at) or now - last_log_at >= @log_interval_ms do
      Logger.warning("spool_object_deleter: some spool object deletes failed",
        error_string: inspect(failed)
      )

      %{state | failed: %{}, last_log_at: now}
    else
      state
    end
  end

  defp config(key, default), do: Application.get_env(:logflare, :spool, [])[key] || default
end
