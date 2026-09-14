defmodule Logflare.Backends.Spool.Partition do
  @moduledoc """
  Accumulates pre-compressed, pre-framed segments pushed by ingest callers
  via `append/5`, buffered however `buffer_mod` (`Logflare.Backends.Spool.Buffer`
  — local-disk WAL or an in-memory batch) decides.

  Owns, uniformly regardless of buffer:

    * **Reply timing** — `append/5` replies once the buffer says the
      segment is durable, or (`wait_until_committed: true`) once the batch
      it ends up part of is actually committed.
    * **When to attempt a roll** — after every append, and on a recurring
      `batch_timeout` timer that forces a roll of whatever's accumulated.
    * **The commit task's lifecycle** — via `Committer`, bounded by
      `max_inflight_commits` (config, default 10), and crash recovery.

  Recovery of leftover work from a crash runs asynchronously after
  `init/1` returns, bounded by `recovery_retry_delay_ms` (config, default
  100ms) between batches.
  """

  use GenServer

  require Logger

  alias Logflare.Backends.Spool.Committer
  alias Logflare.Backends.Spool.Health

  @default_max_inflight_commits 10
  @default_recovery_retry_delay_ms 100

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))
  end

  @doc """
  Appends `segment` and blocks the caller until it's durable in the buffer
  (the default), or — with `wait_until_committed: true` — until the batch
  it ends up part of has actually been committed. `timeout` (default 15s)
  bounds the `GenServer.call` itself, not how long the commit may take.
  """
  @spec append(
          GenServer.server(),
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          timeout: timeout(),
          wait_until_committed: boolean()
        ) :: :ok | {:error, term()}
  def append(partition, segment, raw_byte_size, event_count, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 15_000)

    tag = if Keyword.get(opts, :wait_until_committed, false), do: :append_committed, else: :append
    GenServer.call(partition, {tag, segment, raw_byte_size, event_count}, timeout)
  end

  @impl GenServer
  def init(opts) do
    buffer_mod = Keyword.fetch!(opts, :buffer_mod)

    committer_config = %{
      bucket: Keyword.fetch!(opts, :bucket),
      storage_mod: Keyword.fetch!(opts, :storage_mod),
      queue_mod: Keyword.fetch!(opts, :queue_mod),
      queue_ref: Keyword.fetch!(opts, :queue_ref),
      format: Keyword.fetch!(opts, :format),
      compress: Keyword.fetch!(opts, :compress),
      compression_algorithm: Keyword.fetch!(opts, :compression_algorithm),
      index: Keyword.fetch!(opts, :index)
    }

    spool_config = Application.get_env(:logflare, :spool, [])

    state = %{
      buffer_mod: buffer_mod,
      buffer_state: nil,
      batch_timeout: Keyword.fetch!(opts, :batch_timeout),
      max_inflight_commits:
        Keyword.get(spool_config, :max_inflight_commits, @default_max_inflight_commits),
      recovery_retry_delay_ms:
        Keyword.get(spool_config, :recovery_retry_delay_ms, @default_recovery_retry_delay_ms),
      commit_ack_froms: [],
      sync_ack_froms: [],
      task_in_flight: 0,
      tasks: %{},
      committer_config: committer_config
    }

    {:ok, state, {:continue, {:init_buffer, opts}}}
  end

  # Deferred out of init/1 so this process's name is registered before the
  # buffer's own init/1 runs.
  @impl GenServer
  def handle_continue({:init_buffer, opts}, state) do
    state =
      %{state | buffer_state: state.buffer_mod.init(opts)}
      |> start_flush_loop()
      |> schedule_recovery()

    {:noreply, state}
  end

  @impl GenServer
  def handle_call({:append, segment, raw_byte_size, event_count}, from, state) do
    case do_append(state, segment, raw_byte_size, event_count) do
      {:ok, state} ->
        Enum.each(state.sync_ack_froms, &GenServer.reply(&1, :ok))
        {:reply, :ok, maybe_roll(%{state | sync_ack_froms: []}, :size)}

      {:pending, state} ->
        {:noreply, %{state | sync_ack_froms: [from | state.sync_ack_froms]}}

      {:error, reason, state} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:append_committed, segment, raw_byte_size, event_count}, from, state) do
    case do_append(state, segment, raw_byte_size, event_count) do
      {result, state} when result in [:ok, :pending] ->
        state = %{state | commit_ack_froms: [from | state.commit_ack_froms]}
        {:noreply, maybe_roll(state, :size)}

      {:error, reason, state} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_info(:flush, state) do
    state = state |> maybe_roll(:timeout, true) |> start_flush_loop()
    {:noreply, state}
  end

  def handle_info({:commit_success, tag}, state) do
    {:noreply, settle_commit(state, tag, :ok)}
  end

  def handle_info({:commit_failed, tag, reason}, state) do
    {:noreply, settle_commit(state, tag, {:error, reason})}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Enum.find(state.tasks, fn {_tag, {task_ref, _ctx, _froms}} -> task_ref == ref end) do
      nil ->
        {:noreply, state}

      {tag, _entry} ->
        Logger.error("spool_partition: commit task crashed: #{inspect(reason)}")
        {:noreply, settle_commit(state, tag, {:error, reason})}
    end
  end

  # Starts as many of `items` as capacity allows, rescheduling itself with
  # any remainder after recovery_retry_delay_ms.
  def handle_info({:recover, items}, state) do
    available = max(state.max_inflight_commits - state.task_in_flight, 0)
    {to_start_now, remaining} = Enum.split(items, available)

    state =
      Enum.reduce(to_start_now, state, fn {body_thunk, context, total_count}, state ->
        spawn_commit(state, body_thunk, context, [], total_count, :recovered)
      end)

    if remaining != [] do
      Process.send_after(self(), {:recover, remaining}, state.recovery_retry_delay_ms)
    end

    {:noreply, state}
  end

  # A commit deferred by start_commit_or_defer/6 for lack of a free slot.
  def handle_info({:retry_commit, body_thunk, context, froms, total_count, trigger}, state) do
    {:noreply, start_commit_or_defer(state, body_thunk, context, froms, total_count, trigger)}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp do_append(state, segment, raw_byte_size, event_count) do
    case state.buffer_mod.append(state.buffer_state, segment, raw_byte_size, event_count) do
      {:ok, buffer_state} -> {:ok, %{state | buffer_state: buffer_state}}
      {:pending, buffer_state} -> {:pending, %{state | buffer_state: buffer_state}}
      {:error, reason, buffer_state} -> {:error, reason, %{state | buffer_state: buffer_state}}
    end
  end

  defp start_flush_loop(state) do
    Process.send_after(self(), :flush, state.batch_timeout)
    state
  end

  # Rolling always happens regardless of commit-slot capacity; only
  # starting the upload is capacity-gated (see start_commit_or_defer/6).
  defp maybe_roll(state, trigger, force \\ false) do
    case state.buffer_mod.roll(state.buffer_state, force) do
      {:no_roll, buffer_state} ->
        %{state | buffer_state: buffer_state}

      {:ok, body_thunk, context, total_count, buffer_state} ->
        Enum.each(state.sync_ack_froms, &GenServer.reply(&1, :ok))
        froms = state.commit_ack_froms

        state = %{
          state
          | buffer_state: buffer_state,
            commit_ack_froms: [],
            sync_ack_froms: []
        }

        start_commit_or_defer(state, body_thunk, context, froms, total_count, trigger)

      {:error, _reason, buffer_state} ->
        %{state | buffer_state: buffer_state}
    end
  end

  defp start_commit_or_defer(state, body_thunk, context, froms, total_count, trigger) do
    if state.task_in_flight < state.max_inflight_commits do
      spawn_commit(state, body_thunk, context, froms, total_count, trigger)
    else
      Process.send_after(
        self(),
        {:retry_commit, body_thunk, context, froms, total_count, trigger},
        state.recovery_retry_delay_ms
      )

      state
    end
  end

  # Unlinked (a crashing commit must not take this partition down); monitored
  # instead, so a crash surfaces as a {:DOWN, ...} message.
  defp spawn_commit(state, body_thunk, context, froms, total_count, trigger) do
    tag = make_ref()

    {:ok, pid} =
      Committer.commit_async(
        self(),
        body_thunk,
        total_count,
        trigger,
        tag,
        state.committer_config
      )

    ref = Process.monitor(pid)

    %{
      state
      | tasks: Map.put(state.tasks, tag, {ref, context, froms}),
        task_in_flight: state.task_in_flight + 1
    }
  end

  defp settle_commit(state, tag, result) do
    {{_ref, context, froms}, state} = forget_task(state, tag)
    report_commit_health(result)
    buffer_state = state.buffer_mod.on_commit_result(state.buffer_state, context, result)
    Enum.each(froms, &GenServer.reply(&1, result))
    maybe_roll(%{state | buffer_state: buffer_state}, :pipeline)
  end

  defp report_commit_health(:ok), do: Health.report_recovery!()
  defp report_commit_health({:error, _reason}), do: Health.report_failure!()

  defp forget_task(state, tag) do
    {entry, tasks} = Map.pop(state.tasks, tag)
    if entry, do: Process.demonitor(elem(entry, 0), [:flush])
    {entry, %{state | tasks: tasks, task_in_flight: state.task_in_flight - 1}}
  end

  # Kicks off the self-rescheduling handle_info({:recover, ...}) loop above.
  defp schedule_recovery(state) do
    {items, buffer_state} = state.buffer_mod.recover(state.buffer_state)
    if items != [], do: send(self(), {:recover, items})
    %{state | buffer_state: buffer_state}
  end
end
