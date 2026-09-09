defmodule Logflare.Backends.Spool.Partition do
  @moduledoc """
  Accumulates pre-compressed, pre-framed segments pushed directly by
  ingest callers via `append/5`, buffered however `buffer_mod` (a
  `Logflare.Backends.Spool.Buffer` — local-disk WAL or an in-memory batch,
  see `Logflare.Backends.spool_buffer/0`) decides — this module doesn't
  know or care which, and never touches a file or an in-memory list
  directly.

  What this module *does* own, uniformly regardless of buffer:

    * **Reply timing.** `append/5` replies the moment the buffer itself
      accepts the segment (`buffer_mod.append/4` succeeds) — for the WAL
      buffer that's a local `fsync`; for the Mem buffer, just landing in a
      list. `wait_until_committed: true` instead defers the reply until the
      batch this segment ends up part of is actually committed (uploaded to
      GCS/S3, published to Pub-Sub/SQS) — its `from` is stashed in
      `commit_ack_froms` and carried along with whatever roll eventually
      seals it in.
    * **When to attempt a roll.** After every append (an early roll if the
      buffer's own threshold is crossed) and on a recurring
      `batch_timeout` timer (`force: true` — rolls however little has
      accumulated, so nothing waits on a threshold that may never come).
    * **The commit `Task`'s lifecycle** — via
      `Logflare.Backends.Spool.Committer`, bounded by `max_inflight_commits`
      (config, default 10; only *starting* a commit is capacity-gated —
      rolling itself always happens, deferring only backs up the queue of
      not-yet-started commits, never lets a buffer grow unbounded past its
      own threshold) — and crash recovery (`{:DOWN, ...}`, always treated
      the same as the commit itself failing).

  A commit's `context` (whatever `buffer_mod.roll/2` returned, opaque to
  this module too, plus whichever `commit_ack_froms` had accumulated since
  the last roll) is tracked by a locally-generated `tag`, not the context
  itself — two different in-flight commits could otherwise share an
  identical context (e.g. the Mem buffer's context is always `nil`, and
  two batches with no `wait_until_committed: true` callers at all would
  have identical `commit_ack_froms`, too: `[]`).

  `init/1` must never block on `buffer_mod.recover/1`'s findings actually
  finishing — OTP registers this process's `:via` name before `init/1`
  runs, so a supervisor-driven restart can already have callers routed to
  it while recovery is still draining leftover work from a crash. So
  `init/1` just sends itself one `{:recover, items}` message and returns;
  `handle_info/2` starts as many as capacity allows and reschedules itself
  for the rest after `recovery_retry_delay_ms` (config, default 100ms).
  """

  use GenServer

  require Logger

  alias Logflare.Backends.Spool.Committer

  @default_max_inflight_commits 10
  # Deliberately decoupled from batch_timeout — recovery retrying is "check
  # again for a free slot", not "wait for a batch window", so it shouldn't
  # inherit batch_timeout's (config, up to a few seconds) cadence.
  @default_recovery_retry_delay_ms 100

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))
  end

  @doc """
  Appends `segment` and blocks the caller until either:

    * it's durable in the buffer (the default, `wait_until_committed: false`)
      — for the WAL buffer that's a local `fsync`; for the Mem buffer, just
      landing in a list — or
    * `wait_until_committed: true` — the batch this segment ends up part of
      has actually been committed: uploaded to GCS/S3 and published to
      Pub-Sub/SQS (see this module's doc). This can add real latency to the
      caller — see `Logflare.Backends.Spool.Committer`'s retry budget.

  `timeout` (default 15s) bounds the underlying `GenServer.call` itself, not
  how long the commit is allowed to take — see `max_commit_attempts`/
  `retry_delay_ms` for that.
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
      task_in_flight: 0,
      tasks: %{},
      committer_config: committer_config
    }

    {:ok, state, {:continue, {:init_buffer, opts}}}
  end

  # Deferred out of init/1 so this process's :via name is registered (and
  # `:sys`-inspectable) before the buffer's own init/1 runs — for the WAL
  # buffer that's a few local syscalls (mkdir_p!, recover!, file.open), not
  # network I/O, but OTP guarantees this runs before any other message
  # (including a caller's GenServer.call right after start_link returns),
  # so nothing that depends on buffer_state being set can ever race it.
  @impl GenServer
  def handle_continue({:init_buffer, opts}, state) do
    state =
      %{state | buffer_state: state.buffer_mod.init(opts)}
      |> start_flush_loop()
      |> schedule_recovery()

    {:noreply, state}
  end

  @impl GenServer
  def handle_call({:append, segment, raw_byte_size, event_count}, _from, state) do
    case do_append(state, segment, raw_byte_size, event_count) do
      {:ok, state} -> {:reply, :ok, maybe_roll(state, :size)}
      {:error, reason, state} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:append_committed, segment, raw_byte_size, event_count}, from, state) do
    case do_append(state, segment, raw_byte_size, event_count) do
      {:ok, state} ->
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

  # Bounded, self-rescheduling recovery loop, entirely decoupled from the
  # live append/commit flow above — see schedule_recovery/1 (started once,
  # from init/1) and this module's doc for why it can never block startup.
  # Starts as many of `items` as the current capacity allows, then — if any
  # are left over — reschedules itself with the remainder after
  # recovery_retry_delay_ms.
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

  # A single commit deferred by start_commit_or_defer/6 because no slot was
  # free at the time — retried here, one at a time, the moment this fires.
  def handle_info({:retry_commit, body_thunk, context, froms, total_count, trigger}, state) do
    {:noreply, start_commit_or_defer(state, body_thunk, context, froms, total_count, trigger)}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp do_append(state, segment, raw_byte_size, event_count) do
    case state.buffer_mod.append(state.buffer_state, segment, raw_byte_size, event_count) do
      {:ok, buffer_state} -> {:ok, %{state | buffer_state: buffer_state}}
      {:error, reason, buffer_state} -> {:error, reason, %{state | buffer_state: buffer_state}}
    end
  end

  defp start_flush_loop(state) do
    Process.send_after(self(), :flush, state.batch_timeout)
    state
  end

  # Rolling itself always happens, regardless of whether a commit slot is
  # currently free — see this module's doc for why. Only *starting* the
  # upload is capacity-gated; see start_commit_or_defer/6.
  defp maybe_roll(state, trigger, force \\ false) do
    case state.buffer_mod.roll(state.buffer_state, force) do
      {:no_roll, buffer_state} ->
        %{state | buffer_state: buffer_state}

      {:ok, body_thunk, context, total_count, buffer_state} ->
        froms = state.commit_ack_froms
        state = %{state | buffer_state: buffer_state, commit_ack_froms: []}
        start_commit_or_defer(state, body_thunk, context, froms, total_count, trigger)

      {:error, _reason, buffer_state} ->
        # The buffer already logged/reported this itself — pending work is
        # left as-is inside buffer_state, retried on the next append/tick,
        # no special-cased retry needed here.
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

  # The one place every commit — a normal roll or a recovered item —
  # actually starts. Unlinked (Committer.commit_async/6 uses Task.start/1)
  # so a crashing commit never takes this partition down with it; monitored
  # here instead, so a crash still surfaces as a message (handled above)
  # rather than silently stranding task_in_flight. `tag` (not `context`) is
  # what Committer hands back — see this module's doc for why context
  # itself can't safely double as the map key.
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
    buffer_state = state.buffer_mod.on_commit_result(state.buffer_state, context, result)
    Enum.each(froms, &GenServer.reply(&1, result))
    maybe_roll(%{state | buffer_state: buffer_state}, :pipeline)
  end

  defp forget_task(state, tag) do
    {entry, tasks} = Map.pop(state.tasks, tag)
    if entry, do: Process.demonitor(elem(entry, 0), [:flush])
    {entry, %{state | tasks: tasks, task_in_flight: state.task_in_flight - 1}}
  end

  # Leftover, already-rolled work from a crash — see schedule_recovery/1
  # (started once at boot, or on a supervisor-driven restart of just this
  # partition — see this module's doc for why init/1 must never block
  # here). Only kicks off the self-rescheduling handle_info({:recover, ...})
  # loop above — actually spawning commits happens entirely there, bounded
  # by capacity, so a crash that left behind arbitrarily many items can
  # never spike memory with one concurrent upload per leftover item.
  defp schedule_recovery(state) do
    {items, buffer_state} = state.buffer_mod.recover(state.buffer_state)
    if items != [], do: send(self(), {:recover, items})
    %{state | buffer_state: buffer_state}
  end
end
