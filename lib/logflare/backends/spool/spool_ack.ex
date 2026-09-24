defmodule Logflare.Backends.Spool.SpoolAck do
  @moduledoc """
  Tracks, per spool queue message (`handle` — an SQS receipt handle or PubSub
  ack id), how many pipeline completions are still outstanding before that
  message is safe to ack.

  `register/3` creates the row (`QueueProducer` calls this once, the moment
  it obtains a handle) with the queue details needed to actually perform the
  ack later, and a count of 0. `bump/2` and `ack/2` are plain, fast ETS
  counter updates — no GenServer round-trip — safe to call from any process,
  any number of times, in any order, as long as the total nets out to zero
  only once every real unit of pending work for that handle is done:

    * `bump/2` — call once for every unit of work that now needs its own
      later `ack/2` call. `Backends.dispatch_from_spool/3`'s caller bumps
      once per handle per `handle_batch` call (a cushion so the counter can
      never cross zero while that call's own synchronous fan-out is still
      inserting pointers), and `IngestEventQueue` bumps once more for every
      pointer row that actually survives insertion.
    * `ack/2` — call once for every one of those units finishing, whether
      that's a pipeline's own success/terminal-failure call site, the
      matching release of a `dispatch_from_spool` cushion once it returns,
      or `IngestEventQueue` claiming a pointer whose body has already aged
      out (nothing downstream will ever see that one, so it has to resolve
      right there or it never would).

  A `nil` handle (an event that never came from the spool) is a no-op for
  both — most ingest doesn't go through the spool at all, and this module
  has nothing to track for it.

  The moment a decrement brings a handle's count to zero, this module casts
  itself to actually perform the queue ack (`queue_mod.ack/2`) — off the
  caller's own path, so a hot pipeline-completion call site never blocks on
  the network call. The row is removed from the counter table when the
  perform-ack decision is made, whether the count is at zero right then or
  was found already gone (defensive; should not happen under normal use).
  """

  use GenServer

  require Logger

  @table :spool_ack

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl GenServer
  def init(_opts) do
    :ets.new(@table, [:public, :named_table, :set, write_concurrency: true])
    {:ok, %{}}
  end

  @doc """
  Creates the counter row for `handle`, starting at 0. Call this exactly
  once, when `handle` is first obtained — before anything can legitimately
  call `bump/2` or `ack/2` for it.
  """
  @spec register(term(), module(), String.t()) :: :ok
  def register(handle, queue_mod, queue_url) when not is_nil(handle) do
    :ets.insert(@table, {handle, 0, queue_mod, queue_url})
    :ok
  end

  @doc "Increments `handle`'s outstanding count by `n`. No-op for a nil handle."
  @spec bump(term(), pos_integer()) :: :ok
  def bump(nil, _n), do: :ok

  def bump(handle, n) when is_integer(n) and n > 0 do
    :ets.update_counter(@table, handle, {2, n})
    :ok
  rescue
    ArgumentError ->
      Logger.warning("SpoolAck: bump/2 against a missing handle row", handle: inspect(handle))
      :ok
  end

  @doc """
  Decrements `handle`'s outstanding count by `n`. No-op for a nil handle.
  Triggers the real queue ack once the count reaches zero.
  """
  @spec ack(term(), pos_integer()) :: :ok
  def ack(nil, _n), do: :ok

  def ack(handle, n) when is_integer(n) and n > 0 do
    case :ets.update_counter(@table, handle, {2, -n}) do
      count when count <= 0 -> perform_ack(handle)
      _ -> :ok
    end
  rescue
    ArgumentError ->
      Logger.warning("SpoolAck: ack/2 against a missing handle row", handle: inspect(handle))
      :ok
  end

  defp perform_ack(handle) do
    case :ets.take(@table, handle) do
      [{^handle, _count, queue_mod, queue_url}] ->
        GenServer.cast(__MODULE__, {:perform_ack, queue_mod, queue_url, handle})

      [] ->
        :ok
    end
  end

  @impl GenServer
  def handle_cast({:perform_ack, queue_mod, queue_url, handle}, state) do
    result = queue_mod.ack(queue_url, handle)

    :telemetry.execute([:logflare, :backends, :spool, :queue, :ack], %{}, %{
      reason: :events_processed,
      result: if(result == :ok, do: :ok, else: :error)
    })

    {:noreply, state}
  end
end
