defmodule Logflare.Backends.Spool.SpoolAck do
  @moduledoc """
  Tracks, per spool queue message (`handle` — an SQS receipt handle or
  PubSub ack id), how many pipeline completions are still outstanding
  before that message can be acked.

  `register/3` creates the row at count 0. `bump/2` and `ack/2` are plain
  ETS counter updates, safe to call from any process, any number of times,
  in any order. A `nil` handle is a no-op for both. Once a decrement
  brings the count to zero or below, the real queue ack (`queue_mod.ack/2`)
  is performed asynchronously and the row removed.

  Some terminal outcomes (e.g. a pointer whose generation was rotated out
  before a retry could resolve it) are intentionally never acked here —
  the message redelivers instead, giving it a fresh attempt from the
  durable spool file. That leaves the old handle's row orphaned, so a
  periodic sweep (`sweep_stale/1`) reclaims any row older than
  `@stale_after_ms`. Set `config :logflare, #{inspect(__MODULE__)}, ack_stale: true`
  to also ack those rows instead of just discarding them.
  """

  use GenServer

  import Logflare.Utils.Guards, only: [is_pos_integer: 1]

  @table :spool_ack
  @sweep_interval_ms :timer.minutes(1)
  @stale_after_ms :timer.minutes(10)

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl GenServer
  def init(_opts) do
    :ets.new(@table, [:public, :named_table, :set, write_concurrency: true])
    schedule_sweep()
    {:ok, %{}}
  end

  @doc """
  Creates the counter row for `handle`, starting at 0. Call this exactly
  once, when `handle` is first obtained — before anything can legitimately
  call `bump/2` or `ack/2` for it.
  """
  @spec register(term(), module(), String.t()) :: :ok
  def register(handle, queue_mod, queue_url) when not is_nil(handle) do
    :ets.insert(@table, {handle, 0, queue_mod, queue_url, System.monotonic_time(:millisecond)})
    :ok
  end

  @doc "Increments `handle`'s outstanding count by `n`. No-op for a nil handle."
  @spec bump(term(), pos_integer()) :: :ok
  def bump(nil, _n), do: :ok

  def bump(handle, n) when is_pos_integer(n) do
    :ets.update_counter(@table, handle, {2, n})
    :ok
  rescue
    ArgumentError ->
      emit_missing_handle_telemetry(:bump)
      :ok
  end

  @doc """
  Decrements `handle`'s outstanding count by `n`. No-op for a nil handle.
  Triggers the real queue ack once the count reaches zero.
  """
  @spec ack(term(), pos_integer()) :: :ok
  def ack(nil, _n), do: :ok

  def ack(handle, n) when is_pos_integer(n) do
    case :ets.update_counter(@table, handle, {2, -n}) do
      count when count <= 0 -> perform_ack(handle)
      _ -> :ok
    end
  rescue
    ArgumentError ->
      emit_missing_handle_telemetry(:ack)
      :ok
  end

  defp emit_missing_handle_telemetry(op) do
    :telemetry.execute([:logflare, :backends, :spool, :ack, :missing_handle], %{}, %{op: op})
  end

  defp perform_ack(handle) do
    case :ets.take(@table, handle) do
      [{^handle, _count, queue_mod, queue_url, _registered_at}] ->
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

  @impl GenServer
  def handle_info(:sweep, state) do
    sweep_stale(@stale_after_ms)
    schedule_sweep()
    {:noreply, state}
  end

  defp schedule_sweep, do: Process.send_after(self(), :sweep, @sweep_interval_ms)

  @doc false
  @spec sweep_stale(non_neg_integer()) :: :ok
  def sweep_stale(stale_after_ms) do
    cutoff = System.monotonic_time(:millisecond) - stale_after_ms
    match_spec = [{{:"$1", :_, :_, :_, :"$2"}, [{:<, :"$2", cutoff}], [:"$1"]}]

    deleted =
      @table
      |> :ets.select(match_spec)
      |> Enum.count(&reclaim_stale/1)

    if deleted > 0 do
      :telemetry.execute(
        [:logflare, :backends, :spool, :ack, :swept_stale],
        %{count: deleted},
        %{}
      )
    end

    :ok
  end

  defp reclaim_stale(handle) do
    case :ets.take(@table, handle) do
      [{^handle, _count, queue_mod, queue_url, _registered_at}] ->
        if ack_stale?(),
          do: GenServer.cast(__MODULE__, {:perform_ack, queue_mod, queue_url, handle})

        true

      [] ->
        false
    end
  end

  defp ack_stale? do
    Application.get_env(:logflare, __MODULE__, [])[:ack_stale] || false
  end
end
