defmodule Logflare.Backends.Spool.ChunkProducer do
  @moduledoc """
  GenStage producer that pulls whole chunks from
  `Logflare.Backends.Spool.EventQueue`.

  Unlike `Logflare.Backends.BufferProducer`, chunks aren't owned by a
  particular producer pid and there's no per-instance queue table to seed or
  migrate on restart — `EventQueue` is one global table, so whatever chunks
  are still queued are simply there for the next poll, by this producer or
  its replacement.

  `max_in_flight` (in events, matching `BufferProducer`'s convention) is a
  safety valve, not exact backpressure: since a chunk can't be split, a
  single very large chunk can push the in-flight count over the limit — this
  only stops *new* chunks from being claimed once already over budget.
  """

  use GenStage

  require Logger

  alias Logflare.Backends.BufferProducer
  alias Logflare.Backends.Spool.EventQueue

  @default_interval 1_000
  # See BufferProducer's identical constant: how soon to retry after a fetch
  # came back short specifically because the in-flight cap throttled it.
  @min_in_flight_retry_ms 100

  @type state :: %{
          demand: non_neg_integer(),
          interval: pos_integer(),
          in_flight_ref: :atomics.atomics_ref(),
          max_in_flight: non_neg_integer() | :infinity,
          timer_ref: reference() | nil
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  @impl GenStage
  def init(opts) do
    Process.flag(:trap_exit, true)

    ref = :atomics.new(1, signed: true)
    Registry.register(BufferProducer.InFlightRegistry, self(), ref)

    state = %{
      demand: 0,
      interval: Keyword.get(opts, :interval, @default_interval),
      in_flight_ref: ref,
      max_in_flight: Keyword.get(opts, :max_in_flight) || :infinity,
      timer_ref: nil
    }

    {:producer, reschedule(state, false), buffer_size: Keyword.get(opts, :buffer_size, 10_000)}
  end

  @impl GenStage
  def format_discarded(discarded, _state) do
    Logger.warning("Spool chunk producer has discarded #{discarded} chunks from buffer")
    false
  end

  @impl GenStage
  def handle_demand(demand, state) do
    {chunks, state, capped?} = resolve_demand(state, demand)
    {:noreply, chunks, reschedule(state, capped?)}
  end

  @impl GenStage
  def handle_info(:scheduled_resolve, state) do
    {chunks, state, capped?} = resolve_demand(state)
    {:noreply, chunks, reschedule(state, capped?)}
  end

  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, [], state}
  def handle_info(_message, state), do: {:noreply, [], state}

  @spec resolve_demand(state(), non_neg_integer()) ::
          {[EventQueue.Chunk.t()], state(), boolean()}
  defp resolve_demand(%{demand: prev_demand} = state, new_demand \\ 0) do
    total_demand = prev_demand + new_demand
    {chunks, capped?} = fetch(state, total_demand)

    event_count = Enum.reduce(chunks, 0, fn chunk, acc -> acc + chunk.event_count end)
    if event_count > 0, do: :atomics.add(state.in_flight_ref, 1, event_count)

    remaining_demand = max(total_demand - length(chunks), 0)
    {chunks, %{state | demand: remaining_demand}, capped?}
  end

  # Once already at/over the in-flight cap, stop claiming new chunks entirely
  # rather than trying to fit "however many events are left in the budget" —
  # a chunk can't be split, so there's no finer-grained throttle available.
  @spec fetch(state(), non_neg_integer()) :: {[EventQueue.Chunk.t()], boolean()}
  defp fetch(_state, 0), do: {[], false}

  defp fetch(%{max_in_flight: :infinity}, total_demand) do
    {EventQueue.pop(total_demand), false}
  end

  defp fetch(%{in_flight_ref: ref, max_in_flight: max_in_flight}, total_demand) do
    if :atomics.get(ref, 1) >= max_in_flight do
      {[], true}
    else
      chunks = EventQueue.pop(total_demand)
      {chunks, length(chunks) < total_demand}
    end
  end

  @spec reschedule(state(), boolean()) :: state()
  defp reschedule(state, capped?) do
    if ref = state.timer_ref, do: Process.cancel_timer(ref)
    delay = if capped?, do: @min_in_flight_retry_ms, else: state.interval
    %{state | timer_ref: Process.send_after(self(), :scheduled_resolve, delay)}
  end
end
