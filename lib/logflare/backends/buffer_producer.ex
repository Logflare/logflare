defmodule Logflare.Backends.BufferProducer do
  @moduledoc """
  A generic broadway producer that doesn't actually produce anything.

  Meant for push through Broadway.push_messages/2
  """
  use GenStage
  alias Logflare.Sources
  alias Logflare.Backends.BufferProducer
  alias Logflare.Backends.IngestEvents
  require Logger

  def start_link(opts) when is_list(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  def init(opts) do
    IngestEvents.upsert_tid(opts[:source])

    {:ok, worker_pid} = __MODULE__.DemandWorker.start_link(opts[:source])
    state =
      Enum.into(opts, %{
        demand: 0,
        # TODO: broadcast by id instead.
        source_id: opts[:source].id,
        source_token: nil,
        backend_token: nil,
        worker_pid: worker_pid,
        # discard logging backoff
        last_discard_log_dt: nil
      })

    # {:ok, _pid} = BufferProducer.Worker.start_link(state)

    # loop(state.active_broadcast_interval)
    {:producer, state, buffer_size: Keyword.get(opts, :buffer_size, 50_000)}
  end

  def format_discarded(discarded, state) do
    source = Sources.Cache.get_by_id(state.source_token)

    # backoff logic to prevent torrent of discards
    # defaults to at most 1 log per 5 second per producer
    should_log? =
      cond do
        state.last_discard_log_dt == nil -> true
        DateTime.diff(DateTime.utc_now(), state.last_discard_log_dt) > 5 -> true
        true -> false
      end

    if should_log? do
      Logger.warning(
        "GenStage producer for #{source.name} (#{source.token}) has discarded #{discarded} events from buffer",
        source_token: source.token,
        source_id: source.token,
        backend_token: state.backend_token
      )

      send(self(), {:update_state, %{state | last_discard_log_dt: DateTime.utc_now()}})
    end

    # don't do the default log
    false
  end

  def handle_info(:resolve, state) do
    {items, state} = resolve_demand(state)
    {:noreply, items, state}
  end

  def handle_info({:update_state, new_state}, _state) do
    {:noreply, [], new_state}
  end

  def handle_info({:add_to_buffer, items}, state) do
    {:noreply, items, state}
  end

  def handle_demand(demand, state) do
    {items, state} = resolve_demand(state, demand)
    {:noreply, items, state}
  end

  defp resolve_demand(
         %{demand: prev_demand} = state,
         new_demand \\ 0
       ) do
    total_demand = prev_demand + new_demand
    popped = GenServer.call(state.worker_pid, {:pop, total_demand})
    {popped, %{state | demand: total_demand - Enum.count(popped)}}
  end

  defmodule DemandWorker do
    use GenServer
  alias Logflare.Backends.IngestEvents

    def start_link(source) do
      GenServer.start_link(__MODULE__, source)
    end

    def init(source) do
      {:ok, source}
    end

    def handle_call({:pop, n}, _caller, state) do
      {:ok, popped} = IngestEvents.dirty_pop(state, n)
      {:reply, popped, state}
    end
  end
end
