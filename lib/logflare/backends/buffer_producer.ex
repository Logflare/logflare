defmodule Logflare.Backends.BufferProducer do
  @moduledoc """
  A GenStage producer that pulls from the given source-backend's IngestEventQueue ets table.

  Each source-backend combination has its own ets table.

  Length determination of the buffer is determined by using `:ets.info/2`, which is O(1)
  """
  use GenStage
  alias Logflare.Sources
  alias Logflare.Backends.IngestEventQueue.DemandWorker
  alias Logflare.Backends
  require Logger
  @default_interval 500

  def start_link(opts) when is_list(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  def init(opts) do
    state = %{
      demand: 0,
      # TODO: broadcast by id instead.
      source_id: opts[:source].id,
      source_token: opts[:source].token,
      backend_id: Map.get(opts[:backend] || %{}, :id),
      backend_token: Map.get(opts[:backend] || %{}, :token),
      # discard logging backoff
      last_discard_log_dt: nil,
      interval: Keyword.get(opts, :interval, @default_interval)
    }

    schedule(state.interval)
    {:producer, state, buffer_size: Keyword.get(opts, :buffer_size, 10_000)}
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

  def handle_info(:scheduled_resolve, state) do
    {items, state} = resolve_demand(state)
    schedule(state.interval)
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

  defp schedule(interval) do
    Process.send_after(self(), :scheduled_resolve, interval)
  end

  defp resolve_demand(
         %{demand: prev_demand} = state,
         new_demand \\ 0
       ) do
    total_demand = prev_demand + new_demand

    {:ok, events} =
      Backends.via_source(state.source_id, DemandWorker, state.backend_id)
      |> GenServer.call({:fetch, total_demand})

    {events, %{state | demand: total_demand - Enum.count(events)}}
  end
end
