defmodule Logflare.Backends.Spool.MemoryMonitor do
  @moduledoc """
  Periodically samples system memory pressure and publishes cheap-to-read
  stats for both the spool producer's batch splitter and the spool
  consumer's queue producer. Also tracks the set of sources the spool
  consumer has ever seen and reports whether any of their destination ingest
  buffers are backed up, so the consumer can pause (`consumer_throttled?/0`)
  instead of piling more events into an already-overflowing queue. Sources
  stay watched permanently once registered — no TTL/expiry — until they no
  longer resolve to a real source.

  Mirrors `Logflare.LogEvent.DayBucket`'s pattern: a GenServer refreshes a
  `:persistent_term` on a timer, so hot-path readers pay only the cost of a
  `:persistent_term.get/1` (no GC, no locking) rather than repeating the
  underlying `:erlang.memory/1` + `:memsup` computation themselves.

  Started once, shared by both sides — see `Logflare.Backends.Supervisor`.
  """

  use GenServer

  alias Logflare.Backends

  @pt_key {__MODULE__, :stats}
  @refresh_interval 1_000
  @default_memory_limit_percent 0.70
  @default_max_ets_percent 0.25

  @type stats :: %{
          throttled?: boolean(),
          total_percent: float(),
          total_limit_percent: float(),
          ets_percent: float(),
          ets_limit_percent: float(),
          consumer_throttled?: boolean()
        }

  @doc """
  Returns whether the spool should be treated as under memory pressure
  right now. Reads a cached value refreshed roughly every second; falls
  back to a live computation if the cache hasn't been seeded yet (e.g. a
  read racing this GenServer's own boot).
  """
  @spec throttled?() :: boolean()
  def throttled?, do: stats().throttled?

  @doc """
  Returns whether any registered spool consumer source has a backed-up
  destination ingest buffer right now. Same caching/fallback behavior as
  `throttled?/0`.
  """
  @spec consumer_throttled?() :: boolean()
  def consumer_throttled?, do: stats().consumer_throttled?

  @doc """
  Returns the full stats map behind `throttled?/0` and `consumer_throttled?/0`
  — the raw ratios and configured limits, for diagnostic logging. Same
  caching/fallback behavior as `throttled?/0`.
  """
  @spec stats() :: stats()
  def stats do
    :persistent_term.get(@pt_key)
  rescue
    ArgumentError -> compute_stats(MapSet.new())
  end

  @doc """
  Registers a source as currently active in the spool consumer, so the next
  refresh cycle checks its destination buffer for backlog. Cheap/async and
  idempotent — registering an already-registered source is a no-op. Stays
  watched permanently (no expiry) until it no longer resolves to a real
  source. Callers that see the same sources repeatedly (e.g. `QueueProducer`)
  should track what they've already sent and skip redundant casts rather
  than registering on every single record.
  """
  @spec register_source(pos_integer()) :: :ok
  def register_source(source_id) do
    GenServer.cast(__MODULE__, {:register_source, source_id})
  end

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl GenServer
  def init(_opts) do
    {:ok, %{registered_sources: MapSet.new()}, {:continue, :refresh}}
  end

  @impl GenServer
  def handle_continue(:refresh, state) do
    refresh(state)
    schedule_refresh()
    {:noreply, state}
  end

  @impl GenServer
  def handle_info(:refresh, state) do
    refresh(state)
    schedule_refresh()
    {:noreply, state}
  end

  @impl GenServer
  def handle_cast({:register_source, source_id}, state) do
    {:noreply, %{state | registered_sources: MapSet.put(state.registered_sources, source_id)}}
  end

  defp refresh(state) do
    stats = compute_stats(state.registered_sources)

    :telemetry.execute(
      [:logflare, :backends, :spool, :throttled],
      %{
        throttled: if(stats.throttled?, do: 1, else: 0),
        total_percent: stats.total_percent,
        ets_percent: stats.ets_percent,
        consumer_throttled: if(stats.consumer_throttled?, do: 1, else: 0)
      },
      %{}
    )

    :persistent_term.put(@pt_key, stats)
  end

  defp compute_stats(registered_sources) do
    spool_config = Application.get_env(:logflare, :spool, [])

    memory_limit_percent =
      Keyword.get(spool_config, :spool_memory_limit_percent, @default_memory_limit_percent)

    ets_limit_percent =
      Keyword.get(spool_config, :spool_max_ets_percent, @default_max_ets_percent)

    consumer_throttled? = any_source_backlogged?(registered_sources)

    case Logflare.System.total_memory_bytes() do
      nil ->
        %{
          throttled?: false,
          total_percent: 0.0,
          total_limit_percent: memory_limit_percent,
          ets_percent: 0.0,
          ets_limit_percent: ets_limit_percent,
          consumer_throttled?: consumer_throttled?
        }

      total ->
        total_ratio = :erlang.memory(:total) / total
        ets_ratio = :erlang.memory(:ets) / total

        %{
          throttled?: total_ratio >= memory_limit_percent or ets_ratio >= ets_limit_percent,
          total_percent: total_ratio,
          total_limit_percent: memory_limit_percent,
          ets_percent: ets_ratio,
          ets_limit_percent: ets_limit_percent,
          consumer_throttled?: consumer_throttled?
        }
    end
  end

  defp any_source_backlogged?(registered_sources) do
    Enum.any?(registered_sources, &Backends.any_ingest_queue_over_limit?/1)
  end

  defp schedule_refresh do
    Process.send_after(self(), :refresh, @refresh_interval)
  end
end
