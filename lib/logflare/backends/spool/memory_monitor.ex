defmodule Logflare.Backends.Spool.MemoryMonitor do
  @moduledoc """
  Tracks system memory pressure and destination-backlog status for the
  spool producer and consumer. A GenServer refreshes a read-optimized ETS
  cache on a timer; hot-path readers just read the cache.
  """

  use GenServer

  alias Logflare.Backends

  @table __MODULE__
  @seen_sources_table __MODULE__.SeenSources
  @cache_key :stats
  @throttled_position 2
  @consumer_throttled_position 3
  @stats_position 4
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

  @doc "Whether the spool should be treated as under memory pressure right now."
  @spec throttled?() :: boolean()
  def throttled? do
    :ets.lookup_element(@table, @cache_key, @throttled_position)
  rescue
    ArgumentError -> compute_stats(MapSet.new()).throttled?
  end

  @doc "Whether any registered spool consumer source has a backed-up destination ingest buffer right now."
  @spec consumer_throttled?() :: boolean()
  def consumer_throttled? do
    :ets.lookup_element(@table, @cache_key, @consumer_throttled_position)
  rescue
    ArgumentError -> compute_stats(MapSet.new()).consumer_throttled?
  end

  @doc "The full stats map behind `throttled?/0` and `consumer_throttled?/0`."
  @spec stats() :: stats()
  def stats do
    :ets.lookup_element(@table, @cache_key, @stats_position)
  rescue
    ArgumentError -> compute_stats(MapSet.new())
  end

  @doc """
  Registers a source as currently active in the spool consumer, so the
  next refresh cycle checks its destination buffer for backlog. Safe to
  call unconditionally and concurrently — a plain atomic ETS write, no
  GenServer call/cast involved.
  """
  @spec register_source(pos_integer()) :: :ok
  def register_source(source_id) do
    :ets.insert_new(@seen_sources_table, {source_id})
    :ok
  rescue
    # Table doesn't exist yet — MemoryMonitor isn't started.
    ArgumentError -> :ok
  end

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl GenServer
  def init(_opts) do
    :ets.new(@table, [:named_table, :set, :protected, read_concurrency: true])

    :ets.new(@seen_sources_table, [
      :public,
      :named_table,
      :set,
      write_concurrency: true,
      read_concurrency: true
    ])

    {:ok, %{}, {:continue, :refresh}}
  end

  @impl GenServer
  def handle_continue(:refresh, state) do
    refresh()
    schedule_refresh()
    {:noreply, state}
  end

  @impl GenServer
  def handle_info(:refresh, state) do
    refresh()
    schedule_refresh()
    {:noreply, state}
  end

  defp refresh do
    registered_sources =
      Enum.map(:ets.tab2list(@seen_sources_table), fn {source_id} -> source_id end)

    stats = compute_stats(registered_sources)

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

    :ets.insert(
      @table,
      {@cache_key, stats.throttled?, stats.consumer_throttled?, stats}
    )
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
