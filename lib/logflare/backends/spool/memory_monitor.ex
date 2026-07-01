defmodule Logflare.Backends.Spool.MemoryMonitor do
  @moduledoc """
  Periodically samples system memory pressure and publishes cheap-to-read
  stats for both the spool producer's batch splitter and the spool
  consumer's queue producer.

  Mirrors `Logflare.LogEvent.DayBucket`'s pattern: a GenServer refreshes a
  `:persistent_term` on a timer, so hot-path readers pay only the cost of a
  `:persistent_term.get/1` (no GC, no locking) rather than repeating the
  underlying `:erlang.memory/1` + `:memsup` computation themselves.

  Started once, shared by both sides — see `Logflare.Backends.Supervisor`.
  """

  use GenServer

  @pt_key {__MODULE__, :stats}
  @refresh_interval 1_000
  @default_memory_limit_percent 0.70
  @default_max_ets_percent 0.25

  @type stats :: %{
          throttled?: boolean(),
          total_percent: float(),
          total_limit_percent: float(),
          ets_percent: float(),
          ets_limit_percent: float()
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
  Returns the full stats map behind `throttled?/0` — the raw ratios and
  configured limits, for diagnostic logging. Same caching/fallback
  behavior as `throttled?/0`.
  """
  @spec stats() :: stats()
  def stats do
    :persistent_term.get(@pt_key)
  rescue
    ArgumentError -> compute_stats()
  end

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl GenServer
  def init(_opts) do
    {:ok, nil, {:continue, :refresh}}
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
    :persistent_term.put(@pt_key, compute_stats())
  end

  defp compute_stats do
    spool_config = Application.get_env(:logflare, :spool, [])

    memory_limit_percent =
      Keyword.get(spool_config, :spool_memory_limit_percent, @default_memory_limit_percent)

    ets_limit_percent =
      Keyword.get(spool_config, :spool_max_ets_percent, @default_max_ets_percent)

    case Logflare.System.total_memory_bytes() do
      nil ->
        %{
          throttled?: false,
          total_percent: 0.0,
          total_limit_percent: memory_limit_percent,
          ets_percent: 0.0,
          ets_limit_percent: ets_limit_percent
        }

      total ->
        total_ratio = :erlang.memory(:total) / total
        ets_ratio = :erlang.memory(:ets) / total

        %{
          throttled?: total_ratio >= memory_limit_percent or ets_ratio >= ets_limit_percent,
          total_percent: total_ratio,
          total_limit_percent: memory_limit_percent,
          ets_percent: ets_ratio,
          ets_limit_percent: ets_limit_percent
        }
    end
  end

  defp schedule_refresh do
    Process.send_after(self(), :refresh, @refresh_interval)
  end
end
