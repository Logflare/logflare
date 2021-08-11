defmodule Logflare.SystemMetrics.Cachex.Poller do
  @moduledoc """
  Polls Cachex stats.
  """

  use GenServer
  use TypedStruct

  require Logger

  @poll_every 30_000
  @caches [
    Logflare.ContextCache,
    Logflare.Plans.Cache,
    Logflare.Users.Cache,
    Logflare.Sources.Cache,
    Logflare.BillingAccounts.Cache,
    Logflare.SourceSchemas.Cache,
    Logflare.PubSubRates.Cache
  ]

  typedstruct module: Stats do
    field :name, atom()
    field :calls, map()
    field :evictions, integer()
    field :expiration, integer()
    field :hit_rate, float()
    field :hits, integer()
    field :meta, map()
    field :miss_rate, float()
    field :misses, integer()
    field :operations, integer()
    field :writes, integer()
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(opts) do
    poll_stats()
    {:ok, opts}
  end

  def handle_info(:stats, state) do
    poll_stats()

    caches_stats =
      for c <- @caches do
        case Cachex.stats(c) do
          {:ok, stats} -> stats |> Map.put(:name, c)
          {:error, :stats_disabled} -> :stats_disabled
        end
      end
      |> Enum.filter(&is_map(&1))
      |> Enum.filter(&is_float(&1.hit_rate))
      |> Enum.filter(&is_float(&1.miss_rate))

    if Application.get_env(:logflare, :env) == :prod do
      Logger.info("Cachex stats!", cachex_stats: caches_stats)
    end

    {:noreply, state}
  end

  defp poll_stats() do
    Process.send_after(self(), :stats, @poll_every)
  end
end
