defmodule Logflare.SystemCache do
  @moduledoc false

  @behaviour Logflare.Cache

  require Logger

  alias Logflare.Cache.CachexOps

  @cache __MODULE__

  def child_spec(_) do
    warmer =
      if Application.get_env(:logflare, :env) != :test do
        {__MODULE__.Warmer, interval: :timer.seconds(3)}
      end

    CachexOps.child_spec(@cache,
      limit: 100,
      ttl: to_timeout(second: 5),
      purge_interval: to_timeout(second: 5),
      warmer: warmer
    )
  end

  @impl Logflare.Cache
  def healthy?, do: CachexOps.healthy?(__MODULE__)

  @impl Logflare.Cache
  def stats, do: CachexOps.stats(__MODULE__)

  @impl Logflare.Cache
  def reset, do: CachexOps.reset(__MODULE__)

  @spec memory_utilization() :: float()
  def memory_utilization do
    case Cachex.fetch(@cache, :memory_utilization, fn _ ->
           {:commit, read_memory_utilization()}
         end) do
      {:ok, value} ->
        value

      {:commit, value} ->
        value

      {:error, err} ->
        Logger.warning("SystemCache.memory_utilization cache error: #{inspect(err)}")
        read_memory_utilization()
    end
  end

  defp read_memory_utilization do
    Logflare.System.memory_utilization()
  rescue
    error ->
      Logger.warning("SystemCache.memory_utilization read failed: #{Exception.message(error)}")
      0.0
  catch
    kind, reason ->
      Logger.warning(
        "SystemCache.memory_utilization read failed: #{Exception.format(kind, reason, __STACKTRACE__)}"
      )

      0.0
  end
end
