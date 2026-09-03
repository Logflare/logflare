defmodule Logflare.SystemCache do
  @moduledoc false

  import Cachex.Spec

  require Logger

  alias Logflare.Utils

  @cache __MODULE__

  def child_spec(_) do
    stats = Application.get_env(:logflare, :cache_stats, false)
    env = Application.get_env(:logflare, :env)

    warmers =
      if env == :test do
        []
      else
        [
          warmer(
            required: false,
            module: __MODULE__.Warmer,
            name: __MODULE__.Warmer,
            interval: :timer.seconds(3)
          )
        ]
      end

    %{
      id: __MODULE__,
      start:
        {Cachex, :start_link,
         [
           @cache,
           [
             warmers: warmers,
             hooks:
               [
                 if(stats, do: Utils.cache_stats()),
                 Utils.cache_limit(100)
               ]
               |> Enum.filter(& &1),
             expiration: Utils.cache_expiration_sec(5, 5)
           ]
         ]}
    }
  end

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
