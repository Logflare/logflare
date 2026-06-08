defmodule Logflare.SystemCache do
  @moduledoc false

  import Cachex.Spec

  alias Logflare.Utils

  @cache __MODULE__

  def child_spec(_) do
    stats = Application.get_env(:logflare, :cache_stats, false)
    env = Application.get_env(:logflare, :env)

    warmers =
      if env == :test do
        []
      else
        [warmer(required: true, module: __MODULE__.Warmer, name: __MODULE__.Warmer, interval: :timer.seconds(3))]
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
           {:commit, Logflare.System.memory_utilization()}
         end) do
      {:ok, value} -> value
      {:commit, value} -> value
      {:error, _} -> Logflare.System.memory_utilization()
    end
  end
end
