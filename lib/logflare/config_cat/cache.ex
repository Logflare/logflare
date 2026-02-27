defmodule Logflare.ConfigCatCache do
  @moduledoc """
  Cachex-backed cache for ConfigCat feature flag lookups.
  Used to avoid repeated ConfigCat calls on hot paths like LogEvent processing.
  """

  alias Logflare.ConfigCat.CacheWarmer
  alias Logflare.Utils

  import Cachex.Spec
  require Cachex.Spec

  def child_spec(_) do
    stats = Application.get_env(:logflare, :cache_stats, false)
    config_cat_key = Application.get_env(:logflare, :config_cat_sdk_key)

    warmers =
      if config_cat_key do
        [warmer(module: CacheWarmer, interval: :timer.minutes(2), required: true)]
      else
        []
      end

    %{
      id: __MODULE__,
      start:
        {Cachex, :start_link,
         [
           __MODULE__,
           [
             warmers: warmers,
             hooks:
               [
                 if(stats, do: Utils.cache_stats()),
                 Utils.cache_limit(100_000)
               ]
               |> Enum.filter(& &1),
             expiration: Utils.cache_expiration_min(15, 1)
           ]
         ]}
    }
  end

  @spec get(term()) :: {:ok, term()} | {:error, term()}
  def get(key, identifier \\ nil), do: Cachex.get(__MODULE__, key, identifier)

  def fetch(key, fallback), do: Cachex.fetch(__MODULE__, key, fallback)

  @spec put(term(), term()) :: {:ok, true} | {:error, term()}
  def put(key, value), do: Cachex.put(__MODULE__, key, value)
end
