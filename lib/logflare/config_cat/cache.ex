defmodule Logflare.ConfigCatCache do
  @moduledoc """
  Cachex-backed cache for ConfigCat feature flag lookups.
  Used to avoid repeated ConfigCat calls on hot paths like LogEvent processing.
  """

  alias Logflare.Utils

  require Cachex.Spec

  def child_spec(_) do
    stats = Application.get_env(:logflare, :cache_stats, false)

    %{
      id: __MODULE__,
      start:
        {Cachex, :start_link,
         [
           __MODULE__,
           [
             hooks:
               [
                 if(stats, do: Utils.cache_stats()),
                 Utils.cache_limit(100_000)
               ]
               |> Enum.filter(& &1),
             expiration: Utils.cache_expiration_min(10, 1)
           ]
         ]}
    }
  end

  @spec get(term()) :: {:ok, term()} | {:error, term()}
  def get(key), do: Cachex.get(__MODULE__, key)

  def fetch(key, fallback), do: Cachex.fetch(__MODULE__, key, fallback)

  @spec put(term(), term()) :: {:ok, true} | {:error, term()}
  def put(key, value), do: Cachex.put(__MODULE__, key, value)
end
