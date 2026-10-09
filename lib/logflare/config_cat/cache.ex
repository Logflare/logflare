defmodule Logflare.ConfigCatCache do
  @moduledoc """
  Cachex-backed cache for ConfigCat feature flag lookups.
  Used to avoid repeated ConfigCat calls on hot paths like LogEvent processing.
  """

  alias Logflare.Cache.CachexOps

  def child_spec(_) do
    CachexOps.child_spec(__MODULE__,
      limit: 100_000,
      ttl: to_timeout(minute: 10),
      purge_interval: to_timeout(minute: 1)
    )
  end

  @spec get(term()) :: {:ok, term()} | {:error, term()}
  def get(key), do: Cachex.get(__MODULE__, key)

  def fetch(key, fallback), do: Cachex.fetch(__MODULE__, key, fallback)

  @spec put(term(), term()) :: {:ok, true} | {:error, term()}
  def put(key, value), do: Cachex.put(__MODULE__, key, value)
end
