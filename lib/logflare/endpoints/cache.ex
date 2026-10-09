defmodule Logflare.Endpoints.Cache do
  @moduledoc """
  Cachex for Endpoints context.
  """

  @behaviour Logflare.Cache
  @behaviour Logflare.ContextCache

  alias Logflare.Cache.CachexOps
  alias Logflare.Endpoints

  def child_spec(_) do
    CachexOps.child_spec(__MODULE__,
      limit: 100_000,
      ttl: to_timeout(minute: 2),
      purge_interval: to_timeout(minute: 1)
    )
  end

  @impl Logflare.Cache
  def healthy?, do: CachexOps.healthy?(__MODULE__)

  @impl Logflare.Cache
  def stats, do: CachexOps.stats(__MODULE__)

  @impl Logflare.Cache
  def reset, do: CachexOps.reset(__MODULE__)

  @impl Logflare.ContextCache
  def fetch(key, getter), do: CachexOps.fetch(__MODULE__, key, getter)

  @impl Logflare.ContextCache
  def update(key, value), do: CachexOps.update(__MODULE__, key, value)

  @impl Logflare.ContextCache
  def keys_to_bust(kw), do: CachexOps.keys_to_bust(__MODULE__, kw)

  @impl Logflare.ContextCache
  def delete_keys(keys), do: CachexOps.delete_keys(__MODULE__, keys)

  def get_endpoint_query(kw), do: apply_repo_fun(:get_endpoint_query, [kw])

  def get_endpoint_query_at_version(query_id, version_number),
    do: apply_repo_fun(:get_endpoint_query_at_version, [query_id, version_number])

  def get_by(kw), do: apply_repo_fun(:get_by, [kw])
  def get_mapped_query_by_token(token), do: apply_repo_fun(:get_mapped_query_by_token, [token])

  defp apply_repo_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(Endpoints, arg1, arg2)
  end
end
