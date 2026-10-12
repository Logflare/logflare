defmodule Logflare.Rules.Cache do
  @moduledoc false

  @behaviour Logflare.Cache
  @behaviour Logflare.ContextCache

  alias Logflare.Backends.Backend
  alias Logflare.Cache.CachexOps
  alias Logflare.ContextCache
  alias Logflare.Repo
  alias Logflare.Rules
  alias Logflare.Sources.Source

  def child_spec(_) do
    CachexOps.child_spec(__MODULE__,
      limit: 100_000,
      ttl: to_timeout(hour: 1),
      purge_interval: to_timeout(minute: 5),
      warmer: Rules.CacheWarmer
    )
  end

  @impl Logflare.Cache
  def healthy?, do: CachexOps.healthy?(__MODULE__)

  @impl Logflare.Cache
  def stats, do: CachexOps.stats(__MODULE__)

  @impl Logflare.Cache
  def reset, do: CachexOps.reset(__MODULE__)

  @impl ContextCache
  def fetch(key, getter), do: CachexOps.fetch(__MODULE__, key, getter)

  @impl ContextCache
  def update(key, value), do: CachexOps.update(__MODULE__, key, value)

  @impl ContextCache
  def delete_keys(keys), do: CachexOps.delete_keys(__MODULE__, keys)

  @spec list_rules(Source.t() | Backend.t()) :: [Rules.Rule.t()]
  def list_rules(%Source{id: source_id}), do: list_by_source_id(source_id)
  def list_rules(%Backend{id: backend_id}), do: list_by_backend_id(backend_id)

  def get_rule(id), do: apply_repo_fun(__ENV__.function, [id])

  def get_rules(ids) do
    Cachex.execute!(__MODULE__, fn cache ->
      Enum.map(ids, &fetch_rule(cache, &1))
    end)
  end

  def list_by_source_id(id), do: apply_repo_fun(__ENV__.function, [id])
  def list_by_backend_id(id), do: apply_repo_fun(__ENV__.function, [id])

  def rules_tree_by_source_id(id), do: apply_repo_fun(__ENV__.function, [id])

  @impl ContextCache
  def keys_to_bust(kw) do
    Enum.flat_map(kw, fn
      {:id, id} ->
        [{:get_rule, [id]}]

      {:source_id, source_id} ->
        [{:list_by_source_id, [source_id]}, {:rules_tree_by_source_id, [source_id]}]

      {:backend_id, backend_id} ->
        [{:list_by_backend_id, [backend_id]}]
    end)
  end

  defp fetch_rule(cache, id) do
    CachexOps.fetch(cache, {:get_rule, [id]}, fn ->
      Repo.with_replica(fn -> Rules.get_rule(id) end)
    end)
  end

  defp apply_repo_fun(fun, args) do
    Logflare.ContextCache.apply_fun(Rules, fun, args)
  end
end
