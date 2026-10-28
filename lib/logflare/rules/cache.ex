defmodule Logflare.Rules.Cache do
  @moduledoc false

  use Logflare.ContextCache

  alias Logflare.Backends.Backend
  alias Logflare.Cache.CachexOps
  alias Logflare.ContextCache
  alias Logflare.Repo
  alias Logflare.Rules
  alias Logflare.Sources.Source
  alias Logflare.Sources.SourceRouter.RulesTree

  def child_spec(_) do
    CachexOps.child_spec(__MODULE__,
      limit: 100_000,
      ttl: to_timeout(hour: 1),
      purge_interval: to_timeout(minute: 5),
      warmer: Rules.CacheWarmer
    )
  end

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

  @spec rules_tree_by_source_id(pos_integer()) ::
          RulesTree.t() | {:error, :database_unavailable}
  def rules_tree_by_source_id(id) do
    key = {:rules_tree_by_source_id, [id]}
    loader = fn -> {:ignore, with_tree_lock(key, fn -> load_rules_tree(id, key) end)} end

    case Cachex.fetch!(__MODULE__, key, loader) do
      {:cached, tree} -> tree
      :loaded -> rules_tree_by_source_id(id)
      {:error, :database_unavailable} = error -> error
    end
  end

  @impl ContextCache
  @spec delete_keys(Enumerable.t()) :: {:ok, non_neg_integer()}
  def delete_keys(keys) do
    for key <- keys, reduce: {:ok, 0} do
      {:ok, total} ->
        {:ok, count} = delete_key(key)
        {:ok, total + count}
    end
  end

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

  @spec load_rules_tree(pos_integer(), tuple()) ::
          :loaded | {:error, :database_unavailable}
  defp load_rules_tree(id, key) do
    case Cachex.get!(__MODULE__, key) do
      {:cached, _tree} ->
        :loaded

      nil ->
        tree = Repo.with_primary(fn -> Rules.rules_tree_by_source_id(id) end)
        Cachex.put!(__MODULE__, key, {:cached, tree})
        :loaded
    end
  rescue
    _error in DBConnection.ConnectionError -> {:error, :database_unavailable}
  catch
    :exit, {reason, _call} when reason in [:noproc, :killed] ->
      {:error, :database_unavailable}
  end

  @spec delete_key(tuple()) :: {:ok, non_neg_integer()}
  defp delete_key({:rules_tree_by_source_id, [_id]} = key) do
    with_tree_lock(key, fn -> CachexOps.delete_keys(__MODULE__, [key]) end)
  end

  defp delete_key(key), do: CachexOps.delete_keys(__MODULE__, [key])

  @spec with_tree_lock(tuple(), (-> result)) :: result when result: term()
  defp with_tree_lock(key, fun),
    do: :global.trans({{__MODULE__, key}, self()}, fun, [node()])

  defp apply_repo_fun(fun, args) do
    Logflare.ContextCache.apply_fun(Rules, fun, args)
  end
end
