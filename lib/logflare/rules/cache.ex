defmodule Logflare.Rules.Cache do
  @moduledoc false

  alias Logflare.Backends.Backend
  alias Logflare.ContextCache
  alias Logflare.Rules
  alias Logflare.Sources.Source
  alias Logflare.Utils
  import Cachex.Spec

  @behaviour ContextCache

  def child_spec(_) do
    stats = Application.get_env(:logflare, :cache_stats, false)

    %{
      id: __MODULE__,
      start: {
        Cachex,
        :start_link,
        [
          __MODULE__,
          [
            warmers: [
              warmer(required: false, module: Rules.CacheWarmer, name: Rules.CacheWarmer)
            ],
            hooks:
              [
                if(stats, do: Utils.cache_stats()),
                Utils.cache_limit(100_000)
              ]
              |> Enum.filter(& &1),
            expiration: Utils.cache_expiration_min(60, 5)
          ]
        ]
      }
    }
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

  @spec rules_tree_by_source_id(integer()) ::
          {Logflare.Sources.SourceRouter.RulesTree.t(), Rules.RoutingSnapshot.t()}
  def rules_tree_by_source_id(id) do
    ContextCache.fetch(__MODULE__, {:rules_tree_by_source_id, [id]}, fn ->
      {tree, targets} =
        Logflare.Repo.apply_with_replica(Rules, :rules_tree_by_source_id, [id])

      snapshot =
        Rules.RoutingSnapshot.new(id, targets, extra_estimated_bytes: :erlang.external_size(tree))

      {tree, snapshot}
    end)
  end

  @doc false
  @spec repair_routing_snapshot(integer(), Rules.RoutingSnapshot.t(), tuple()) ::
          {:repaired, Rules.RoutingSnapshot.t()} | :stale | {:error, term()}
  def repair_routing_snapshot(source_id, %Rules.RoutingSnapshot{} = snapshot, targets) do
    cache_key = {:rules_tree_by_source_id, [source_id]}

    try do
      case Cachex.transaction(__MODULE__, [cache_key], fn cache ->
             repair_routing_snapshot(cache, source_id, snapshot, targets)
           end) do
        {:ok, result} -> result
        {:error, reason} -> {:error, reason}
      end
    catch
      :exit, reason -> {:error, reason}
    end
  end

  defp repair_routing_snapshot(cache, source_id, snapshot, targets) do
    case Cachex.get(cache, {:rules_tree_by_source_id, [source_id]}) do
      {:ok, {:cached, {tree, %Rules.RoutingSnapshot{key: key}}}}
      when key == snapshot.key ->
        replacement = Rules.RoutingSnapshot.rehydrate(snapshot, source_id, targets)

        {:ok, true} =
          Cachex.put(
            cache,
            {:rules_tree_by_source_id, [source_id]},
            {:cached, {tree, replacement}}
          )

        {:repaired, replacement}

      _ ->
        :stale
    end
  catch
    :exit, reason -> {:error, reason}
  end

  @doc false
  @spec delete_routing_snapshot({integer(), reference()}) :: :deleted | :stale
  def delete_routing_snapshot({source_id, _generation} = snapshot_key) do
    cache_key = {:rules_tree_by_source_id, [source_id]}

    case Cachex.transaction(__MODULE__, [cache_key], fn cache ->
           delete_routing_snapshot(cache, cache_key, snapshot_key)
         end) do
      {:ok, result} -> result
      _ -> :stale
    end
  end

  defp delete_routing_snapshot(cache, cache_key, snapshot_key) do
    case Cachex.get(cache, cache_key) do
      {:ok, {:cached, {_tree, %Rules.RoutingSnapshot{key: ^snapshot_key}}}} ->
        Cachex.del(cache, cache_key)
        :deleted

      _ ->
        :stale
    end
  end

  @impl ContextCache
  def bust_by(kw) do
    entries =
      kw
      |> Enum.flat_map(fn
        {:id, id} ->
          [{:get_rule, [id]}]

        {:source_id, source_id} ->
          [{:list_by_source_id, [source_id]}, {:rules_tree_by_source_id, [source_id]}]

        {:backend_id, backend_id} ->
          [{:list_by_backend_id, [backend_id]}]
      end)

    Cachex.execute(Rules.Cache, fn worker ->
      Enum.reduce(entries, 0, fn k, acc ->
        acc + delete_and_count(worker, k)
      end)
    end)
  end

  defp fetch_rule(cache, id) do
    ContextCache.fetch(cache, {:get_rule, [id]}, fn -> Rules.get_rule(id) end)
  end

  defp delete_and_count(cache, key) do
    case Cachex.take(cache, key) do
      {:ok, nil} ->
        0

      {:ok, {:cached, {_tree, %Rules.RoutingSnapshot{} = snapshot}}} ->
        Rules.RoutingSnapshotStore.delete(
          Rules.RoutingSnapshotStore,
          snapshot.key
        )

        1

      {:ok, _value} ->
        1
    end
  end

  defp apply_repo_fun(fun, args) do
    Logflare.ContextCache.apply_fun(Rules, fun, args)
  end
end
