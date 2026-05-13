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
      for id <- ids do
        ContextCache.fetch(cache, {:get_rule, [id]}, fn -> Rules.get_rule(id) end)
      end
    end)
  end

  def list_by_source_id(id), do: apply_repo_fun(__ENV__.function, [id])
  def list_by_backend_id(id), do: apply_repo_fun(__ENV__.function, [id])

  def rules_tree_by_source_id(id), do: apply_repo_fun(__ENV__.function, [id])

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

  defp delete_and_count(cache, key) do
    case Cachex.take(cache, key) do
      {:ok, nil} -> 0
      {:ok, _value} -> 1
    end
  end

  defp apply_repo_fun(fun, args) do
    Logflare.ContextCache.apply_fun(Rules, fun, args)
  end
end
