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
      for id <- ids, do: get_rule_via_cache(cache, id)
    end)
  end

  defp get_rule_via_cache(cache, id) do
    Cachex.fetch(cache, {:get_rule, [id]}, fn _key ->
      # Use a `:cached` tuple here otherwise when an fn returns nil Cachex will miss
      # the cache because it thinks ETS returned nil
      {:commit, {:cached, Rules.get_rule(id)}}
    end)
    |> case do
      {:commit, {:cached, value}} ->
        value

      {:ok, {:cached, value}} ->
        value
    end
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

  defp apply_repo_fun(fun, args) do
    Logflare.ContextCache.apply_fun(Rules, fun, args)
  end
end
