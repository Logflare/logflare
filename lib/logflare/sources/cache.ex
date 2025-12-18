defmodule Logflare.Sources.Cache do
  @moduledoc false

  alias Logflare.Repo
  alias Logflare.Rules
  alias Logflare.Sources
  alias Logflare.Sources.Source
  alias Logflare.Utils

  import Cachex.Spec

  def child_spec(_) do
    %{
      id: __MODULE__,
      start:
        {Cachex, :start_link,
         [
           __MODULE__,
           [
             warmers: [
               warmer(required: false, module: Sources.CacheWarmer, name: Sources.CacheWarmer)
             ],
             hooks:
               [
                 Utils.cache_stats(),
                 Utils.cache_limit(100_000)
               ]
               |> Enum.filter(& &1),
             expiration: Utils.cache_expiration_min(60, 5)
           ]
         ]}
    }
  end

  # For ingest
  def get_by_and_preload_rules(kv) do
    case get_by(kv) do
      nil -> nil
      %Source{} = source -> preload_rules(source)
    end
  end

  def preload_rules(nil), do: nil

  def preload_rules(%Source{} = source) do
    source
    |> Repo.preload(rules: fn [id] -> Rules.Cache.list_by_source_id(id) end)
  end

  def get_by_and_preload(kv), do: apply_repo_fun(__ENV__.function, [kv])
  def get_by_id_and_preload(arg) when is_integer(arg), do: get_by_and_preload(id: arg)
  def get_by_id_and_preload(arg) when is_atom(arg), do: get_by_and_preload(token: arg)

  def get_by(kv), do: apply_repo_fun(__ENV__.function, [kv])
  def get_by_id(arg) when is_integer(arg), do: get_by(id: arg)
  def get_by_id(arg) when is_atom(arg), do: get_by(token: arg)
  def get_source_by_token(arg) when is_atom(arg), do: get_by(token: arg)

  defp apply_repo_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(Sources, arg1, arg2)
  end
end
