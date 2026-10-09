defmodule Logflare.Sources.Cache do
  @moduledoc false

  use Logflare.ContextCache

  alias Logflare.Cache.CachexOps
  alias Logflare.ContextCache.Warmer
  alias Logflare.Repo
  alias Logflare.Rules
  alias Logflare.Sources
  alias Logflare.Sources.Source

  def child_spec(_) do
    ttl = to_timeout(hour: 1)

    CachexOps.child_spec(__MODULE__,
      limit: 100_000,
      ttl: ttl,
      purge_interval: to_timeout(minute: 5),
      warmer: {Sources.CacheWarmer, interval: Warmer.interval(ttl)}
    )
  end

  # For ingest
  def get_by_and_preload_rules(kv) do
    case get_by(kv) do
      nil ->
        nil

      %Source{} = source ->
        source
        |> preload_rules()
        |> Source.parse_key_values_config()
        |> Source.parse_copy_fields_config()
        |> Source.parse_drop_fields_config()
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
