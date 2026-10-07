defmodule Logflare.Sources.Cache do
  @moduledoc false

  alias Logflare.Repo
  alias Logflare.Rules
  alias Logflare.Sources
  alias Logflare.Sources.Source
  alias Logflare.Utils

  import Cachex.Spec

  def child_spec(_) do
    stats = Application.get_env(:logflare, :cache_stats, false)

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
                 if(stats, do: Utils.cache_stats()),
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

  @doc """
  Returns the source for the id. Reads the primary database when the cache holds `nil`.

  The cache can hold `nil` for a source that exists, for example after a read from a replica that
  lags behind. The cache buster does not remove a cached `nil`. When the primary has the source,
  this function also puts it in the cache entry, so later cache reads get it.
  """
  @spec get_by_id_or_primary(pos_integer()) :: Source.t() | nil
  def get_by_id_or_primary(id) when is_integer(id) do
    with nil <- get_by_id(id),
         %Source{} = source <- Sources.get(id) do
      update_by_id(source)
      source
    end
  end

  @doc """
  Replaces the cached `get_by(id: id)` entry of the source with the given struct.

  The entry must exist. A cached `nil` counts as an entry.
  """
  @spec update_by_id(Source.t()) :: {:ok, boolean()} | {:error, term()}
  def update_by_id(%Source{id: id} = source),
    do: Logflare.ContextCache.update(Sources, :get_by, [[id: id]], source)

  def get_by_id(arg) when is_integer(arg), do: get_by(id: arg)
  def get_by_id(arg) when is_atom(arg), do: get_by(token: arg)
  def get_source_by_token(arg) when is_atom(arg), do: get_by(token: arg)

  defp apply_repo_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(Sources, arg1, arg2)
  end
end
