defmodule Logflare.Sources.Cache do
  @moduledoc false

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

  def get_by_for_ingest(kv) do
    case get_by(kv) do
      nil ->
        nil

      %Source{} = source ->
        source
        |> Source.parse_key_values_config()
        |> Source.parse_copy_fields_config()
        |> Source.parse_drop_fields_config()
    end
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
