defmodule Logflare.SourceSchemas.Cache do
  @moduledoc false
  alias Logflare.SourceSchemas
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
               warmer(
                 required: false,
                 module: SourceSchemas.CacheWarmer,
                 name: SourceSchemas.CacheWarmer
               )
             ],
             hooks:
               [
                 if(stats, do: Utils.cache_stats()),
                 Utils.cache_limit(100_000)
               ]
               |> Enum.filter(& &1),
             # shorter expiration for schemas
             expiration: Utils.cache_expiration_min(10, 2)
           ]
         ]}
    }
  end

  @behaviour Logflare.ContextCache

  @impl Logflare.ContextCache
  def bust_actions(:update, kw) when is_list(kw) do
    id = Keyword.fetch!(kw, :id)
    schema = SourceSchemas.get_source_schema(id)
    {:full, %{{:get_source_schema_by_source_id, [schema.source_id]} => schema}}
  end

  def bust_actions(:delete, kw) when is_list(kw) do
    source_id = Keyword.fetch!(kw, :source_id)
    {:full, %{{:get_source_schema_by_source_id, [source_id]} => :bust}}
  end

  def get_source_schema_by_source_id(s_id), do: apply_fun(__ENV__.function, [s_id])

  defp apply_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(SourceSchemas, arg1, arg2)
  end
end
