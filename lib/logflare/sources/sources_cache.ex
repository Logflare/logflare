defmodule Logflare.Sources.Cache do
  alias Logflare.{Sources}
  import Cachex.Spec
  @ttl :timer.minutes(5)

  @cache __MODULE__

  def child_spec(_) do
    cachex_opts = [
      expiration: expiration(default: @ttl)
    ]

    %{
      id: :cachex_sources_cache,
      start: {Cachex, :start_link, [Sources.Cache, cachex_opts]}
    }
  end

  def get_by_id(source_id) when is_atom(source_id) do
    fetch_or_commit({:source_id, [source_id]}, &Sources.get_by_id/1)
  end

  def get_by_name(source_name) do
    fetch_or_commit({:source_name, [source_name]}, &Sources.get_by_name/1)
  end

  def fetch_or_commit({type, args}, fun) when is_list(args) and is_atom(type) do
    case Cachex.fetch(@cache, {type, args}, fn {_type, args} ->
           {:commit, apply(fun, args)}
         end) do
      {:commit, value} -> value
      {:ok, value} -> value
    end
  end
end
