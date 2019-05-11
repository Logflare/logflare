defmodule Logflare.Sources.Cache do
  alias Logflare.{Sources}
  @cache __MODULE__

  def get_by_id(source_id) when is_atom(source_id) do
    fetch_or_commit({:source_id, [source_id]}, &Sources.get_by_id/1)
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
