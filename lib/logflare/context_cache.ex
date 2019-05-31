defmodule Logflare.ContextCache do
  def apply_repo_fun(context, {fun, arity}, args) do
    cache = Module.concat(context, Cache)
    case Cachex.fetch(cache, {{fun, arity}, args}, fn {_type, args} ->
           {:commit, apply(context, fun, args)}
         end) do
      {:commit, value} -> value
      {:ok, value} -> value
    end
  end
end
