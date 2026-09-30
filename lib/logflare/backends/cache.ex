defmodule Logflare.Backends.Cache do
  @moduledoc false

  use Logflare.ContextCache, refresh_ahead: true

  alias Logflare.Backends
  alias Logflare.Cache.CachexOps

  def child_spec(_) do
    CachexOps.child_spec(__MODULE__, limit: 100_000, warmer: Backends.CacheWarmer)
  end

  def list_backends(arg), do: apply_repo_fun(__ENV__.function, [arg])
  def get_backend(arg), do: apply_repo_fun(__ENV__.function, [arg])

  defp apply_repo_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(Backends, arg1, arg2)
  end
end
