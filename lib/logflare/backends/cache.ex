defmodule Logflare.Backends.Cache do
  @moduledoc false

  use Logflare.ContextCache

  alias Logflare.Backends
  alias Logflare.Cache.CachexOps

  def child_spec(_) do
    CachexOps.child_spec(__MODULE__, limit: 100_000, warmer: Backends.CacheWarmer)
  end

  def list_backends(arg), do: apply_repo_fun(__ENV__.function, [arg])

  @spec list_default_ingest_backends(pos_integer()) :: [Backends.Backend.t()]
  def list_default_ingest_backends(user_id) do
    fetch({:list_default_ingest_backends, [user_id]}, fn ->
      Backends.list_default_ingest_backends(user_id)
    end)
  end

  @spec list_enabled_backends(keyword()) :: [Logflare.Backends.Backend.t()]
  def list_enabled_backends(arg) do
    # Filter here instead of passing `enabled: true` to `list_backends/1`. Otherwise a disabled
    # backend disappears from the cached list, so re-enabling it cannot clear that list by ID.
    arg
    |> list_backends()
    |> Enum.filter(& &1.enabled)
  end

  def get_backend(arg), do: apply_repo_fun(__ENV__.function, [arg])

  defp apply_repo_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(Backends, arg1, arg2)
  end
end
