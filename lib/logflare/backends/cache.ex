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

  def get_backend(arg), do: apply_repo_fun(__ENV__.function, [arg])

  @impl Logflare.ContextCache
  def keys_to_bust(id: :not_found) do
    owner_lists =
      Cachex.stream!(__MODULE__)
      |> Stream.map(fn {:entry, key, _, _, _} -> key end)
      |> Stream.filter(&match?({:list_backends, [[user_id: _]]}, &1))

    Stream.concat(super(id: :not_found), owner_lists)
  end

  def keys_to_bust(kw), do: super(kw)

  defp apply_repo_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(Backends, arg1, arg2)
  end
end
