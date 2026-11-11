defmodule Logflare.QueryContext.Cache do
  @moduledoc false

  use Logflare.ContextCache

  alias Logflare.Cache.CachexOps
  alias Logflare.ContextCache.CatalogCache

  @spec child_spec(term()) :: Supervisor.child_spec()
  def child_spec(_) do
    CachexOps.child_spec(__MODULE__, limit: 100_000)
  end

  @impl Logflare.ContextCache
  @spec fetch(term(), (-> result)) :: result | {:error, :database_unavailable} when result: term()
  def fetch(key, loader), do: CatalogCache.fetch(__MODULE__, key, loader)

  @spec invalidate(keyword()) :: {:ok, non_neg_integer()}
  def invalidate(fields), do: delete_keys(keys_to_bust(fields))

  @impl Logflare.ContextCache
  @spec keys_to_bust(keyword()) :: [term()]
  def keys_to_bust(fields) do
    users = Enum.map(Keyword.get_values(fields, :user_id), &{:get_user, [&1]})
    backends = Enum.map(Keyword.get_values(fields, :backend_id), &{:get_backend, [&1]})

    owners =
      case Keyword.get_values(fields, :owner_id) do
        [] when backends != [] -> owner_keys()
        ids -> Enum.map(ids, &{:list_backends, [&1]})
      end

    CatalogCache.keys_to_bust(__MODULE__, users ++ backends ++ owners)
  end

  defp owner_keys do
    for {:entry, {__MODULE__, {:list_backends, [_]} = key}, _, _, _} <-
          Cachex.stream!(CatalogCache),
        do: key
  end
end
