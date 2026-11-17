defmodule Logflare.Sources.Catalog.Cache do
  @moduledoc false

  use Logflare.ContextCache

  alias Logflare.Cache.CachexOps
  alias Logflare.ContextCache
  alias Logflare.ContextCache.CatalogCache
  alias Logflare.Repo
  alias Logflare.Sources.Catalog
  alias Logflare.User

  @spec child_spec(term()) :: Supervisor.child_spec()
  def child_spec(_) do
    CachexOps.child_spec(__MODULE__,
      limit: 100_000,
      ttl: to_timeout(hour: 1),
      purge_interval: to_timeout(minute: 5)
    )
  end

  @spec list_by_user(User.t() | pos_integer()) ::
          [Logflare.Sources.Source.t()] | CatalogCache.error()
  def list_by_user(%User{id: user_id}), do: list_by_user(user_id)

  def list_by_user(user_id) when is_integer(user_id) do
    if Repo.in_transaction?() do
      Catalog.list_by_user(user_id)
    else
      fetch(user_id)
    end
  end

  @spec source_name_by_token(pos_integer(), String.t()) :: String.t() | nil | CatalogCache.error()
  def source_name_by_token(user_id, token) when is_integer(user_id) and is_binary(token) do
    with sources when is_list(sources) <- list_by_user(user_id),
         %{name: name} <- Enum.find(sources, &(to_string(&1.token) == token)) do
      name
    end
  end

  defp fetch(user_id) do
    CatalogCache.fetch(__MODULE__, {:list_by_user, [user_id]}, fn ->
      Catalog.list_by_user(user_id)
    end)
  end

  @impl ContextCache
  @spec keys_to_bust(keyword()) :: Enumerable.t()
  def keys_to_bust(kw) do
    entries =
      kw
      |> Keyword.get_values(:user_id)
      |> Enum.map(&{:list_by_user, [&1]})

    CatalogCache.keys_to_bust(__MODULE__, entries)
  end
end
