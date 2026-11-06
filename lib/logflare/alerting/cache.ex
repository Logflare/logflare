defmodule Logflare.Alerting.Cache do
  @moduledoc false

  use Logflare.ContextCache

  alias Logflare.Alerting
  alias Logflare.Cache.CachexOps
  alias Logflare.ContextCache
  alias Logflare.ContextCache.CatalogCache
  alias Logflare.Repo
  alias Logflare.User

  @spec child_spec(term()) :: Supervisor.child_spec()
  def child_spec(_) do
    CachexOps.child_spec(__MODULE__,
      limit: 100_000,
      ttl: to_timeout(minute: 5),
      purge_interval: to_timeout(minute: 1)
    )
  end

  @spec list_by_user_id(User.t() | pos_integer()) ::
          [Logflare.Alerting.AlertQuery.t()] | CatalogCache.error()
  def list_by_user_id(%User{id: user_id}), do: list_by_user_id(user_id)

  def list_by_user_id(user_id) when is_integer(user_id) do
    if Repo.in_transaction?() do
      Alerting.list_alert_queries_by_user_id(user_id)
    else
      fetch_list_by_user_id(user_id)
    end
  end

  @impl ContextCache
  @spec keys_to_bust(keyword()) :: Enumerable.t()
  def keys_to_bust(kw) do
    if Keyword.has_key?(kw, :user_id) do
      entries =
        kw
        |> Keyword.get_values(:user_id)
        |> Enum.map(&{:list_alert_queries_by_user_id, [&1]})

      CatalogCache.keys_to_bust(__MODULE__, entries)
    else
      CachexOps.keys_to_bust(__MODULE__, kw)
    end
  end

  defp fetch_list_by_user_id(user_id) do
    CatalogCache.fetch(__MODULE__, {:list_alert_queries_by_user_id, [user_id]}, fn ->
      Alerting.list_alert_queries_by_user_id(user_id)
    end)
  end
end
