defmodule Logflare.Alerting.Cache do
  @moduledoc false

  @behaviour Logflare.ContextCache

  alias Logflare.Alerting
  alias Logflare.ContextCache
  alias Logflare.Repo
  alias Logflare.User
  alias Logflare.Utils

  def child_spec(_) do
    stats = Application.get_env(:logflare, :cache_stats, false)

    %{
      id: __MODULE__,
      start:
        {Cachex, :start_link,
         [
           __MODULE__,
           [
             hooks:
               [
                 if(stats, do: Utils.cache_stats()),
                 Utils.cache_limit(100_000)
               ]
               |> Enum.filter(& &1),
             expiration: Utils.cache_expiration_min(5, 1)
           ]
         ]}
    }
  end

  @spec list_by_user_id(User.t() | pos_integer()) :: [Logflare.Alerting.AlertQuery.t()]
  def list_by_user_id(%User{id: user_id}), do: list_by_user_id(user_id)

  def list_by_user_id(user_id) when is_integer(user_id) do
    if Repo.in_transaction?() do
      Alerting.list_alert_queries_by_user_id(user_id)
    else
      fetch_list_by_user_id(user_id)
    end
  end

  @impl ContextCache
  def bust_by(kw) do
    entries =
      kw
      |> Keyword.get_values(:user_id)
      |> Enum.map(&{:list_alert_queries_by_user_id, [&1]})

    Cachex.execute(__MODULE__, fn cache ->
      Enum.reduce(entries, 0, fn key, acc -> acc + delete_and_count(cache, key) end)
    end)
  end

  defp fetch_list_by_user_id(user_id) do
    cache_key = {:list_alert_queries_by_user_id, [user_id]}

    case Cachex.fetch(__MODULE__, cache_key, fn -> fetch_from_repo(user_id) end) do
      {_status, {:cached, alerts}} -> alerts
    end
  end

  defp fetch_from_repo(user_id) do
    alerts = Repo.with_replica(fn -> Alerting.list_alert_queries_by_user_id(user_id) end)

    if alerts == [] do
      {:ignore, {:cached, alerts}}
    else
      {:commit, {:cached, alerts}}
    end
  end

  defp delete_and_count(cache, key) do
    case Cachex.take(cache, key) do
      {:ok, nil} -> 0
      {:ok, _value} -> 1
    end
  end
end
