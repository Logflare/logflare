defmodule Logflare.Sources.Catalog.Cache do
  @moduledoc false

  @behaviour Logflare.ContextCache

  alias Logflare.ContextCache
  alias Logflare.ContextCache.Gossip
  alias Logflare.Repo
  alias Logflare.Sources.Catalog
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
             expiration: Utils.cache_expiration_min(60, 5)
           ]
         ]}
    }
  end

  @spec list_by_user(User.t() | pos_integer()) :: [Logflare.Sources.Source.t()]
  def list_by_user(%User{id: user_id}), do: list_by_user(user_id)

  def list_by_user(user_id) when is_integer(user_id) do
    if Repo.in_transaction?() do
      Catalog.list_by_user(user_id)
    else
      fetch(user_id)
    end
  end

  @spec source_name_by_token(pos_integer(), String.t()) :: String.t() | nil
  def source_name_by_token(user_id, token) when is_integer(user_id) and is_binary(token) do
    user_id
    |> list_by_user()
    |> Enum.find_value(fn source ->
      if to_string(source.token) == token, do: source.name
    end)
  end

  defp fetch(user_id) do
    cache_key = {:list_by_user, [user_id]}

    case Cachex.fetch(__MODULE__, cache_key, fn ->
           sources = Repo.with_replica(fn -> Catalog.list_by_user(user_id) end)

           if sources == [] do
             {:ignore, {:cached, sources}}
           else
             {:commit, {:cached, sources}}
           end
         end) do
      {:ok, {:cached, sources}} ->
        sources

      {:commit, {:cached, sources}} ->
        Gossip.multicast(__MODULE__, cache_key, sources)
        sources

      {:ignore, {:cached, sources}} ->
        sources
    end
  end

  @impl ContextCache
  def bust_by(kw) do
    entries =
      kw
      |> Keyword.get_values(:user_id)
      |> Enum.map(&{:list_by_user, [&1]})

    Cachex.execute(__MODULE__, fn cache ->
      Enum.reduce(entries, 0, fn key, acc ->
        case Cachex.take(cache, key) do
          {:ok, nil} -> acc
          {:ok, _value} -> acc + 1
        end
      end)
    end)
  end
end
