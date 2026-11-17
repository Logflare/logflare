defmodule Logflare.Users.CacheWarmer do
  alias Logflare.ContextCache.Warmer
  alias Logflare.Users
  alias Logflare.Users.Cache

  use Cachex.Warmer

  @impl true
  def execute(_state), do: Warmer.warm(Cache, &warm/0)

  @spec warm() :: Warmer.pairs()
  defp warm do
    users = Users.list_ingesting_users(limit: 1_000)

    get_kv =
      for u <- users do
        [
          {{:get, [u.id]}, u},
          {{:get_by, [[api_key: u.api_key]]}, u}
        ]
      end

    preloaded_kv =
      for {u, preloaded} <-
            Enum.zip([
              users,
              Users.preload_defaults(users)
            ]) do
        [
          {{:get_by_and_preload, [[api_key: u.api_key]]}, preloaded},
          {{:preload_defaults, [u]}, preloaded}
        ]
      end

    List.flatten(get_kv ++ preloaded_kv) |> Enum.map(fn {k, v} -> {k, {:cached, v}} end)
  end
end
