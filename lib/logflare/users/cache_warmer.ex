defmodule Logflare.Users.CacheWarmer do
  require Logger

  alias Logflare.ContextCache.WriteFence
  alias Logflare.Users

  use Cachex.Warmer

  @impl true
  def execute(_state) do
    {:ok, snapshot} = WriteFence.begin_snapshot(Users)
    users = Users.list_ingesting_users(limit: 1_000)
    preloaded_users = Users.preload_defaults(users)

    entry_groups =
      for {user, preloaded} <- Enum.zip(users, preloaded_users) do
        {user.id,
         [
           {{:get, [user.id]}, {:cached, user}},
           {{:get_by, [[api_key: user.api_key]]}, {:cached, user}},
           {{:get_by_and_preload, [[api_key: user.api_key]]}, {:cached, preloaded}},
           {{:preload_defaults, [user]}, {:cached, preloaded}}
         ]}
      end

    case WriteFence.commit(snapshot, Users.Cache, entry_groups) do
      {:ok, %{skipped: skipped}} ->
        if skipped > 0 do
          Logger.warning("Users cache warmer skipped #{skipped} invalidated users")
        end

      {:error, reason} ->
        Logger.warning("Users cache warmer could not commit: #{inspect(reason)}")
    end

    :ignore
  end
end
