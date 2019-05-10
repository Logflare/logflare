defmodule Logflare.Users.CacheTest do
  @moduledoc false
  alias Logflare.Users.Cache
  import Cache
  use ExUnit.Case
  alias Logflare.{Repo, User, Source}

  setup do
    source = %Source{token: Faker.UUID.v4()}
    {:ok, user} = Repo.insert(%User{sources: [source]})
    {:ok, user: user, source: source}
  end

  describe "users cache" do
    test "get_by_id/1", %{user: user} do
      assert get_by_id(user.id) == user
      assert get_by_id(user.id) == user
    end

    test "get_by_id/1 returns user with sources preloaded", %{user: user, source: source} do
      user = get_by_id(user.id)
      assert is_list(user.sources)
      assert length(user.sources) > 0
      source_db = hd(user.sources)
      assert source_db.token == source.token
      assert source_db.user_id == user.id
    end

    test "list_sources/1 returns a list of sources", %{user: user, source: source} do
      sources_db = Enum.map(list_sources(user.id), & &1.token)
      assert sources_db == [source.token]
    end

    test "get_api_quotas/2 returns a quota map", %{user: user, source: source} do
      assert get_api_quotas(user.id, source.token) == %{source: 25, user: 1000}
    end
  end
end
