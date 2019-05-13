defmodule Logflare.Users.CacheTest do
  @moduledoc false
  alias Logflare.Users.Cache
  import Cache
  use Logflare.DataCase
  alias Logflare.{Repo, User, Source}

  setup do
    source = %Source{token: Faker.UUID.v4() |> String.to_atom()}
    {:ok, user} = Repo.insert(%User{sources: [source], api_key: Faker.String.base64()})
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
      sources_db = list_source_ids(user.id)
      assert sources_db == [source.token]
    end

    test "get_api_quotas/2 returns a quota map", %{user: user, source: source} do
      assert get_api_quotas(user.id, source.token) ==
               {:ok,
                %{
                  source: source.api_quota,
                  user: user.api_quota
                }}
    end

    test "find_user_by_api_key/1", %{user: right_user} do
      left_user = find_user_by_api_key(right_user.api_key)
      assert left_user.id == right_user.id
      assert find_user_by_api_key("nil") == nil
    end
  end
end
