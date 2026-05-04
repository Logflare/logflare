defmodule Logflare.Users.CacheTest do
  use Logflare.DataCase

  alias Logflare.Users
  alias Logflare.User
  alias Logflare.Users.CacheWarmer

  setup do
    insert(:plan)
    source = build(:source, notifications: %{})

    user =
      insert(:user,
        sources: [source],
        bigquery_dataset_id: "test_dataset_id",
        bigquery_project_id: "test_project_id"
      )

    {:ok, user: user, source: source}
  end

  describe "users cache" do
    test "get/1", %{user: user} do
      %_{id: user_id} =
        Users.Cache.get(user.id)
        |> Users.preload_defaults()
        |> Map.update!(:sources, &Enum.map(&1, fn s -> %{s | rules: []} end))

      assert %User{id: ^user_id} = user
    end
  end

  test "warmer" do
    assert {:ok, []} = CacheWarmer.execute(nil)
    user = insert(:user)

    insert(:source,
      user: user,
      log_events_updated_at: NaiveDateTime.shift(NaiveDateTime.utc_now(), hour: -2)
    )

    assert {:ok, pairs} = CacheWarmer.execute(nil)
    assert {:ok, true} = Cachex.put_many(Users.Cache, pairs)

    Users
    |> reject(:get, 1)

    assert Users.Cache.get(user.id)
  end
end
