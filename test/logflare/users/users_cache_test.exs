defmodule Logflare.Users.CacheTest do
  @moduledoc false
  alias Logflare.Users
  use Logflare.DataCase

  setup do
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
    test "get_by_id/1", %{user: user} do
      u =
        Users.Cache.get_by(id: user.id)
        |> Users.preload_defaults()
        |> Map.update!(:sources, &Enum.map(&1, fn s -> %{s | rules: []} end))

      assert u == user
    end
  end
end
