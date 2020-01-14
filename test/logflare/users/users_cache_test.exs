defmodule Logflare.Users.CacheTest do
  @moduledoc false
  alias Logflare.Users.Cache
  alias Logflare.Users
  import Cache
  use Logflare.DataCase

  setup do
    source = build(:source)
    user = insert(:user, sources: [source])
    {:ok, user: user, source: source}
  end

  describe "users cache" do
    test "get_by_id/1", %{user: user} do
      u =
        get_by(id: user.id)
        |> Users.preload_defaults()
        |> Map.update!(:sources, &Enum.map(&1, fn s -> %{s | rules: []} end))

      assert u == user
    end
  end
end
