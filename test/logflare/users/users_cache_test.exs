defmodule Logflare.Users.CacheTest do
  @moduledoc false
  alias Logflare.Users.Cache
  import Cache
  use Logflare.DataCase
  alias Logflare.{Repo, User, Source}

  setup do
    source = insert(:source)
    user = insert(:user, sources: [source])
    {:ok, user: user, source: source}
  end

  describe "users cache" do
    test "get_by_id/1", %{user: user} do
      assert get_by(id: user.id) == user
    end
  end
end
