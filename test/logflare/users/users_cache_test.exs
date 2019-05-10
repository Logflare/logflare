defmodule Logflare.Users.CacheTest do
  @moduledoc false
  alias Logflare.Users.Cache
  import Cache
  use ExUnit.Case
  alias Logflare.{Repo, User}

  setup do
    {:ok, user} = Repo.insert(%User{})
    {:ok, user: user}
  end

  describe "users cache" do
    test "get_by_id/1", %{user: user} do
      assert get_by_id(user.id) == user
    end
  end

end
