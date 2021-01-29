defmodule Logflare.UsersTest do
  @moduledoc false
  import Logflare.Factory
  use Logflare.DataCase
  alias Logflare.Sources
  alias Logflare.Users

  setup do
    {:ok, user} = Users.insert_or_update_user(params_for(:user))

    {:ok, source} = Sources.create_source(params_for(:source), user)
    user = user |> Users.preload_defaults()

    {:ok, user: user, source: source}
  end

  describe "Users" do
    test "get_by/1", %{source: s1, user: u1} do
      assert u1 ==
               Users.get_user_by(id: u1.id)
               |> Users.preload_defaults()

      assert length(u1.sources) > 0
      assert s1_db = hd(u1.sources)
      assert s1_db.token == s1.token
      assert s1_db.user_id == u1.id
    end

    test "get_by api_key", %{user: right_user} do
      left_user = Users.get_user_by(api_key: right_user.api_key)
      assert left_user.id == right_user.id
      assert Users.get_user_by(api_key: "nil") == nil
    end

    test "delete preferred email", %{user: u1} do
      user = Users.get_user_by(api_key: u1.api_key)

      email = Faker.Internet.free_email()
      {:ok, user} = Users.update_user_allowed(user, %{"email_preferred" => email})
      assert Users.get_user_by(api_key: u1.api_key).email_preferred == email

      {:ok, _user} = Users.update_user_allowed(user, %{"email_preferred" => nil})
      assert Users.get_user_by(api_key: u1.api_key).email_preferred == nil
    end
  end
end
