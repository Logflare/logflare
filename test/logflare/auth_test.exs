defmodule Logflare.AuthTest do
  use Logflare.DataCase
  alias Logflare.Auth
  alias Logflare.OauthAccessTokens.OauthAccessToken
  alias Logflare.Factory
  alias Logflare.User

  setup do
    user = Factory.insert(:user)
    [user: user, team: Factory.insert(:team, user: user)]
  end

  describe "api access tokens" do
    test "can create api key", %{user: user, team: team} do
      {:ok, %OauthAccessToken{}} = Auth.create_access_token(user)
      {:ok, %OauthAccessToken{}} = Auth.create_access_token(team)

      {:ok, %OauthAccessToken{description: "some test"}} =
        Auth.create_access_token(user, %{description: "some test"})

      assert Auth.list_valid_access_tokens(user) |> length() == 3
    end

    test "can revoke access tokens", %{user: user} do
      key = access_token_fixture(user)
      :ok = Auth.revoke_access_token(key)
    end

    test "verify access tokens", %{user: user} do
      key = access_token_fixture(user)
      {:ok, %User{}} = Auth.verify_access_token(key)

      # string token
      {:ok, _} = Auth.verify_access_token(key.token)
    end
  end

  test "verify_access_token/2 public scope", %{user: user} do
    # no scope set
    {:ok, key} = Auth.create_access_token(user)
    assert {:ok, _} = Auth.verify_access_token(key.token, ~w(public))
    assert {:ok, _} = Auth.verify_access_token(key.token, "public")
    assert {:ok, _} = Auth.verify_access_token(key.token)

    # scope is set
    {:ok, key} = Auth.create_access_token(user, %{scopes: "public"})
    assert {:ok, _} = Auth.verify_access_token(key.token, ~w(public))
    assert {:ok, _} = Auth.verify_access_token(key.token, "public")
    assert {:ok, _} = Auth.verify_access_token(key.token)
  end

  test "verify_access_token/2 private scope", %{user: user} do
    # no scope set
    {:ok, key} = Auth.create_access_token(user)
    assert {:error, _} = Auth.verify_access_token(key.token, ~w(private))

    # public scope set
    {:ok, key} = Auth.create_access_token(user, %{scopes: "public"})
    assert {:error, _} = Auth.verify_access_token(key.token, ~w(private))

    # scope is set
    {:ok, key} = Auth.create_access_token(user, %{scopes: "private"})
    assert {:ok, _} = Auth.verify_access_token(key.token, ~w(public))
    assert {:ok, _} = Auth.verify_access_token(key.token, ~w(private))
  end

  defp access_token_fixture(user_or_team) do
    {:ok, key} = Auth.create_access_token(user_or_team)
    key
  end
end
