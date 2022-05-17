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

      assert Auth.list_valid_access_tokens(user)  |> length() == 2
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

  defp access_token_fixture(user_or_team) do
    {:ok, key} = Auth.create_access_token(user_or_team)
    key
  end
end
