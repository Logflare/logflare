defmodule Logflare.AuthTest do
  use Logflare.DataCase
  alias Logflare.Auth
  alias Logflare.Factory
  alias Logflare.OauthAccessTokens.OauthAccessToken
  alias Logflare.OauthAccessTokens.PartnerOauthAccessToken
  alias Logflare.Partners.Partner
  alias Logflare.User

  setup do
    user = Factory.insert(:user)
    [user: user, team: Factory.insert(:team, user: user), partner: Factory.insert(:partner)]
  end

  describe "api access tokens" do
    test "can create api key", %{user: user, team: team, partner: partner} do
      assert {:ok, %OauthAccessToken{}} = Auth.create_access_token(user)
      assert {:ok, %OauthAccessToken{}} = Auth.create_access_token(team)
      assert {:ok, %PartnerOauthAccessToken{}} = Auth.create_access_token(partner)

      {:ok, %OauthAccessToken{description: "some test"}} =
        Auth.create_access_token(user, %{description: "some test"})

      assert {:ok, %PartnerOauthAccessToken{}} =
               Auth.create_access_token(partner, %{description: "some test"})

      assert Auth.list_valid_access_tokens(user) |> length() == 3
      assert Auth.list_valid_partner_access_tokens(partner) |> length() == 2
    end

    test "can revoke access tokens", %{user: user} do
      key = access_token_fixture(user)
      :ok = Auth.revoke_access_token(key)
    end

    test "verify access tokens", %{user: user, partner: partner} do
      key = access_token_fixture(user)
      assert {:ok, %User{}} = Auth.verify_access_token(key)
      assert {:ok, _} = Auth.verify_access_token(key.token)

      key = access_token_fixture(partner)
      assert {:ok, %Partner{}} = Auth.verify_partner_access_token(key)
      assert {:ok, _} = Auth.verify_access_token(key.token)
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

  defp access_token_fixture(user_or_team_or_partner) do
    {:ok, key} = Auth.create_access_token(user_or_team_or_partner)
    key
  end
end
