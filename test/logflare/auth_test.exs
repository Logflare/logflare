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
      assert {:ok, %PartnerOauthAccessToken{scopes: scopes}} = Auth.create_access_token(partner)
      assert scopes =~ "partner"

      {:ok, %OauthAccessToken{description: "some test"}} =
        Auth.create_access_token(user, %{description: "some test"})

      assert {:ok, %PartnerOauthAccessToken{}} =
               Auth.create_access_token(partner, %{description: "some test"})

      assert Auth.list_valid_access_tokens(user) |> length() == 3
      assert Auth.list_valid_access_tokens(partner) |> length() == 2
    end

    test "can revoke access tokens", %{user: user} do
      key = access_token_fixture(user)
      :ok = Auth.revoke_access_token(key)
    end

    test "verify access tokens", %{user: user} do
      key = access_token_fixture(user)
      assert {:ok, key, %User{}} = Auth.verify_access_token(key)
      assert {:ok, _token, _user} = Auth.verify_access_token(key.token)
    end
  end

  test "verify_access_token/2 ingest scope", %{user: user} do
    # no scope set on token, defaults to ingest to any sources
    {:ok, key} = Auth.create_access_token(user)
    assert {:ok, _, _} = Auth.verify_access_token(key.token, ~w(ingest))
    assert {:ok, _, _} = Auth.verify_access_token(key.token, "ingest")
    assert {:ok, _, _} = Auth.verify_access_token(key.token)
    assert {:ok, _, _} = Auth.verify_access_token(key.token, ~w(ingest source:2))

    # scope is set
    {:ok, key} = Auth.create_access_token(user, %{scopes: "ingest"})
    assert {:ok, _, _} = Auth.verify_access_token(key.token, ~w(ingest))
    assert {:ok, _, _} = Auth.verify_access_token(key.token, "ingest")
    assert {:ok, _, _} = Auth.verify_access_token(key.token)

    # scope to a specific resource
    # source and collection are resource aliases. i.e. they refer to the same resource.
    for name <- ["source", "collection"] do
      {:ok, key} = Auth.create_access_token(user, %{scopes: "ingest:#{name}:3 ingest:#{name}:1"})
      assert {:ok, _, _} = Auth.verify_access_token(key.token, ~w(ingest:#{name}:1))
      assert {:ok, _, _} = Auth.verify_access_token(key.token, ~w(ingest:#{name}:3))
    end
  end

  test "verify_access_token/2 query scope", %{user: user} do
    # no scope set on token
    {:ok, key} = Auth.create_access_token(user)
    assert {:error, _} = Auth.verify_access_token(key.token, ~w(query))

    # scope is set on token
    {:ok, key} = Auth.create_access_token(user, %{scopes: "query"})
    assert {:error, _} = Auth.verify_access_token(key.token, ~w(ingest))
    assert {:ok, _, _} = Auth.verify_access_token(key.token, ~w(query))
    assert {:ok, _, _} = Auth.verify_access_token(key.token)

    # scope to a specific resource
    {:ok, key} = Auth.create_access_token(user, %{scopes: "query:endpoint:3 query:endpoint:1"})
    assert {:ok, _, _} = Auth.verify_access_token(key.token, ~w(query:endpoint:1))
    assert {:ok, _, _} = Auth.verify_access_token(key.token, ~w(query:endpoint:3))
  end

  test "check_scopes/2 private scope ", %{user: user} do
    {:ok, key} = Auth.create_access_token(user, %{scopes: "private"})
    assert :ok = Auth.check_scopes(key, ~w(query))
    assert :ok = Auth.check_scopes(key, ~w(ingest))
    assert :ok = Auth.check_scopes(key, ~w(ingest:endpoint:3))
    assert :ok = Auth.check_scopes(key, ~w(ingest:source:3))
  end

  test "check_scopes/2 private:admin scope ", %{user: user} do
    {:ok, key} = Auth.create_access_token(user, %{scopes: "private:admin"})
    assert :ok = Auth.check_scopes(key, ~w(query))
    assert :ok = Auth.check_scopes(key, ~w(ingest))
    assert :ok = Auth.check_scopes(key, ~w(ingest:endpoint:3))
    assert :ok = Auth.check_scopes(key, ~w(ingest:source:3))
    assert :ok = Auth.check_scopes(key, ~w(private))
    assert :ok = Auth.check_scopes(key, ~w(private:admin))
  end

  test "check_scopes/2 default empty scopes", %{user: user} do
    # empty scopes means ingest into any source, the legacy behaviour
    {:ok, key} = Auth.create_access_token(user, %{scopes: ""})
    assert :ok = Auth.check_scopes(key, ~w(ingest))
    assert :ok = Auth.check_scopes(key, ~w(ingest:source:1))
  end

  test "check_scopes/2 deprecated public scope", %{user: user} do
    # empty scopes means ingest into any source, the legacy behaviour
    {:ok, key} = Auth.create_access_token(user, %{scopes: "public"})
    assert :ok = Auth.check_scopes(key, ~w(ingest))
    assert :ok = Auth.check_scopes(key, ~w(ingest:source:1))
    assert {:error, :unauthorized} = Auth.check_scopes(key, ~w(query))
    assert {:error, :unauthorized} = Auth.check_scopes(key, ~w(private))
  end

  test "check_scopes/2 matches all required scopes ", %{user: user} do
    # should only allow ingest into source 1
    {:ok, key} = Auth.create_access_token(user, %{scopes: "ingest:source:1 ingest:source:4"})
    assert {:error, _} = Auth.check_scopes(key, ~w(ingest))
    assert {:error, _} = Auth.check_scopes(key, ~w(ingest:source:3))
    assert {:error, _} = Auth.check_scopes(key, ~w(query))
    assert {:error, _} = Auth.check_scopes(key, ~w(query:source:4))
    assert {:error, _} = Auth.check_scopes(key, ~w(query:source:1))

    assert :ok = Auth.check_scopes(key, ~w(ingest:source:1))
    assert :ok = Auth.check_scopes(key, ~w(ingest:source:4))
    assert :ok = Auth.check_scopes(key, ~w(ingest:source:4 ingest:source:1))
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
    assert {:ok, _, _} = Auth.verify_access_token(key.token, ~w(public))
    assert {:ok, _, _} = Auth.verify_access_token(key.token, ~w(private))
  end

  test "verify_access_token/2 partner scope", %{partner: partner} do
    key = access_token_fixture(partner)
    assert {:ok, key, %Partner{}} = Auth.verify_access_token(key, ~w(partner))
    assert {:ok, _token, %Partner{}} = Auth.verify_access_token(key.token, ~w(partner))

    # If scope is missing, should be unauthorized
    assert {:error, :unauthorized} = Auth.verify_access_token(key)
  end

  defp access_token_fixture(user_or_team_or_partner) do
    {:ok, key} = Auth.create_access_token(user_or_team_or_partner)
    key
  end

  describe "can_create_admin_token?/1" do
    test "with private:admin token scope", %{user: user} do
      {:ok, token} = Auth.create_access_token(user, %{scopes: "private:admin"})
      assert Auth.can_create_admin_token?(token) == true
    end

    test "with private token scope", %{user: user} do
      {:ok, token} = Auth.create_access_token(user, %{scopes: "private"})
      assert Auth.can_create_admin_token?(token) == false
    end

    test "with no token scope", %{user: user} do
      {:ok, token} = Auth.create_access_token(user, %{scopes: nil})
      assert Auth.can_create_admin_token?(token) == false
    end

    test "with owner TeamContext", %{user: user, team: team} do
      {:ok, team_context} = Logflare.Teams.TeamContext.resolve(team.id, user.email)
      assert Auth.can_create_admin_token?(team_context) == true
    end

    test "with invited team user TeamContext", %{team: team} do
      team_user = insert(:team_user, team: team)
      {:ok, team_context} = Logflare.Teams.TeamContext.resolve(team.id, team_user.email)
      assert Auth.can_create_admin_token?(team_context) == false
    end
  end
end
