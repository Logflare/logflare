defmodule Logflare.Auth.CacheTest do
  use Logflare.DataCase
  alias Logflare.Auth
  alias Logflare.Factory
  alias Logflare.Partners.Partner
  alias Logflare.ContextCache

  setup do
    user = Factory.insert(:user)
    [user: user, team: Factory.insert(:team, user: user), partner: Factory.insert(:partner)]
  end

  test "verify_access_token/2 public scope is cached", %{user: user} do
    # no scope set
    {:ok, key} = Auth.create_access_token(user)
    assert {:ok, _, _} = Auth.Cache.verify_access_token(key.token, ~w(public))
    assert {:ok, _, _} = Auth.Cache.verify_access_token(key.token, "public")
    assert {:ok, _, _} = Auth.Cache.verify_access_token(key.token)

    # scope is set
    {:ok, key} = Auth.create_access_token(user, %{scopes: "public"})
    assert {:ok, _, _} = Auth.Cache.verify_access_token(key.token, ~w(public))
    assert {:ok, _, _} = Auth.Cache.verify_access_token(key.token, "public")
    assert {:ok, _, _} = Auth.Cache.verify_access_token(key.token)
  end

  test "verify_access_token/2 private scope is cached", %{user: user} do
    # no scope set
    {:ok, key} = Auth.create_access_token(user)
    assert {:error, _} = Auth.Cache.verify_access_token(key.token, ~w(private))

    # public scope set
    {:ok, key} = Auth.create_access_token(user, %{scopes: "public"})
    assert {:error, _} = Auth.Cache.verify_access_token(key.token, ~w(private))

    # scope is set
    {:ok, key} = Auth.create_access_token(user, %{scopes: "private"})
    assert {:ok, _, _} = Auth.Cache.verify_access_token(key.token, ~w(public))
    assert {:ok, _, _} = Auth.Cache.verify_access_token(key.token, ~w(private))
  end

  test "verify_access_token/2 partner scope is cached", %{partner: partner} do
    key = access_token_fixture(partner)
    assert {:ok, key, %Partner{}} = Auth.Cache.verify_access_token(key, ~w(partner))
    assert {:ok, _token, %Partner{}} = Auth.Cache.verify_access_token(key.token, ~w(partner))

    # If scope is missing, should be unauthorized
    assert {:error, :unauthorized} = Auth.Cache.verify_access_token(key)
  end

  test "cache busting", %{user: user} do
    {:ok, key} = Auth.create_access_token(user)

    Auth
    |> expect(:verify_access_token, 2, fn _ -> {:ok, key, user} end)

    assert {:ok, _, _} = Auth.Cache.verify_access_token(key.token)
    ContextCache.bust_keys([{Auth, key.id}])
    assert {:ok, _, _} = Auth.Cache.verify_access_token(key.token)
  end

  defp access_token_fixture(user_or_team_or_partner) do
    {:ok, key} = Auth.create_access_token(user_or_team_or_partner)
    key
  end
end
