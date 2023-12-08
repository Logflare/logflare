defmodule LogflareWeb.Api.AccessTokensTest do
  use LogflareWeb.ConnCase

  import Logflare.Factory

  setup do
    insert(:plan, name: "Free")
    user = insert(:user)
    {:ok, user: user}
  end

  describe "index/2" do
    test "returns list of access tokens for given user", %{conn: conn, user: user} do
      assert [token] =
        conn
        |> add_access_token(user, "private")
        |> get(~p"/api/access-tokens")
        |> json_response(200)
      assert token["scopes"] =~ "private"
      # don't reveal value of management tokens
      refute token["token"]
    end

    test "reveal value of public tokens", %{conn: conn, user: user} do
      insert(:access_token, resource_owner: user, scopes: "public")
      assert response =
        conn
        |> add_access_token(user, "private")
        |> get(~p"/api/access-tokens")
        |> json_response(200)

      assert [token] = Enum.filter(response, &(&1["scopes"] =~ "public"))
      # don't reveal value of management tokens
      assert token["token"]
    end

    test "must use private token", %{conn: conn, user: user } do
        conn
        |> add_access_token(user, "public")
        |> get(~p"/api/access-tokens")
        |> json_response(401)
    end
  end

  describe "create/2" do
    test "creating a public token", %{conn: conn, user: user} do
      assert %{"token"=> token} =
        conn
        |> add_access_token(user, "private")
        |> post(~p"/api/access-tokens")
        |> json_response(201)

      assert token
    end


    test "creates a new access token for an authenticated user", %{conn: conn, user: user} do

      response =
        conn
        |> add_access_token(user, "private")
        |> post(~p"/api/access-tokens?#{[description: "some value"]}")
        |> json_response(201)

      assert response["description"]
      # reveal the value
      assert response["token"]
    end

    test "cannot create partner scope", %{conn: conn, user: user} do
      conn
      |> add_access_token(user, "private")
      |> post("/api/access-tokens", %{scopes: "partner"})
      |> json_response(401)
    end

    test "must use private token", %{conn: conn, user: user } do
      conn
      |> add_access_token(user, "public")
      |> post(~p"/api/access-tokens")
      |> json_response(401)
    end
  end

  describe "DELETE access tokens" do
    test "revokes an existing access token", %{
      conn: conn,
      user: user,
    } do
      access_token = insert(:access_token, resource_owner: user)

      assert conn
             |> add_access_token(user, "private")
             |> delete("/api/access-tokens/#{access_token.token}")
             |> response(204)

      assert conn
             |> add_access_token(user, "private")
             |> get("/api/access-tokens/#{access_token.token}")
             |> response(404)
    end

    test "returns not found if doesn't own the source", %{
      conn: conn,
      user: user
    } do
      access_token = insert(:access_token, resource_owner: user)
      invalid_user = insert(:user)

      assert conn
             |> add_access_token(invalid_user, "private")
             |> delete("/api/access-tokens/#{access_token.token}")
             |> response(404)
    end
  end
end
