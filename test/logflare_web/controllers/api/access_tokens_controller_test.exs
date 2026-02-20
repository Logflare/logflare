defmodule LogflareWeb.Api.AccessTokensTest do
  @moduledoc false
  use LogflareWeb.ConnCase

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

    test "must use private token", %{conn: conn, user: user} do
      conn
      |> add_access_token(user, "public")
      |> get(~p"/api/access-tokens")
      |> json_response(401)
    end
  end

  describe "create/2" do
    test "creating a public token", %{conn: conn, user: user} do
      assert %{"token" => token} =
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
      assert response["scopes"] =~ "public"
    end

    test "cannot create partner scope", %{conn: conn, user: user} do
      conn
      |> add_access_token(user, "private")
      |> post("/api/access-tokens", %{scopes: "partner"})
      |> json_response(401)
    end

    test "users with private:admin scope token can create admin scope tokens", %{
      conn: conn,
      user: user
    } do
      {:ok, admin_token} = Logflare.Auth.create_access_token(user, %{scopes: "private:admin"})

      response =
        conn
        |> put_req_header("authorization", "Bearer #{admin_token.token}")
        |> post("/api/access-tokens", %{scopes: "private:admin"})
        |> json_response(201)

      assert response["scopes"] =~ "private:admin"
      assert response["token"]
    end

    test "users with other scope token cannot create admin scope tokens", %{
      conn: conn,
      user: user
    } do
      conn
      |> add_access_token(user, "private")
      |> post("/api/access-tokens", %{scopes: "private:admin"})
      |> json_response(401)

      conn
      |> add_access_token(user, "ingest:source:1")
      |> post("/api/access-tokens", %{scopes: "private:admin"})
      |> json_response(401)
    end

    test "must use private token", %{conn: conn, user: user} do
      conn
      |> add_access_token(user, "public")
      |> post(~p"/api/access-tokens")
      |> json_response(401)
    end
  end

  describe "DELETE access tokens" do
    test "try to revoke a non-existent access token", %{
      conn: conn,
      user: user
    } do
      assert conn
             |> add_access_token(user, "private")
             |> delete("/api/access-tokens/123")
             |> response(404)
    end

    test "revokes an existing access token", %{
      conn: conn,
      user: user
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

      #  try to revoke again
      assert conn
             |> add_access_token(user, "private")
             |> delete("/api/access-tokens/#{access_token.token}")
             |> response(204)
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
