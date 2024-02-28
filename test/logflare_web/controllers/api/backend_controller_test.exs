defmodule LogflareWeb.Api.BackendControllerTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  setup do
    insert(:plan, name: "Free")
    user = insert(:user)

    {:ok, user: user}
  end

  describe "index/2" do
    test "returns list of backends for given user", %{conn: conn, user: user} do
      insert(:backend)
      backend = insert(:backend, user: user)

      [result] =
        conn
        |> add_access_token(user, "private")
        |> get(~p"/api/backends")
        |> json_response(200)

      assert result["id"] == backend.id
    end
  end

  describe "show/2" do
    test "returns single backend for given user", %{conn: conn, user: user} do
      backend = insert(:backend, user: user)

      response =
        conn
        |> add_access_token(user, "private")
        |> get("/api/backends/#{backend.token}")
        |> json_response(200)

      assert response["id"] == backend.id
    end

    test "returns not found if doesn't own the source", %{conn: conn} do
      backend = insert(:backend)
      invalid_user = insert(:user)

      conn
      |> add_access_token(invalid_user, "private")
      |> get("/api/backends/#{backend.token}")
      |> response(404)
    end
  end

  describe "create/2" do
    test "creates a new backend for an authenticated user", %{conn: conn, user: user} do
      name = TestUtils.random_string()

      response =
        conn
        |> add_access_token(user, "private")
        |> post("/api/backends", %{
          name: name,
          type: "webhook",
          config: %{url: "http://example.com"}
        })
        |> json_response(201)

      assert response["name"] == name
      assert response["config"]["url"] =~ "example.com"
    end

    test "returns 422 on missing arguments", %{conn: conn, user: user} do
      resp =
        conn
        |> add_access_token(user, "private")
        |> post("/api/backends")
        |> json_response(422)

      assert %{"errors" => %{"name" => ["can't be blank"], "config" => _, "type" => _}} = resp
    end

    test "returns 422 on bad arguments", %{conn: conn, user: user} do
      resp =
        conn
        |> add_access_token(user, "private")
        |> post("/api/backends", %{name: 123})
        |> json_response(422)

      assert %{"errors" => %{"name" => ["is invalid"]}} = resp
    end
  end

  describe "update/2" do
    test "updates an existing backend from a user", %{
      conn: conn,
      user: user
    } do
      backend = insert(:backend, user: user)
      name = TestUtils.random_string()

      response =
        conn
        |> add_access_token(user, "private")
        |> patch("/api/backends/#{backend.token}", %{name: name})
        |> json_response(204)

      assert response["name"] == name
    end

    test "returns not found if doesn't own the resource", %{conn: conn, user: user} do
      invalid_user = insert(:user)
      backend = insert(:backend, user: user)

      conn
      |> add_access_token(invalid_user, "private")
      |> patch("/api/backends/#{backend.token}", %{name: TestUtils.random_string()})
      |> response(404)
    end

    test "returns 422 on bad arguments", %{conn: conn, user: user} do
      backend = insert(:backend, user: user)

      resp =
        conn
        |> add_access_token(user, "private")
        |> patch("/api/backends/#{backend.token}", %{name: 123})
        |> json_response(422)

      assert resp == %{"errors" => %{"name" => ["is invalid"]}}
    end
  end

  describe "delete/2" do
    test "deletes an existing source from a user", %{
      conn: conn,
      user: user
    } do
      name = TestUtils.random_string()
      backend = insert(:backend, user: user)

      assert conn
             |> add_access_token(user, "private")
             |> delete("/api/backends/#{backend.token}", %{name: name})
             |> response(204)

      assert conn
             |> add_access_token(user, "private")
             |> get("/api/backends/#{backend.token}")
             |> response(404)
    end

    test "returns not found if doesn't own the source", %{
      conn: conn,
      user: user
    } do
      invalid_user = insert(:user)
      backend = insert(:backend, user: user)

      assert conn
             |> add_access_token(invalid_user, "private")
             |> delete("/api/backends/#{backend.token}")
             |> response(404)
    end
  end
end
