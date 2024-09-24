defmodule LogflareWeb.Api.EndpointControllerTest do
  use LogflareWeb.ConnCase

  setup do
    endpoints = insert_list(2, :endpoint)
    user = insert(:user, endpoint_queries: endpoints)
    insert(:source, name: "logs", user: user)

    {:ok, user: user, endpoints: endpoints}
  end

  describe "index/2" do
    test "returns list of endpoint queries for given user", %{
      conn: conn,
      user: user,
      endpoints: endpoints
    } do
      response =
        conn
        |> add_access_token(user, ~w(private))
        |> get("/api/endpoints")
        |> json_response(200)

      response = response |> Enum.map(& &1["token"]) |> Enum.sort()
      expected = endpoints |> Enum.map(& &1.token) |> Enum.sort()

      assert response == expected
    end
  end

  describe "show/2" do
    test "returns single endpoint query for given user", %{
      conn: conn,
      user: user,
      endpoints: [endpoint | _]
    } do
      response =
        conn
        |> add_access_token(user, ~w(private))
        |> get("/api/endpoints/#{endpoint.token}")
        |> json_response(200)

      assert response["token"] == endpoint.token
    end

    test "returns not found if doesn't own the endpoint query", %{
      conn: conn,
      endpoints: [endpoint | _]
    } do
      invalid_user = insert(:user)

      conn
      |> add_access_token(invalid_user, ~w(private))
      |> get("/api/endpoints/#{endpoint.token}")
      |> response(404)
    end
  end

  describe "create/2" do
    test "creates a new endpoint query for an authenticated user", %{
      conn: conn,
      user: user
    } do
      name = TestUtils.random_string()

      response =
        conn
        |> add_access_token(user, ~w(private))
        |> post("/api/endpoints", %{name: name, language: "bq_sql", query: "select a from logs"})
        |> json_response(201)

      assert response["name"] == name
    end

    test "returns 422 on missing arguments", %{conn: conn, user: user} do
      resp =
        conn
        |> add_access_token(user, ~w(private))
        |> post("/api/endpoints")
        |> json_response(422)

      assert %{"errors" => %{"name" => _, "query" => _}} = resp
    end

    test "returns 422 on bad arguments", %{conn: conn, user: user} do
      resp =
        conn
        |> add_access_token(user, ~w(private))
        |> post("/api/endpoints", %{name: 123})
        |> json_response(422)

      assert %{"errors" => %{"name" => _, "query" => _}} = resp
    end
  end

  describe "update/2" do
    test "updates an existing enpoint query from a user", %{
      conn: conn,
      user: user,
      endpoints: [endpoint | _]
    } do
      name = TestUtils.random_string()

      response =
        conn
        |> add_access_token(user, ~w(private))
        |> patch("/api/endpoints/#{endpoint.token}", %{name: name})
        |> response(204)

      assert response == ""
    end

    test "returns not found if doesn't own the enpoint query", %{
      conn: conn,
      endpoints: [endpoint | _]
    } do
      invalid_user = insert(:user)

      conn
      |> add_access_token(invalid_user, ~w(private))
      |> patch("/api/endpoints/#{endpoint.token}", %{name: TestUtils.random_string()})
      |> response(404)
    end

    test "returns 422 on bad arguments", %{
      conn: conn,
      user: user,
      endpoints: [endpoint | _]
    } do
      resp =
        conn
        |> add_access_token(user, ~w(private))
        |> patch("/api/endpoints/#{endpoint.token}", %{name: 123})
        |> json_response(422)

      assert resp == %{"errors" => %{"name" => ["is invalid"]}}
    end
  end

  describe "delete/2" do
    test "deletes an existing enpoint query from a user", %{
      conn: conn,
      user: user,
      endpoints: [endpoint | _]
    } do
      name = TestUtils.random_string()

      assert conn
             |> add_access_token(user, ~w(private))
             |> delete("/api/endpoints/#{endpoint.token}", %{name: name})
             |> response(204)

      assert conn
             |> add_access_token(user, ~w(private))
             |> get("/api/endpoints/#{endpoint.token}")
             |> response(404)
    end

    test "returns not found if doesn't own the enpoint query", %{
      conn: conn,
      endpoints: [endpoint | _]
    } do
      invalid_user = insert(:user)

      assert conn
             |> add_access_token(invalid_user, ~w(private))
             |> delete("/api/endpoints/#{endpoint.token}", %{name: TestUtils.random_string()})
             |> response(404)
    end
  end
end
