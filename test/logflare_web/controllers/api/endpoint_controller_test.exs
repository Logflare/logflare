defmodule LogflareWeb.Api.EndpointControllerTest do
  use LogflareWeb.ConnCase
  import Logflare.Factory

  setup do
    endpoints = insert_list(2, :endpoint)
    user = insert(:user, endpoint_queries: endpoints)

    Logflare.SQL
    |> stub(:transform, fn _, _ -> {:ok, nil} end)
    |> stub(:sources, fn _, _ -> {:ok, nil} end)

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
        |> login_user(user)
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
        |> login_user(user)
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
      |> login_user(invalid_user)
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
        |> login_user(user)
        |> post("/api/endpoints", %{name: name, query: "select * from logs"})
        |> json_response(201)

      assert response["name"] == name
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
        |> login_user(user)
        |> patch("/api/endpoints/#{endpoint.token}", %{name: name})
        |> json_response(204)

      assert response["name"] == name
    end

    test "returns not found if doesn't own the enpoint query", %{
      conn: conn,
      endpoints: [endpoint | _]
    } do
      invalid_user = insert(:user)

      conn
      |> login_user(invalid_user)
      |> patch("/api/endpoints/#{endpoint.token}", %{name: TestUtils.random_string()})
      |> response(404)
    end
  end

  describe "delete/2" do
    test "deletes an existing enpoint query from a user", %{
      conn: conn,
      user: user,
      endpoints: [endpoint | _]
    } do
      name = TestUtils.random_string()

      conn
      |> login_user(user)
      |> delete("/api/endpoints/#{endpoint.token}", %{name: name})
      |> response(204)

      conn
      |> login_user(user)
      |> get("/api/endpoints/#{endpoint.token}")
      |> response(404)
    end

    test "returns not found if doesn't own the enpoint query", %{
      conn: conn,
      endpoints: [endpoint | _]
    } do
      invalid_user = insert(:user)

      conn
      |> login_user(invalid_user)
      |> delete("/api/endpoints/#{endpoint.token}", %{name: TestUtils.random_string()})
      |> response(404)
    end
  end

  test "changeset errors handled gracefully", %{
    conn: conn,
    user: user,
    endpoints: [endpoint | _]
  } do
    resp =
      conn
      |> login_user(user)
      |> post("/api/endpoints")
      |> json_response(422)

    assert resp == %{"errors" => %{"name" => ["can't be blank"], "query" => ["can't be blank"]}}

    resp =
      conn
      |> login_user(user)
      |> patch("/api/endpoints/#{endpoint.token}", %{name: 123})
      |> json_response(422)

    assert resp == %{"errors" => %{"name" => ["is invalid"]}}
  end
end
