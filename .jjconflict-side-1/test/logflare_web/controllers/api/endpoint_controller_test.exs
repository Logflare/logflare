defmodule LogflareWeb.Api.EndpointControllerTest do
  use LogflareWeb.ConnCase

  setup do
    insert(:plan)
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
        |> get(~p"/api/endpoints")
        |> json_response(200)
        |> assert_schema("EndpointApiSchemaListResponse")

      response = response |> Enum.map(& &1.token) |> Enum.sort()
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
        |> get(~p"/api/endpoints/#{endpoint.token}")
        |> json_response(200)
        |> assert_schema("EndpointApiSchema")

      assert response.token == endpoint.token
    end

    test "returns not found if doesn't own the endpoint query", %{
      conn: conn,
      endpoints: [endpoint | _]
    } do
      invalid_user = insert(:user)

      conn
      |> add_access_token(invalid_user, ~w(private))
      |> get(~p"/api/endpoints/#{endpoint.token}")
      |> json_response(404)
      |> assert_schema("NotFoundResponse")
    end
  end

  describe "create/2" do
    test "creates a new endpoint query for an authenticated user", %{
      conn: conn,
      user: user
    } do
      name = TestUtils.random_string()
      query = "select a from logs"

      response =
        conn
        |> add_access_token(user, ~w(private))
        |> post(~p"/api/endpoints", %{name: name, language: "bq_sql", query: query})
        |> json_response(201)
        |> assert_schema("EndpointApiSchema")

      assert response.name == name
      assert response.query == query
    end

    test "returns 422 on missing arguments", %{conn: conn, user: user} do
      response =
        conn
        |> add_access_token(user, ~w(private))
        |> post(~p"/api/endpoints")
        |> json_response(422)
        |> assert_schema("UnprocessableEntityResponse")

      assert %{errors: %{"name" => _, "query" => _}} = response
    end

    test "returns 422 on bad arguments", %{conn: conn, user: user} do
      response =
        conn
        |> add_access_token(user, ~w(private))
        |> post(~p"/api/endpoints", %{name: 123})
        |> json_response(422)
        |> assert_schema("UnprocessableEntityResponse")

      assert %{errors: %{"name" => _, "query" => _}} = response
    end
  end

  describe "update/2" do
    test "updates an existing endpoint query from a user", %{
      conn: conn,
      user: user,
      endpoints: [endpoint | _]
    } do
      name = TestUtils.random_string()
      token = endpoint.token

      response =
        conn
        |> add_access_token(user, ~w(private))
        |> patch(~p"/api/endpoints/#{token}", %{name: name})
        |> text_response(204)
        |> assert_schema("AcceptedResponse")

      assert response == ""

      another_name = TestUtils.random_string()

      response =
        conn
        |> add_access_token(user, ~w(private))
        |> put(~p"/api/endpoints/#{token}", %{name: another_name})
        |> json_response(200)
        |> assert_schema("EndpointApiSchema")

      assert %{name: ^another_name, token: ^token} = response
    end

    test "returns not found if doesn't own the endpoint query", %{
      conn: conn,
      endpoints: [endpoint | _]
    } do
      invalid_user = insert(:user)

      conn
      |> add_access_token(invalid_user, ~w(private))
      |> patch(~p"/api/endpoints/#{endpoint.token}", %{name: TestUtils.random_string()})
      |> json_response(404)
      |> assert_schema("NotFoundResponse")
    end

    test "returns 422 on bad arguments", %{
      conn: conn,
      user: user,
      endpoints: [endpoint | _]
    } do
      response =
        conn
        |> add_access_token(user, ~w(private))
        |> patch(~p"/api/endpoints/#{endpoint.token}", %{name: 123})
        |> json_response(422)
        |> assert_schema("UnprocessableEntityResponse")

      assert %{errors: %{"name" => ["is invalid"]}} = response
    end
  end

  describe "delete/2" do
    test "deletes an existing endpoint query from a user", %{
      conn: conn,
      user: user,
      endpoints: [endpoint | _]
    } do
      name = TestUtils.random_string()

      conn
      |> add_access_token(user, ~w(private))
      |> delete(~p"/api/endpoints/#{endpoint.token}", %{name: name})
      |> text_response(204)
      |> assert_schema("AcceptedResponse")

      conn
      |> add_access_token(user, ~w(private))
      |> get(~p"/api/endpoints/#{endpoint.token}")
      |> json_response(404)
      |> assert_schema("NotFoundResponse")
    end

    test "returns not found if doesn't own the endpoint query", %{
      conn: conn,
      endpoints: [endpoint | _]
    } do
      invalid_user = insert(:user)

      conn
      |> add_access_token(invalid_user, ~w(private))
      |> delete(~p"/api/endpoints/#{endpoint.token}", %{name: TestUtils.random_string()})
      |> json_response(404)
      |> assert_schema("NotFoundResponse")
    end
  end
end
